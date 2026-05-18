from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

import pytest

import retl
from retl.backends.bigquery import BigQueryConnection, BigQueryConnectionError, BigQuerySqlBackend
from retl.collect_identity import is_uuidv7
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationProgressScope,
    EventKeysetScanPosition,
    EventSourceWindowHandle,
    StateSnapshotHandle,
)
from retl.stores.sql_runtime.schema import RUNTIME_TABLE_CATALOG
from tests.backends.sandbox.runtime_cleanup_helpers import (
    assert_event_keyset_skip_operation,
    assert_runtime_cleanup_operations,
    assert_runtime_inspect_reset_operations,
)

pytestmark = pytest.mark.live_sandbox

_SIMPLE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PROJECT_ID = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
_OPTIONAL_DEPENDENCY_MESSAGE = "optional `bigquery` dependency"


@dataclass(frozen=True)
class _LiveBigQueryConfig:
    project: str
    location: str
    source_dataset: str
    runtime_dataset: str


@dataclass(frozen=True)
class _LiveBigQuerySandbox:
    config: _LiveBigQueryConfig
    backend: BigQuerySqlBackend
    admin_connection: BigQueryConnection


@pytest.fixture(scope="module")
def live_bigquery_sandbox() -> Iterator[_LiveBigQuerySandbox]:
    config = _live_bigquery_config()
    admin_connection = _open_admin_connection(config)
    try:
        _execute(
            admin_connection,
            f"create schema if not exists {_dataset(config, config.source_dataset)} OPTIONS(location='{config.location}')",
        )
        _execute(
            admin_connection,
            f"create schema if not exists {_dataset(config, config.runtime_dataset)} OPTIONS(location='{config.location}')",
        )
        backend = BigQuerySqlBackend(
            project=config.project,
            location=config.location,
            source_project=config.project,
            source_dataset=config.source_dataset,
            runtime_project=config.project,
            runtime_dataset=config.runtime_dataset,
        )
        _create_source_tables(admin_connection, config)
        yield _LiveBigQuerySandbox(
            config=config,
            backend=backend,
            admin_connection=admin_connection,
        )
    finally:
        _drop_dataset(admin_connection, config, config.runtime_dataset)
        _drop_dataset(admin_connection, config, config.source_dataset)
        admin_connection.close()


def test_bigquery_live_sandbox_schema_collect_and_arrow_reads(
    live_bigquery_sandbox: _LiveBigQuerySandbox,
) -> None:
    sandbox = live_bigquery_sandbox
    store = sandbox.backend.runtime_store()
    try:
        _assert_runtime_schema_initialized(sandbox.admin_connection, sandbox.config)

        state_result = store.produce_state_collect(
            declaration=_state_declaration(sandbox.backend, sandbox.config),
            snapshot=StateSnapshotHandle(
                backend="bigquery",
                source_name="customers",
                source_identity=_source_identity(sandbox.config),
                query=_customers_query(sandbox.config),
                source_space=sandbox.backend.source_space,
            ),
        )
        assert state_result.current_row_count == 4
        assert state_result.upsert_count == 4
        assert state_result.remove_count == 0
        assert state_result.work_row_count == 4
        assert is_uuidv7(state_result.collect_id)

        _replace_customer_rows_for_second_collect(sandbox.admin_connection, sandbox.config)
        second_state_result = store.produce_state_collect(
            declaration=_state_declaration(sandbox.backend, sandbox.config),
            snapshot=StateSnapshotHandle(
                backend="bigquery",
                source_name="customers",
                source_identity=_source_identity(sandbox.config),
                query=_customers_query(sandbox.config),
                source_space=sandbox.backend.source_space,
            ),
        )
        assert second_state_result.current_row_count == 4
        assert second_state_result.remove_count == 2
        assert second_state_result.upsert_count == 4
        assert second_state_result.work_row_count == 6
        assert is_uuidv7(second_state_result.collect_id)
        assert second_state_result.collect_id != state_result.collect_id
        _assert_second_state_collect_pending_order(store, second_state_result.collect_id)

        first_event_result = store.produce_event_collect(
            declaration=_event_declaration(sandbox.backend, sandbox.config),
            window=EventSourceWindowHandle(
                backend="bigquery",
                source_name="purchases",
                source_identity=_source_identity(sandbox.config),
                query=_purchases_query(sandbox.config),
                cursor_column="occurred_at",
                primary_key_column="purchase_id",
                source_space=sandbox.backend.source_space,
                limit=2,
            ),
        )
        assert first_event_result.window_row_count == 2
        assert first_event_result.work_row_count == 0
        assert first_event_result.scan_upper_bound == EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-02T00:00:00"),
            primary_key_value=CanonicalKeyScalar.string("purchase_2"),
        )

        second_event_result = store.produce_event_collect(
            declaration=_event_declaration(sandbox.backend, sandbox.config),
            window=EventSourceWindowHandle(
                backend="bigquery",
                source_name="purchases",
                source_identity=_source_identity(sandbox.config),
                query=_purchases_query(sandbox.config),
                cursor_column="occurred_at",
                primary_key_column="purchase_id",
                scan_after=first_event_result.scan_upper_bound,
                source_space=sandbox.backend.source_space,
                limit=2,
            ),
        )
        assert second_event_result.window_row_count == 1
        assert second_event_result.work_row_count == 0
        assert second_event_result.scan_after == first_event_result.scan_upper_bound
        assert second_event_result.scan_upper_bound == EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-03T00:00:00"),
            primary_key_value=CanonicalKeyScalar.string("purchase_3"),
        )

        _assert_collect_rows_written(sandbox.admin_connection, sandbox.config)
        event_skip_store = sandbox.backend.runtime_store()
        try:
            assert_event_keyset_skip_operation(event_skip_store)
        finally:
            event_skip_store.close()
        _assert_arrow_read_path(sandbox.admin_connection, sandbox.config)
        assert_runtime_cleanup_operations(store)
        assert_runtime_inspect_reset_operations(store)
    finally:
        store.close()


def _live_bigquery_config() -> _LiveBigQueryConfig:
    project = os.environ.get("RETL_BIGQUERY_PROJECT") or os.environ.get(
        "BACKENDS__BIGQUERY__PROJECT"
    )
    if not project:
        pytest.skip("BigQuery live sandbox config is absent; missing RETL_BIGQUERY_PROJECT.")
    project = _required_project(project, "project")
    location = (
        os.environ.get("RETL_BIGQUERY_LOCATION")
        or os.environ.get("BACKENDS__BIGQUERY__LOCATION")
        or "US"
    ).strip()
    location = location or "US"
    prefix = os.environ.get("RETL_BIGQUERY_SANDBOX_DATASET_PREFIX", "retl_live")
    suffix = uuid.uuid4().hex[:10]
    worker = re.sub(r"[^A-Za-z0-9_]", "_", os.environ.get("PYTEST_XDIST_WORKER", "gw0")).lower()
    source_dataset = _test_identifier(prefix, worker, suffix, "src")
    runtime_dataset = _test_identifier(prefix, worker, suffix, "rt")
    return _LiveBigQueryConfig(
        project=project,
        location=location,
        source_dataset=source_dataset,
        runtime_dataset=runtime_dataset,
    )


def _open_admin_connection(config: _LiveBigQueryConfig) -> BigQueryConnection:
    try:
        return BigQueryConnection(project=config.project, location=config.location)
    except BigQueryConnectionError as exc:
        if _OPTIONAL_DEPENDENCY_MESSAGE in str(exc):
            pytest.skip("BigQuery optional dependency is not installed.")
        raise


def _create_source_tables(connection: BigQueryConnection, config: _LiveBigQueryConfig) -> None:
    customers = _relation(config, config.source_dataset, "customers")
    purchases = _relation(config, config.source_dataset, "purchases")
    _execute(
        connection,
        f"""
        create table {customers} (
            customer_id string,
            email string,
            plan string,
            audience_key string
        )
        """,
    )
    _execute(
        connection,
        f"""
        insert into {customers} values
            ('cust_remove_b', 'remove-b@example.com', 'old', 'audience_2'),
            ('cust_update_b', 'update-b@example.com', 'old', 'audience_2'),
            ('cust_remove_a', 'remove-a@example.com', 'old', 'audience_1'),
            ('cust_update_a', 'update-a@example.com', 'old', 'audience_1')
        """,
    )
    _execute(
        connection,
        f"""
        create table {purchases} (
            purchase_id string,
            email string,
            occurred_at string,
            amount numeric
        )
        """,
    )
    _execute(
        connection,
        f"""
        insert into {purchases} values
            ('purchase_1', 'alpha@example.com', '2026-01-01T00:00:00', 10.25),
            ('purchase_2', 'bravo@example.com', '2026-01-02T00:00:00', 20.50),
            ('purchase_3', 'alpha@example.com', '2026-01-03T00:00:00', 30.75)
        """,
    )


def _replace_customer_rows_for_second_collect(
    connection: BigQueryConnection,
    config: _LiveBigQueryConfig,
) -> None:
    customers = _relation(config, config.source_dataset, "customers")
    _execute(connection, f"delete from {customers} where true")
    _execute(
        connection,
        f"""
        insert into {customers} values
            ('cust_update_b', 'update-b@example.com', 'new', 'audience_2'),
            ('cust_new_b', 'new-b@example.com', 'new', 'audience_2'),
            ('cust_update_a', 'update-a@example.com', 'new', 'audience_1'),
            ('cust_new_a', 'new-a@example.com', 'new', 'audience_1')
        """,
    )


def _state_declaration(backend: BigQuerySqlBackend, config: _LiveBigQueryConfig) -> object:
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query=_customers_query(config),
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=({"type": "email", "value": "email"},),
        payload={"plan": "plan"},
    )


def _event_declaration(backend: BigQuerySqlBackend, config: _LiveBigQueryConfig) -> object:
    return retl.event(
        name="purchase_event",
        source=retl.source(
            name="purchases",
            query=_purchases_query(config),
            mode="checkpointed",
            checkpoint={
                "cursor": "occurred_at",
                "primary_key": "purchase_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
            backend=backend.source_backend(),
        ),
        key={"purchase": "purchase_id"},
        occurred_at="occurred_at",
        identifiers=({"type": "email", "value": "email"},),
        payload={"amount": "amount"},
    )


def _assert_runtime_schema_initialized(
    connection: BigQueryConnection,
    config: _LiveBigQueryConfig,
) -> None:
    rows = _execute(
        connection,
        f"""
        select table_name
        from {_dataset(config, config.runtime_dataset)}.INFORMATION_SCHEMA.TABLES
        """,
    ).fetchall()
    created_tables = {str(row[0]).casefold() for row in rows}
    expected_tables = {name.casefold() for name in RUNTIME_TABLE_CATALOG}
    assert expected_tables <= created_tables


def _assert_collect_rows_written(
    connection: BigQueryConnection,
    config: _LiveBigQueryConfig,
) -> None:
    ordered_work_count = _execute(
        connection,
        f"""
        select count(*)
        from {_relation(config, config.runtime_dataset, "ordered_work")}
        where declaration_name in ('customer_state', 'purchase_event')
        """,
    ).fetchone()
    assert ordered_work_count is not None
    assert int(ordered_work_count[0]) == 10

    state_current_count = _execute(
        connection,
        f"""
        select count(*)
        from {_relation(config, config.runtime_dataset, "state_current")}
        where declaration_name = 'customer_state'
          and source_name = 'customers'
        """,
    ).fetchone()
    assert state_current_count is not None
    assert int(state_current_count[0]) == 4


def _assert_second_state_collect_pending_order(store: Any, collect_id: str) -> None:
    page = store.read_pending_work(
        scope=DestinationProgressScope(
            sync_name="sync_live",
            destination_name="destination_live",
            surface="profile",
            family="state",
            declaration_name="customer_state",
        ),
        source_collect_id=collect_id,
        max_rows=10,
    )

    assert page.row_count == 6
    assert _column_values(page.payload, "collect_id") == [collect_id] * 6
    assert _column_values(page.payload, "sequence_order") == [0, 1, 2, 3, 4, 5]
    assert _column_values(page.payload, "kind") == [
        "remove",
        "remove",
        "upsert",
        "upsert",
        "upsert",
        "upsert",
    ]
    assert _json_column_values(page.payload, "target_json") == [
        {"value": "audience_1"},
        {"value": "audience_2"},
        {"value": "audience_1"},
        {"value": "audience_1"},
        {"value": "audience_2"},
        {"value": "audience_2"},
    ]
    assert _json_column_values(page.payload, "key_json") == [
        {"customer": "cust_remove_a"},
        {"customer": "cust_remove_b"},
        {"customer": "cust_new_a"},
        {"customer": "cust_update_a"},
        {"customer": "cust_new_b"},
        {"customer": "cust_update_b"},
    ]


def _assert_arrow_read_path(
    connection: BigQueryConnection,
    config: _LiveBigQueryConfig,
) -> None:
    result = _execute(
        connection,
        f"""
        select declaration_name, count(*) as row_count
        from {_relation(config, config.runtime_dataset, "ordered_work")}
        group by declaration_name
        order by declaration_name
        """,
    )
    table = result.fetch_arrow_all()
    assert table.column_names == ["declaration_name", "row_count"]
    assert table.num_rows == 1
    assert table.column("row_count").to_pylist() == [10]


def _column_values(batch: Any, column: str) -> list[Any]:
    return batch.column(column).to_pylist()


def _json_column_values(batch: Any, column: str) -> list[Any]:
    return [json.loads(value) if value else None for value in _column_values(batch, column)]


def _customers_query(config: _LiveBigQueryConfig) -> str:
    return (
        "select customer_id, email, plan, audience_key "
        f"from {_relation(config, config.source_dataset, 'customers')}"
    )


def _purchases_query(config: _LiveBigQueryConfig) -> str:
    return (
        "select purchase_id, email, occurred_at, amount "
        f"from {_relation(config, config.source_dataset, 'purchases')}"
    )


def _source_identity(config: _LiveBigQueryConfig) -> dict[str, object]:
    return {
        "backend": "bigquery",
        "project": config.project,
        "source_dataset": config.source_dataset,
    }


def _execute(
    connection: BigQueryConnection,
    sql: str,
    parameters: Sequence[object] = (),
) -> Any:
    return connection.execute(" ".join(sql.split()), parameters)


def _drop_dataset(
    connection: BigQueryConnection,
    config: _LiveBigQueryConfig,
    dataset: str,
) -> None:
    try:
        _execute(connection, f"drop schema if exists {_dataset(config, dataset)} cascade")
    except Exception:
        pass


def _dataset(config: _LiveBigQueryConfig, dataset: str) -> str:
    return f"`{config.project}`.`{dataset}`"


def _relation(config: _LiveBigQueryConfig, dataset: str, table: str) -> str:
    return f"{_dataset(config, dataset)}.`{table}`"


def _required_project(value: str, label: str) -> str:
    value = value.strip()
    if not _PROJECT_ID.fullmatch(value):
        raise ValueError(f"BigQuery live sandbox {label} must be a project id.")
    return value


def _test_identifier(*parts: str) -> str:
    value = "_".join(part.strip("_").lower() for part in parts if part.strip("_"))
    if not _SIMPLE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"BigQuery live sandbox generated invalid dataset `{value}`.")
    return value
