from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest

import retl
from retl.backends.databricks import (
    DATABRICKS_PAT_AUTH,
    DatabricksBackendAuth,
    DatabricksConnection,
    DatabricksConnectionError,
    DatabricksSqlBackend,
)
from retl.declarations import SecretLiteral
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
_OPTIONAL_DEPENDENCY_MESSAGE = "optional `databricks` dependency"


@dataclass(frozen=True)
class _LiveDatabricksConfig:
    server_hostname: str
    http_path: str
    catalog: str
    source_schema: str
    runtime_schema: str
    token: str


@dataclass(frozen=True)
class _LiveDatabricksSandbox:
    config: _LiveDatabricksConfig
    backend: DatabricksSqlBackend
    admin_connection: DatabricksConnection


@pytest.fixture(scope="module")
def live_databricks_sandbox() -> Iterator[_LiveDatabricksSandbox]:
    base_backend = _live_backend()
    suffix = uuid.uuid4().hex[:10]
    worker = re.sub(r"[^A-Za-z0-9_]", "_", os.environ.get("PYTEST_XDIST_WORKER", "gw0")).lower()
    source_schema = _test_identifier("retl_live", worker, suffix, "src")
    runtime_schema = _test_identifier("retl_live", worker, suffix, "rt")
    token = _required_env("BACKENDS__DATABRICKS__PAT__TOKEN")
    config = _LiveDatabricksConfig(
        server_hostname=base_backend.server_hostname,
        http_path=base_backend.http_path,
        catalog=base_backend.runtime_catalog,
        source_schema=source_schema,
        runtime_schema=runtime_schema,
        token=token,
    )
    admin_connection = _open_admin_connection(config)
    try:
        _execute(admin_connection, f"create schema if not exists {_schema(config, source_schema)}")
        _execute(admin_connection, f"create schema if not exists {_schema(config, runtime_schema)}")
        backend = DatabricksSqlBackend(
            server_hostname=config.server_hostname,
            http_path=config.http_path,
            source_catalog=config.catalog,
            source_schema=config.source_schema,
            runtime_catalog=config.catalog,
            runtime_schema=config.runtime_schema,
            auth=DatabricksBackendAuth(
                mode=DATABRICKS_PAT_AUTH,
                credentials={"token": SecretLiteral(config.token)},
            ),
        )
        _create_source_tables(admin_connection, config)
        yield _LiveDatabricksSandbox(
            config=config,
            backend=backend,
            admin_connection=admin_connection,
        )
    finally:
        _drop_schema(admin_connection, config, runtime_schema)
        _drop_schema(admin_connection, config, source_schema)
        admin_connection.close()


def test_databricks_live_temp_table_visibility_and_rollback_behavior() -> None:
    backend = _live_backend()
    temp_name = "retl_state_collect_snapshot"
    durable_name = f"retl_rollback_probe_{uuid.uuid4().hex}"
    store = backend.runtime_store()
    try:
        context = store._runtime_context  # noqa: SLF001
        assert context is not None
        context.connection.execute(f"create temporary table `{temp_name}` as select 1 as marker")
        assert context.connection.execute(f"select marker from `{temp_name}`").fetchone()[0] == 1

        context.connection.execute(
            f"create table `{backend.runtime_catalog}`.`{backend.runtime_schema}`.`{durable_name}` "
            "(marker int) using delta "
            "tblproperties ('delta.feature.catalogManaged' = 'supported')"
        )
        context.connection.execute("begin transaction")
        context.connection.execute(
            f"insert into `{backend.runtime_catalog}`.`{backend.runtime_schema}`.`{durable_name}` "
            "values (7)"
        )
        context.connection.execute("rollback")
        try:
            rows = context.connection.execute(
                f"select count(*) from `{backend.runtime_catalog}`.`{backend.runtime_schema}`."
                f"`{durable_name}`"
            ).fetchone()
        except Exception:
            rows = None
        assert rows is None or rows[0] == 0
    finally:
        try:
            context = store._runtime_context  # noqa: SLF001
            if context is not None:
                context.connection.execute(
                    f"drop table if exists `{backend.runtime_catalog}`.`{backend.runtime_schema}`."
                    f"`{durable_name}`"
                )
        except Exception:
            pass
        store.close()


def test_databricks_live_sandbox_schema_collect_and_runtime_cli(
    live_databricks_sandbox: _LiveDatabricksSandbox,
) -> None:
    sandbox = live_databricks_sandbox
    store = sandbox.backend.runtime_store()
    try:
        _assert_runtime_schema_initialized(sandbox.admin_connection, sandbox.config)

        state_result = store.produce_state_collect(
            declaration=_state_declaration(sandbox.backend, sandbox.config),
            snapshot=StateSnapshotHandle(
                backend="databricks",
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

        _replace_customer_rows_for_second_collect(sandbox.admin_connection, sandbox.config)
        second_state_result = store.produce_state_collect(
            declaration=_state_declaration(sandbox.backend, sandbox.config),
            snapshot=StateSnapshotHandle(
                backend="databricks",
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
        _assert_second_state_collect_pending_order(store, second_state_result.collect_id)

        first_event_result = store.produce_event_collect(
            declaration=_event_declaration(sandbox.backend, sandbox.config),
            window=EventSourceWindowHandle(
                backend="databricks",
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
                backend="databricks",
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
        _assert_arrow_read_path(sandbox.admin_connection, sandbox.config)
        assert_event_keyset_skip_operation(store)
        assert_runtime_cleanup_operations(store)
        assert_runtime_inspect_reset_operations(store)
    finally:
        store.close()


def _live_backend() -> DatabricksSqlBackend:
    required = {
        "BACKENDS__DATABRICKS__SERVER_HOSTNAME": os.environ.get(
            "BACKENDS__DATABRICKS__SERVER_HOSTNAME"
        ),
        "BACKENDS__DATABRICKS__HTTP_PATH": os.environ.get("BACKENDS__DATABRICKS__HTTP_PATH"),
        "BACKENDS__DATABRICKS__SOURCE_CATALOG": os.environ.get(
            "BACKENDS__DATABRICKS__SOURCE_CATALOG"
        ),
        "BACKENDS__DATABRICKS__SOURCE_SCHEMA": os.environ.get(
            "BACKENDS__DATABRICKS__SOURCE_SCHEMA"
        ),
        "BACKENDS__DATABRICKS__RUNTIME_CATALOG": os.environ.get(
            "BACKENDS__DATABRICKS__RUNTIME_CATALOG"
        ),
        "BACKENDS__DATABRICKS__RUNTIME_SCHEMA": os.environ.get(
            "BACKENDS__DATABRICKS__RUNTIME_SCHEMA"
        ),
        "BACKENDS__DATABRICKS__PAT__TOKEN": os.environ.get("BACKENDS__DATABRICKS__PAT__TOKEN"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        pytest.skip(f"Databricks live sandbox env is not configured: {', '.join(missing)}")
    return DatabricksSqlBackend(
        server_hostname=required["BACKENDS__DATABRICKS__SERVER_HOSTNAME"] or "",
        http_path=required["BACKENDS__DATABRICKS__HTTP_PATH"] or "",
        source_catalog=required["BACKENDS__DATABRICKS__SOURCE_CATALOG"] or "",
        source_schema=required["BACKENDS__DATABRICKS__SOURCE_SCHEMA"] or "",
        runtime_catalog=required["BACKENDS__DATABRICKS__RUNTIME_CATALOG"] or "",
        runtime_schema=required["BACKENDS__DATABRICKS__RUNTIME_SCHEMA"] or "",
        auth=DatabricksBackendAuth(
            mode=DATABRICKS_PAT_AUTH,
            credentials={
                "token": SecretLiteral(required["BACKENDS__DATABRICKS__PAT__TOKEN"] or ""),
            },
        ),
    )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"Databricks live sandbox env is not configured: {name}")
    return value


def _open_admin_connection(config: _LiveDatabricksConfig) -> DatabricksConnection:
    try:
        return DatabricksConnection(
            server_hostname=config.server_hostname,
            http_path=config.http_path,
            catalog=config.catalog,
            schema=config.runtime_schema,
            connect_kwargs={"access_token": config.token},
        )
    except DatabricksConnectionError as exc:
        if _OPTIONAL_DEPENDENCY_MESSAGE in str(exc):
            pytest.skip("Databricks optional dependency is not installed.")
        raise


def _create_source_tables(
    connection: DatabricksConnection,
    config: _LiveDatabricksConfig,
) -> None:
    customers = _relation(config, config.source_schema, "customers")
    purchases = _relation(config, config.source_schema, "purchases")
    _execute(
        connection,
        f"""
        create table {customers} (
            customer_id string,
            email string,
            plan string,
            audience_key string
        ) using delta
        tblproperties ('delta.feature.catalogManaged' = 'supported')
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
            amount decimal(10, 2)
        ) using delta
        tblproperties ('delta.feature.catalogManaged' = 'supported')
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
    connection: DatabricksConnection,
    config: _LiveDatabricksConfig,
) -> None:
    customers = _relation(config, config.source_schema, "customers")
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


def _state_declaration(backend: DatabricksSqlBackend, config: _LiveDatabricksConfig) -> object:
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


def _event_declaration(backend: DatabricksSqlBackend, config: _LiveDatabricksConfig) -> object:
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
    connection: DatabricksConnection,
    config: _LiveDatabricksConfig,
) -> None:
    rows = _execute(
        connection,
        f"""
        select table_name
        from `{config.catalog}`.information_schema.tables
        where table_schema = ?
        """,
        (config.runtime_schema,),
    ).fetchall()
    created_tables = {str(row[0]).casefold() for row in rows}
    expected_tables = {name.casefold() for name in RUNTIME_TABLE_CATALOG}
    assert expected_tables <= created_tables


def _assert_collect_rows_written(
    connection: DatabricksConnection,
    config: _LiveDatabricksConfig,
) -> None:
    ordered_work_count = _execute(
        connection,
        f"""
        select count(*)
        from {_relation(config, config.runtime_schema, "ordered_work")}
        where declaration_name in ('customer_state', 'purchase_event')
        """,
    ).fetchone()
    assert ordered_work_count is not None
    assert int(ordered_work_count[0]) == 10

    state_current_count = _execute(
        connection,
        f"""
        select count(*)
        from {_relation(config, config.runtime_schema, "state_current")}
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
    connection: DatabricksConnection,
    config: _LiveDatabricksConfig,
) -> None:
    result = _execute(
        connection,
        f"""
        select declaration_name, count(*) as row_count
        from {_relation(config, config.runtime_schema, "ordered_work")}
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


def _customers_query(config: _LiveDatabricksConfig) -> str:
    return (
        "select customer_id, email, plan, audience_key "
        f"from {_relation(config, config.source_schema, 'customers')}"
    )


def _purchases_query(config: _LiveDatabricksConfig) -> str:
    return (
        "select purchase_id, email, occurred_at, amount "
        f"from {_relation(config, config.source_schema, 'purchases')}"
    )


def _source_identity(config: _LiveDatabricksConfig) -> dict[str, object]:
    return {
        "backend": "databricks",
        "catalog": config.catalog,
        "source_schema": config.source_schema,
    }


def _execute(
    connection: DatabricksConnection,
    sql: str,
    parameters: Sequence[object] | Mapping[str, object] = (),
) -> Any:
    return connection.execute(" ".join(sql.split()), parameters)


def _drop_schema(
    connection: DatabricksConnection,
    config: _LiveDatabricksConfig,
    schema: str,
) -> None:
    try:
        _execute(connection, f"drop schema if exists {_schema(config, schema)} cascade")
    except Exception:
        pass


def _schema(config: _LiveDatabricksConfig, schema: str) -> str:
    return f"`{config.catalog}`.`{schema}`"


def _relation(config: _LiveDatabricksConfig, schema: str, table: str) -> str:
    return f"{_schema(config, schema)}.`{table}`"


def _test_identifier(*parts: str) -> str:
    value = "_".join(part.strip("_").lower() for part in parts if part.strip("_"))
    if not _SIMPLE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Databricks live sandbox generated invalid schema `{value}`.")
    return value
