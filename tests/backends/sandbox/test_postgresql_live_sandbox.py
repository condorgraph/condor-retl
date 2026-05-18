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
from retl.auth import AuthResolutionError, EnvironmentSecretResolver
from retl.backends.postgresql import (
    PostgreSqlBackend,
    PostgreSqlBackendAuth,
    PostgreSqlConnection,
    PostgreSqlConnectionError,
)
from retl.backends.postgresql.auth import postgresql_auth_connect_kwargs
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
_OPTIONAL_DEPENDENCY_MESSAGE = "optional `psycopg` dependency"


@dataclass(frozen=True)
class _LivePostgreSqlConfig:
    host: str
    port: int
    database: str
    source_schema: str
    runtime_schema: str
    auth: PostgreSqlBackendAuth


@dataclass(frozen=True)
class _LivePostgreSqlSandbox:
    config: _LivePostgreSqlConfig
    backend: PostgreSqlBackend
    admin_connection: PostgreSqlConnection


@pytest.fixture(scope="module")
def live_postgresql_sandbox() -> Iterator[_LivePostgreSqlSandbox]:
    config = _live_postgresql_config()
    admin_connection = _open_admin_connection(config)
    try:
        _execute(admin_connection, f"create schema if not exists {_schema(config.source_schema)}")
        _execute(admin_connection, f"create schema if not exists {_schema(config.runtime_schema)}")
        backend = PostgreSqlBackend(
            host=config.host,
            port=config.port,
            database=config.database,
            source_schema=config.source_schema,
            runtime_schema=config.runtime_schema,
            auth=config.auth,
        )
        _create_source_tables(admin_connection, config)
        yield _LivePostgreSqlSandbox(
            config=config,
            backend=backend,
            admin_connection=admin_connection,
        )
    finally:
        _drop_schema(admin_connection, config.runtime_schema)
        _drop_schema(admin_connection, config.source_schema)
        admin_connection.close()


def test_postgresql_live_sandbox_schema_collect_reads_and_runtime_cli(
    live_postgresql_sandbox: _LivePostgreSqlSandbox,
) -> None:
    sandbox = live_postgresql_sandbox
    store = sandbox.backend.runtime_store()
    try:
        _assert_runtime_schema_initialized(sandbox.admin_connection, sandbox.config)

        state_result = store.produce_state_collect(
            declaration=_state_declaration(sandbox.backend, sandbox.config),
            snapshot=StateSnapshotHandle(
                backend="postgresql",
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
                backend="postgresql",
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
                backend="postgresql",
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
        first_event_page = store.read_event_source_window(
            declaration=_event_declaration(sandbox.backend, sandbox.config),
            window=EventSourceWindowHandle(
                backend="postgresql",
                source_name="purchases",
                source_identity=_source_identity(sandbox.config),
                query=_purchases_query(sandbox.config),
                cursor_column="occurred_at",
                primary_key_column="purchase_id",
                source_space=sandbox.backend.source_space,
                limit=2,
            ),
            max_rows=2,
        )
        assert first_event_page.row_count == 2

        second_event_result = store.produce_event_collect(
            declaration=_event_declaration(sandbox.backend, sandbox.config),
            window=EventSourceWindowHandle(
                backend="postgresql",
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
        second_event_page = store.read_event_source_window(
            declaration=_event_declaration(sandbox.backend, sandbox.config),
            window=EventSourceWindowHandle(
                backend="postgresql",
                source_name="purchases",
                source_identity=_source_identity(sandbox.config),
                query=_purchases_query(sandbox.config),
                cursor_column="occurred_at",
                primary_key_column="purchase_id",
                scan_after=first_event_result.scan_upper_bound,
                source_space=sandbox.backend.source_space,
                limit=2,
            ),
            max_rows=2,
        )
        assert _json_column_values(first_event_page.payload, "key_json") + _json_column_values(
            second_event_page.payload,
            "key_json",
        ) == [
            {"purchase": "purchase_1"},
            {"purchase": "purchase_2"},
            {"purchase": "purchase_3"},
        ]

        _assert_collect_rows_written(sandbox.admin_connection, sandbox.config)
        _assert_arrow_read_path(sandbox.admin_connection, sandbox.config)
        assert_event_keyset_skip_operation(store)
        assert_runtime_cleanup_operations(store)
        assert_runtime_inspect_reset_operations(store)
    finally:
        store.close()


def _live_postgresql_config() -> _LivePostgreSqlConfig:
    required = {
        "BACKENDS__POSTGRESQL__HOST": os.environ.get("BACKENDS__POSTGRESQL__HOST"),
        "BACKENDS__POSTGRESQL__PORT": os.environ.get("BACKENDS__POSTGRESQL__PORT"),
        "BACKENDS__POSTGRESQL__DATABASE": os.environ.get("BACKENDS__POSTGRESQL__DATABASE"),
    }
    missing = sorted(name for name, value in required.items() if not value)
    if missing:
        pytest.skip("PostgreSQL live sandbox config is absent; missing " + ", ".join(missing))

    auth_mode = os.environ.get("BACKENDS__POSTGRESQL__AUTH_MODE", "password").strip()
    auth_mode = auth_mode or "password"
    credential_namespace = f"backends.postgresql.{auth_mode}"
    auth = PostgreSqlBackendAuth.from_namespace(
        auth_mode=auth_mode,
        credential_namespace=credential_namespace,
    )
    try:
        postgresql_auth_connect_kwargs(auth, resolver=EnvironmentSecretResolver())
    except AuthResolutionError as exc:
        pytest.skip(f"PostgreSQL live sandbox authentication is absent; {exc}")

    prefix = os.environ.get("RETL_POSTGRESQL_SANDBOX_SCHEMA_PREFIX", "retl_live")
    suffix = uuid.uuid4().hex[:10]
    worker = re.sub(r"[^A-Za-z0-9_]", "_", os.environ.get("PYTEST_XDIST_WORKER", "gw0")).lower()
    source_schema = _test_identifier(prefix, worker, suffix, "src")
    runtime_schema = _test_identifier(prefix, worker, suffix, "rt")

    return _LivePostgreSqlConfig(
        host=str(required["BACKENDS__POSTGRESQL__HOST"]).strip(),
        port=int(str(required["BACKENDS__POSTGRESQL__PORT"]).strip()),
        database=_required_identifier(str(required["BACKENDS__POSTGRESQL__DATABASE"]), "database"),
        source_schema=source_schema,
        runtime_schema=runtime_schema,
        auth=auth,
    )


def _open_admin_connection(config: _LivePostgreSqlConfig) -> PostgreSqlConnection:
    try:
        return PostgreSqlConnection(
            host=config.host,
            port=config.port,
            dbname=config.database,
            autocommit=True,
            connect_kwargs=postgresql_auth_connect_kwargs(
                config.auth,
                resolver=EnvironmentSecretResolver(),
            ),
        )
    except PostgreSqlConnectionError as exc:
        if _OPTIONAL_DEPENDENCY_MESSAGE in str(exc):
            pytest.skip("PostgreSQL optional dependency is not installed.")
        raise


def _create_source_tables(
    connection: PostgreSqlConnection,
    config: _LivePostgreSqlConfig,
) -> None:
    customers = _relation(config.source_schema, "customers")
    purchases = _relation(config.source_schema, "purchases")
    _execute(
        connection,
        f"""
        create table {customers} (
            customer_id text,
            email text,
            plan text,
            audience_key text
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
            purchase_id text,
            email text,
            occurred_at text,
            amount numeric(10, 2)
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
    connection: PostgreSqlConnection,
    config: _LivePostgreSqlConfig,
) -> None:
    customers = _relation(config.source_schema, "customers")
    _execute(connection, f"delete from {customers}")
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


def _state_declaration(backend: PostgreSqlBackend, config: _LivePostgreSqlConfig) -> object:
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


def _event_declaration(backend: PostgreSqlBackend, config: _LivePostgreSqlConfig) -> object:
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
    connection: PostgreSqlConnection,
    config: _LivePostgreSqlConfig,
) -> None:
    rows = _execute(
        connection,
        """
        select table_name
        from information_schema.tables
        where table_schema = %s
        """,
        (config.runtime_schema,),
    ).fetchall()
    created_tables = {str(row[0]).casefold() for row in rows}
    expected_tables = {name.casefold() for name in RUNTIME_TABLE_CATALOG}
    assert expected_tables <= created_tables

    pgcrypto = _execute(
        connection,
        "select exists (select 1 from pg_extension where extname = %s)",
        ("pgcrypto",),
    ).fetchone()
    assert pgcrypto is not None
    assert bool(pgcrypto[0]) is True


def _assert_collect_rows_written(
    connection: PostgreSqlConnection,
    config: _LivePostgreSqlConfig,
) -> None:
    ordered_work_count = _execute(
        connection,
        f"""
        select count(*)
        from {_relation(config.runtime_schema, "ordered_work")}
        where declaration_name in (%s, %s)
        """,
        ("customer_state", "purchase_event"),
    ).fetchone()
    assert ordered_work_count is not None
    assert int(ordered_work_count[0]) == 10

    state_current_count = _execute(
        connection,
        f"""
        select count(*)
        from {_relation(config.runtime_schema, "state_current")}
        where declaration_name = %s
          and source_name = %s
        """,
        ("customer_state", "customers"),
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
    connection: PostgreSqlConnection,
    config: _LivePostgreSqlConfig,
) -> None:
    result = _execute(
        connection,
        f"""
        select declaration_name, count(*) as row_count
        from {_relation(config.runtime_schema, "ordered_work")}
        group by declaration_name
        order by declaration_name
        """,
    )
    table = result.fetch_arrow_all()
    assert table.column_names == ["declaration_name", "row_count"]
    assert table.num_rows == 1
    assert table.column("declaration_name").to_pylist() == ["customer_state"]
    assert table.column("row_count").to_pylist() == [10]


def _column_values(batch: Any, column: str) -> list[Any]:
    return batch.column(column).to_pylist()


def _json_column_values(batch: Any, column: str) -> list[Any]:
    return [json.loads(value) if value else None for value in _column_values(batch, column)]


def _customers_query(config: _LivePostgreSqlConfig) -> str:
    return (
        "select customer_id, email, plan, audience_key "
        f"from {_relation(config.source_schema, 'customers')}"
    )


def _purchases_query(config: _LivePostgreSqlConfig) -> str:
    return (
        "select purchase_id, email, occurred_at, amount "
        f"from {_relation(config.source_schema, 'purchases')}"
    )


def _source_identity(config: _LivePostgreSqlConfig) -> dict[str, object]:
    return {
        "backend": "postgresql",
        "database": config.database,
        "source_schema": config.source_schema,
    }


def _execute(
    connection: PostgreSqlConnection,
    sql: str,
    parameters: Sequence[object] | Mapping[str, object] = (),
) -> Any:
    return connection.execute(" ".join(sql.split()), parameters)


def _drop_schema(connection: PostgreSqlConnection, schema: str) -> None:
    try:
        _execute(connection, f"drop schema if exists {_schema(schema)} cascade")
    except Exception:
        pass


def _schema(schema: str) -> str:
    return f'"{schema}"'


def _relation(schema: str, table: str) -> str:
    return f'{_schema(schema)}."{table}"'


def _required_identifier(value: str, label: str) -> str:
    value = value.strip()
    if not _SIMPLE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"PostgreSQL live sandbox {label} must be a simple SQL identifier.")
    return value


def _test_identifier(*parts: str) -> str:
    value = "_".join(part.strip("_").lower() for part in parts if part.strip("_"))
    if not _SIMPLE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"PostgreSQL live sandbox generated invalid schema `{value}`.")
    return value
