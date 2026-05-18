from __future__ import annotations

from pathlib import Path
from typing import Sequence

import retl
from retl.backends.duckdb import DUCKDB_DIALECT, DuckDBSqlBackend
from retl.backends.snowflake import SNOWFLAKE_DIALECT, SnowflakeBackendAuth, SnowflakeSqlBackend
from retl.stores.contracts import (
    CanonicalKeyScalar,
    EventKeysetScanPosition,
    EventSourceWindowHandle,
    StateSnapshotHandle,
)
from retl.stores.sql_runtime.collect import (
    compile_event_collect_window_query,
    compile_state_collect_snapshot_query,
)
from retl.stores.sql_runtime.context import SqlRuntimeContext


class _Connection:
    def execute(self, sql: str, parameters: Sequence[object] = ()) -> object:
        raise AssertionError("compile tests must not execute SQL")


def _backend(tmp_path: Path) -> DuckDBSqlBackend:
    return DuckDBSqlBackend(
        database=tmp_path / "warehouse.duckdb",
        source_schema="source_data",
        runtime_schema="retl",
    )


def _context(backend: DuckDBSqlBackend) -> SqlRuntimeContext:
    return SqlRuntimeContext(
        connection=_Connection(),
        dialect=DUCKDB_DIALECT,
        runtime_space=backend.runtime_space,
        collect_placement=backend.placement,
    )


def _snowflake_backend() -> SnowflakeSqlBackend:
    return SnowflakeSqlBackend(
        account="xy12345.us-east-1",
        warehouse="RETL_WH",
        source_database="SOURCE_DB",
        source_schema="APP",
        runtime_database="RETL_DB",
        runtime_schema="RETL_RUNTIME",
        auth=SnowflakeBackendAuth.from_namespace(
            auth_mode="password",
            credential_namespace="backends.snowflake.password",
        ),
    )


def _snowflake_context(backend: SnowflakeSqlBackend) -> SqlRuntimeContext:
    return SqlRuntimeContext(
        connection=_Connection(),
        dialect=SNOWFLAKE_DIALECT,
        runtime_space=backend.runtime_space,
        collect_placement=backend.placement,
    )


def test_state_collect_compile_uses_sqlglot_source_columns_and_temp_relation(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    declaration = retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="select customer_id, email, plan, audience_key from customers",
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    snapshot = StateSnapshotHandle(
        backend="duckdb",
        source_name="customers",
        source_identity={"backend": "duckdb"},
        query=declaration.source.query,
        source_space=backend.source_space,
    )

    compiled = compile_state_collect_snapshot_query(
        _context(backend),
        declaration=declaration,
        snapshot=snapshot,
    )

    assert compiled.params == ()
    assert 'create temporary table "temp"."retl_state_collect_snapshot" as' in compiled.sql
    assert 'source_rows."customer_id"' in compiled.sql
    assert 'source_rows."audience_key"' in compiled.sql
    assert "sha256(json_object(" in compiled.sql


def test_state_collect_compile_static_target_does_not_require_source_column(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    declaration = retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="select customer_id, email, plan from customers",
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target=retl.target("newsletter_customers"),
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    snapshot = StateSnapshotHandle(
        backend="duckdb",
        source_name="customers",
        source_identity={"backend": "duckdb"},
        query=declaration.source.query,
        source_space=backend.source_space,
    )

    compiled = compile_state_collect_snapshot_query(
        _context(backend),
        declaration=declaration,
        snapshot=snapshot,
    )

    assert "'newsletter_customers'" in compiled.sql
    assert '"newsletter_customers"' not in compiled.sql
    assert 'source_rows."email"' in compiled.sql


def test_event_collect_compile_preserves_keyset_parameter_order(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    declaration = retl.event(
        name="purchase_event",
        source=retl.source(
            name="purchases",
            query="select purchase_id, email, occurred_at, amount from purchases",
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
        identifiers=[{"type": "email", "value": "email"}],
        payload={"amount": "amount"},
    )
    scan_after = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-02T03:04:05"),
        primary_key_value=CanonicalKeyScalar.string("purchase_9"),
    )
    window = EventSourceWindowHandle(
        backend="duckdb",
        source_name="purchases",
        source_identity={"backend": "duckdb"},
        query=declaration.source.query,
        cursor_column="occurred_at",
        primary_key_column="purchase_id",
        scan_after=scan_after,
        source_space=backend.source_space,
        limit=25,
    )

    compiled = compile_event_collect_window_query(
        _context(backend),
        declaration=declaration,
        window=window,
    )

    assert compiled.params == (
        "2026-01-02T03:04:05",
        "2026-01-02T03:04:05",
        "purchase_9",
        25,
    )
    assert 'create temporary table "temp"."retl_event_collect_window" as' in compiled.sql
    assert 'source_rows."purchase_id"' in compiled.sql
    assert 'source_rows."occurred_at"' in compiled.sql
    assert 'as "retl_cursor"' in compiled.sql
    assert '"occurred_at" > ?' in compiled.sql
    assert '"purchase_id" > ?' in compiled.sql


def test_snowflake_event_collect_compile_uses_numeric_params_and_runtime_temp_relation() -> None:
    backend = _snowflake_backend()
    declaration = retl.event(
        name="purchase_event",
        source=retl.source(
            name="purchases",
            query="select purchase_id, email, occurred_at, amount from purchases",
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
        identifiers=[{"type": "email", "value": "email"}],
        payload={"amount": "amount"},
    )
    scan_after = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-02T03:04:05"),
        primary_key_value=CanonicalKeyScalar.string("purchase_9"),
    )
    window = EventSourceWindowHandle(
        backend="snowflake",
        source_name="purchases",
        source_identity={"backend": "snowflake"},
        query=declaration.source.query,
        cursor_column="occurred_at",
        primary_key_column="purchase_id",
        scan_after=scan_after,
        source_space=backend.source_space,
        limit=25,
    )

    compiled = compile_event_collect_window_query(
        _snowflake_context(backend),
        declaration=declaration,
        window=window,
    )

    assert compiled.params == (
        "2026-01-02T03:04:05",
        "2026-01-02T03:04:05",
        "purchase_9",
        25,
    )
    assert 'create temporary table "retl_event_collect_window" as' in compiled.sql
    assert '"occurred_at" > :1' in compiled.sql
    assert '"occurred_at" = :2' in compiled.sql
    assert '"purchase_id" > :3' in compiled.sql
    assert "LIMIT :4" in compiled.sql
    assert "object_construct_keep_null" in compiled.sql
    assert "to_json(" in compiled.sql
    assert 'source_rows."occurred_at"' in compiled.sql
    assert 'as "retl_cursor"' in compiled.sql


def test_snowflake_state_collect_compile_expands_list_identifier_without_subquery() -> None:
    backend = _snowflake_backend()
    declaration = retl.state(
        name="customer_email_list_state",
        source=retl.source(
            name="customer_email_lists",
            query="""
                select customer_id, emails, audience_key
                from customer_email_lists
            """,
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "values": "emails"}],
    )
    snapshot = StateSnapshotHandle(
        backend="snowflake",
        source_name="customer_email_lists",
        source_identity={"backend": "snowflake"},
        query=declaration.source.query,
        source_space=backend.source_space,
    )

    compiled = compile_state_collect_snapshot_query(
        _snowflake_context(backend),
        declaration=declaration,
        snapshot=snapshot,
    )

    assert 'transform(array_sort(source_rows."emails"), retl_identifier_value ->' in compiled.sql
    assert "from table(flatten" not in compiled.sql.lower()
    assert (
        "object_construct_keep_null('type', 'email', 'value', retl_identifier_value::string)"
        in (compiled.sql)
    )
