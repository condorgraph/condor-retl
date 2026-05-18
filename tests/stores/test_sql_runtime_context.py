from __future__ import annotations

import importlib
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from sqlglot import exp, select

from retl.backends.duckdb import DUCKDB_DIALECT, DuckDBSqlBackend
from retl.sql import RelationPath, SqlParamAllocator, render_sql, table
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace
from retl.stores.sql_runtime import SqlRuntimeContext, SqlRuntimeStore

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SNOWFLAKE_DIALECT_MODULE_EXISTS = (
    _REPO_ROOT / "src" / "retl" / "backends" / "snowflake" / "dialect.py"
).exists()

requires_step5_snowflake_dialect = pytest.mark.xfail(
    not _SNOWFLAKE_DIALECT_MODULE_EXISTS,
    reason="Step 5 adds SnowflakeSqlDialect support for SqlRuntimeContext.",
    strict=True,
)


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        self.calls.append((sql, tuple(parameters)))
        return self


def _runtime_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="duckdb",
        database="warehouse.duckdb",
        schema="retl",
        access="read_write",
    )


def _source_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="duckdb",
        database="warehouse.duckdb",
        schema="source",
        access="read_only",
    )


def _context(*, collect: bool = False) -> SqlRuntimeContext:
    runtime_space = _runtime_space()
    placement = (
        SqlCollectPlacement(source=_source_space(), runtime=runtime_space) if collect else None
    )
    return SqlRuntimeContext(
        connection=RecordingConnection(),
        dialect=DUCKDB_DIALECT,
        runtime_space=runtime_space,
        collect_placement=placement,
    )


def _snowflake_runtime_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="snowflake",
        database="RETL_DB",
        schema="RETL_RUNTIME",
        access="read_write",
    )


def _snowflake_source_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="snowflake",
        database="SOURCE_DB",
        schema="APP",
        access="read_only",
    )


def _snowflake_context(*, collect: bool = False) -> SqlRuntimeContext:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SNOWFLAKE_DIALECT = snowflake_module.SNOWFLAKE_DIALECT  # type: ignore[attr-defined]

    runtime_space = _snowflake_runtime_space()
    placement = (
        SqlCollectPlacement(source=_snowflake_source_space(), runtime=runtime_space)
        if collect
        else None
    )
    return SqlRuntimeContext(
        connection=RecordingConnection(),
        dialect=SNOWFLAKE_DIALECT,
        runtime_space=runtime_space,
        collect_placement=placement,
    )


def test_context_exposes_validated_connection_dialect_and_runtime_space() -> None:
    connection = RecordingConnection()
    runtime_space = _runtime_space()

    context = SqlRuntimeContext(
        connection=connection,
        dialect=DUCKDB_DIALECT,
        runtime_space=runtime_space,
    )

    assert context.connection is connection
    assert context.dialect is DUCKDB_DIALECT
    assert context.sqlglot_dialect == "duckdb"
    assert context.runtime_space == runtime_space
    assert context.collect_placement is None


def test_context_rejects_non_runtime_relation_space() -> None:
    with pytest.raises(ValueError, match="runtime_space access must be read_write"):
        SqlRuntimeContext(
            connection=RecordingConnection(),
            dialect=DUCKDB_DIALECT,
            runtime_space=SqlRelationSpace(
                backend_name="duckdb",
                database="warehouse.duckdb",
                schema="source",
                access="read_only",
            ),
        )


def test_context_rejects_collect_placement_for_different_runtime_space() -> None:
    with pytest.raises(ValueError, match="collect_placement runtime must match runtime_space"):
        SqlRuntimeContext(
            connection=RecordingConnection(),
            dialect=DUCKDB_DIALECT,
            runtime_space=_runtime_space(),
            collect_placement=SqlCollectPlacement(
                source=_source_space(),
                runtime=SqlRelationSpace(
                    backend_name="duckdb",
                    database="warehouse.duckdb",
                    schema="other_runtime",
                    access="read_write",
                ),
            ),
        )


def test_runtime_relation_helpers_render_runtime_space() -> None:
    context = _context()

    assert context.runtime_relation("ordered_work") == RelationPath(
        "ordered_work",
        schema="retl",
    )
    assert context.render_runtime_relation("ordered_work") == '"retl"."ordered_work"'


def test_source_relation_helpers_require_collect_placement() -> None:
    context = _context()

    with pytest.raises(ValueError, match="source relations require collect placement"):
        context.source_relation("customers")


def test_source_relation_helpers_render_collect_source_space_when_placement_exists() -> None:
    context = _context(collect=True)

    assert context.source_relation("customers") == RelationPath("customers", schema="source")
    assert context.render_source_relation("customers") == '"source"."customers"'


def test_context_parameter_allocator_uses_dialect_parameter_style() -> None:
    context = _context()
    allocator = context.new_params()

    assert isinstance(allocator, SqlParamAllocator)
    assert allocator.style == DUCKDB_DIALECT.parameter_style

    family = allocator.add("state")
    limit = allocator.add(10)
    expression = (
        select("*")
        .from_(table(context.runtime_relation("ordered_work")))
        .where(exp.EQ(this=exp.column("family", quoted=True), expression=family))
        .limit(limit)
    )
    compiled = render_sql(expression, dialect=context.dialect, params=allocator)

    assert compiled.sql == ('SELECT * FROM "retl"."ordered_work" WHERE "family" = ? LIMIT ?')
    assert compiled.params == ("state", 10)


def test_transaction_context_commits_after_successful_body() -> None:
    connection = RecordingConnection()
    context = SqlRuntimeContext(
        connection=connection,
        dialect=DUCKDB_DIALECT,
        runtime_space=_runtime_space(),
    )

    with context.transaction():
        context.connection.execute("insert into rows values (?)", [1])

    assert connection.calls == [
        ("begin transaction", ()),
        ("insert into rows values (?)", (1,)),
        ("commit", ()),
    ]


def test_transaction_context_rolls_back_and_reraises_after_failure() -> None:
    connection = RecordingConnection()
    context = SqlRuntimeContext(
        connection=connection,
        dialect=DUCKDB_DIALECT,
        runtime_space=_runtime_space(),
    )

    with pytest.raises(RuntimeError, match="boom"):
        with context.transaction():
            context.connection.execute("insert into rows values (?)", [1])
            raise RuntimeError("boom")

    assert connection.calls == [
        ("begin transaction", ()),
        ("insert into rows values (?)", (1,)),
        ("rollback", ()),
    ]


def test_temp_table_helpers_delegate_to_dialect_capability_sql() -> None:
    connection = RecordingConnection()
    context = SqlRuntimeContext(
        connection=connection,
        dialect=DUCKDB_DIALECT,
        runtime_space=_runtime_space(),
    )

    assert context.temp_relation("retl_state_collect_snapshot") == RelationPath(
        "retl_state_collect_snapshot",
        schema="temp",
    )
    assert context.render_temp_relation("retl_state_collect_snapshot") == (
        '"temp"."retl_state_collect_snapshot"'
    )
    assert context.create_temp_table_as_sql("retl_state_collect_snapshot", "select 1") == (
        'create temporary table "temp"."retl_state_collect_snapshot" as select 1'
    )
    assert context.drop_temp_table_sql("retl_state_collect_snapshot") == (
        'drop table if exists "temp"."retl_state_collect_snapshot"'
    )

    context.create_temp_table_as("retl_state_collect_snapshot", "select 1")
    context.drop_temp_table("retl_state_collect_snapshot")
    context.cleanup_temp_tables(["scratch_a", "scratch_b"])

    assert connection.calls == [
        ('create temporary table "temp"."retl_state_collect_snapshot" as select 1', ()),
        ('drop table if exists "temp"."retl_state_collect_snapshot"', ()),
        ('drop table if exists "temp"."scratch_a"', ()),
        ('drop table if exists "temp"."scratch_b"', ()),
    ]


def test_duckdb_backend_runtime_store_constructs_shared_context(tmp_path) -> None:
    backend = DuckDBSqlBackend(
        database=tmp_path / "warehouse.duckdb",
        source_schema="source",
        runtime_schema="retl_runtime",
    )

    store = backend.runtime_store()
    try:
        assert isinstance(store, SqlRuntimeStore)
        assert type(store).allocate_collect_id is SqlRuntimeStore.allocate_collect_id
        assert type(store).begin_attempt is SqlRuntimeStore.begin_attempt

        context = store._runtime_context  # noqa: SLF001

        assert isinstance(context, SqlRuntimeContext)
        assert context.dialect is DUCKDB_DIALECT
        assert context.sqlglot_dialect == "duckdb"
        assert context.runtime_space == backend.runtime_space
        assert context.collect_placement == backend.placement
    finally:
        store.close()


@requires_step5_snowflake_dialect
def test_snowflake_context_relation_helpers_render_database_and_schema_qualified_spaces() -> None:
    context = _snowflake_context(collect=True)

    assert context.runtime_relation("ordered_work") == RelationPath(
        "ordered_work",
        schema="RETL_RUNTIME",
        database="RETL_DB",
    )
    assert context.source_relation("customers") == RelationPath(
        "customers",
        schema="APP",
        database="SOURCE_DB",
    )
    assert context.render_runtime_relation("ordered_work") == (
        '"RETL_DB"."RETL_RUNTIME"."ordered_work"'
    )
    assert context.render_source_relation("customers") == '"SOURCE_DB"."APP"."customers"'


@requires_step5_snowflake_dialect
def test_snowflake_context_parameter_allocator_uses_dialect_numeric_parameter_style() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SNOWFLAKE_DIALECT = snowflake_module.SNOWFLAKE_DIALECT  # type: ignore[attr-defined]

    context = _snowflake_context()
    allocator = context.new_params()

    assert isinstance(allocator, SqlParamAllocator)
    assert allocator.style == SNOWFLAKE_DIALECT.parameter_style

    family = allocator.add("state")
    limit = allocator.add(10)
    expression = (
        select("*")
        .from_(table(context.runtime_relation("ordered_work")))
        .where(exp.EQ(this=exp.column("family", quoted=True), expression=family))
        .limit(limit)
    )
    compiled = render_sql(expression, dialect=context.dialect, params=allocator)

    assert compiled.sql == (
        'SELECT * FROM "RETL_DB"."RETL_RUNTIME"."ordered_work" WHERE "FAMILY" = :1 LIMIT :2'
    )
    assert compiled.params == ("state", 10)


@requires_step5_snowflake_dialect
def test_snowflake_context_temp_table_helpers_delegate_to_dialect_capability_sql() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SNOWFLAKE_DIALECT = snowflake_module.SNOWFLAKE_DIALECT  # type: ignore[attr-defined]

    connection = RecordingConnection()
    context = SqlRuntimeContext(
        connection=connection,
        dialect=SNOWFLAKE_DIALECT,
        runtime_space=_snowflake_runtime_space(),
    )

    assert context.temp_relation("retl_state_collect_snapshot") == RelationPath(
        "retl_state_collect_snapshot"
    )
    assert context.render_temp_relation("retl_state_collect_snapshot") == (
        '"retl_state_collect_snapshot"'
    )
    assert context.create_temp_table_as_sql("retl_state_collect_snapshot", "select 1") == (
        'create temporary table "retl_state_collect_snapshot" as select 1'
    )
    assert context.drop_temp_table_sql("retl_state_collect_snapshot") == (
        'drop table if exists "retl_state_collect_snapshot"'
    )

    context.create_temp_table_as("retl_state_collect_snapshot", "select 1")
    context.drop_temp_table("retl_state_collect_snapshot")

    assert connection.calls == [
        ('create temporary table "retl_state_collect_snapshot" as select 1', ()),
        ('drop table if exists "retl_state_collect_snapshot"', ()),
    ]


@requires_step5_snowflake_dialect
def test_snowflake_context_transaction_delegates_to_dialect() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SNOWFLAKE_DIALECT = snowflake_module.SNOWFLAKE_DIALECT  # type: ignore[attr-defined]

    connection = RecordingConnection()
    context = SqlRuntimeContext(
        connection=connection,
        dialect=SNOWFLAKE_DIALECT,
        runtime_space=_snowflake_runtime_space(),
    )

    with context.transaction():
        context.connection.execute("insert into rows values (:1)", [1])

    assert connection.calls == [
        ("begin", ()),
        ("insert into rows values (:1)", (1,)),
        ("commit", ()),
    ]
