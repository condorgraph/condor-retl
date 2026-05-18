from __future__ import annotations

import ast
import inspect
import json
from collections.abc import Sequence
from typing import Any

import pytest
from sqlglot import exp

from retl.backends.duckdb import DUCKDB_DIALECT
from retl.sql import (
    RelationPath,
    SqlDialectCapabilities,
    SqlParamAllocator,
    row_write_input,
    runtime_upsert,
    upsert_assignment,
)
from retl.stores.contracts import SqlRelationSpace
from retl.stores.sql_runtime.schema import (
    _create_index_sql,
    _runtime_relation,
    _RuntimeIndex,
)


def test_duckdb_dialect_exposes_sqlglot_render_name_and_parameter_style() -> None:
    assert isinstance(DUCKDB_DIALECT, SqlDialectCapabilities)
    assert DUCKDB_DIALECT.name == "duckdb"
    assert DUCKDB_DIALECT.sqlglot_dialect == "duckdb"
    assert DUCKDB_DIALECT.placeholder(1) == "?"


def test_duckdb_relation_space_helpers_render_source_and_runtime_relations() -> None:
    source_space = SqlRelationSpace(
        backend_name="duckdb",
        database="warehouse.duckdb",
        schema="source",
        access="read_only",
    )
    runtime_space = SqlRelationSpace(
        backend_name="duckdb",
        database="warehouse.duckdb",
        schema="retl",
        access="read_write",
    )

    assert DUCKDB_DIALECT.source_relation(source_space, "customers") == RelationPath(
        "customers",
        schema="source",
    )
    assert DUCKDB_DIALECT.runtime_relation(runtime_space, "ordered_work") == RelationPath(
        "ordered_work",
        schema="retl",
    )
    assert DUCKDB_DIALECT.render_source_relation(source_space, "customers") == (
        '"source"."customers"'
    )
    assert DUCKDB_DIALECT.render_runtime_relation(runtime_space, "ordered_work") == (
        '"retl"."ordered_work"'
    )


def test_duckdb_schema_helpers_render_validated_runtime_relations_with_sqlglot() -> None:
    assert _runtime_relation("retl", "ordered_work", DUCKDB_DIALECT) == ('"retl"."ordered_work"')
    assert _create_index_sql(
        "retl",
        _RuntimeIndex(
            name="ordered_work_pending_idx",
            table="ordered_work",
            columns_sql="family, declaration_name",
        ),
        DUCKDB_DIALECT,
    ) == (
        'create index if not exists "ordered_work_pending_idx" '
        'on "retl"."ordered_work" (family, declaration_name)'
    )

    with pytest.raises(ValueError, match="simple SQL identifier"):
        _runtime_relation("bad-schema", "ordered_work", DUCKDB_DIALECT)
    with pytest.raises(ValueError, match="simple SQL identifier"):
        _runtime_relation("retl", "ordered-work", DUCKDB_DIALECT)


def test_duckdb_relation_space_helpers_validate_backend_access_and_schema() -> None:
    with pytest.raises(ValueError, match="backend must be duckdb"):
        DUCKDB_DIALECT.source_relation(
            SqlRelationSpace(
                backend_name="snowflake",
                database="warehouse",
                schema="source",
                access="read_only",
            ),
            "customers",
        )

    with pytest.raises(ValueError, match="access must be read_write"):
        DUCKDB_DIALECT.runtime_relation(
            SqlRelationSpace(
                backend_name="duckdb",
                database="warehouse.duckdb",
                schema="source",
                access="read_only",
            ),
            "ordered_work",
        )

    with pytest.raises(ValueError, match="simple SQL identifier"):
        DUCKDB_DIALECT.source_relation(
            SqlRelationSpace(
                backend_name="duckdb",
                database="warehouse.duckdb",
                schema="bad-schema",
                access="read_only",
            ),
            "customers",
        )


def test_duckdb_schema_context_sql_uses_validated_string_literal() -> None:
    assert DUCKDB_DIALECT.current_schema_sql() == "select current_schema()"
    assert DUCKDB_DIALECT.set_schema_sql("source") == "set schema 'source'"

    with pytest.raises(ValueError, match="simple SQL identifier"):
        DUCKDB_DIALECT.set_schema_sql("source; drop schema retl")


def test_duckdb_capability_helpers_render_backend_specific_sql() -> None:
    assert DUCKDB_DIALECT.json_object_sql({"id": '"id"', "name": '"name"'}) == (
        "json_object('id', \"id\", 'name', \"name\")"
    )
    assert DUCKDB_DIALECT.json_extract_scalar_sql('"payload"', "$.id") == (
        "json_extract_string(\"payload\", '$.id')"
    )
    assert DUCKDB_DIALECT.sha256_sql('"payload"') == 'sha256("payload"::varchar)'
    assert DUCKDB_DIALECT.render_temp_relation("retl_state_collect_snapshot") == (
        '"temp"."retl_state_collect_snapshot"'
    )
    assert DUCKDB_DIALECT.create_temp_table_as_sql("retl_state_collect_snapshot", "select 1") == (
        'create temporary table "temp"."retl_state_collect_snapshot" as select 1'
    )
    assert DUCKDB_DIALECT.drop_temp_table_sql("retl_state_collect_snapshot") == (
        'drop table if exists "temp"."retl_state_collect_snapshot"'
    )
    assert DUCKDB_DIALECT.limit_sql("select * from rows", "?") == "select * from rows limit ?"
    assert DUCKDB_DIALECT.begin_transaction_sql() == "begin transaction"
    assert DUCKDB_DIALECT.commit_sql() == "commit"
    assert DUCKDB_DIALECT.rollback_sql() == "rollback"


def test_duckdb_upsert_sql_uses_on_conflict_with_sqlglot_row_pieces() -> None:
    params = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)
    upsert = runtime_upsert(
        RelationPath("destination_progress", schema="retl"),
        row_write_input(
            [
                ("declaration_name", "customers"),
                ("position_json", '{"id": 7}'),
                ("active", True),
            ],
            params=params,
        ),
        key_columns=["declaration_name"],
        update_columns=["position_json"],
        update_assignments=[
            upsert_assignment("active", exp.Boolean(this=True)),
            upsert_assignment("last_seen_at", exp.Var(this="current_timestamp")),
        ],
    )

    assert DUCKDB_DIALECT.upsert_sql(upsert) == (
        'INSERT INTO "retl"."destination_progress" '
        '("declaration_name", "position_json", "active") VALUES (?, ?, ?) '
        'ON CONFLICT ("declaration_name") DO UPDATE SET '
        '"position_json" = excluded."position_json", '
        '"active" = TRUE, '
        '"last_seen_at" = current_timestamp'
    )
    assert params.params == ("customers", '{"id": 7}', True)


def test_duckdb_upsert_sql_supports_conflict_do_nothing() -> None:
    params = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)
    upsert = runtime_upsert(
        RelationPath("destination_progress", schema="retl"),
        row_write_input([("declaration_name", "customers")], params=params),
        key_columns=["declaration_name"],
    )

    assert DUCKDB_DIALECT.upsert_sql(upsert) == (
        'INSERT INTO "retl"."destination_progress" ("declaration_name") VALUES (?) '
        'ON CONFLICT ("declaration_name") DO NOTHING'
    )
    assert params.params == ("customers",)


class RecordingConnection:
    def __init__(self) -> None:
        self.schema = "main"
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        self.calls.append((sql, tuple(parameters)))
        if sql == "select current_schema()":
            return self
        if sql == "set schema 'source'":
            self.schema = "source"
            return self
        if sql == "set schema 'main'":
            self.schema = "main"
            return self
        raise AssertionError(sql)

    def fetchone(self) -> tuple[str]:
        return (self.schema,)


class FakeRawConnection:
    def __init__(self) -> None:
        self.closed = False
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> FakeRawConnection:
        self.calls.append((sql, tuple(parameters)))
        return self

    def close(self) -> None:
        self.closed = True


def test_duckdb_schema_context_restores_previous_schema() -> None:
    connection = RecordingConnection()

    with DUCKDB_DIALECT.schema_context(connection, "source"):
        assert connection.schema == "source"

    assert connection.schema == "main"
    assert connection.calls == [
        ("select current_schema()", ()),
        ("set schema 'source'", ()),
        ("set schema 'main'", ()),
    ]


def test_duckdb_connection_module_has_no_top_level_driver_import() -> None:
    import retl.backends.duckdb.connection as duckdb_connection_module

    tree = ast.parse(inspect.getsource(duckdb_connection_module))
    top_level_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.Import | ast.ImportFrom)
        and (
            any(alias.name == "duckdb" for alias in getattr(node, "names", ()))
            or getattr(node, "module", None) == "duckdb"
        )
    ]

    assert top_level_imports == []


def test_duckdb_connection_adapter_wraps_existing_connection_without_opening_driver() -> None:
    from retl.backends.duckdb import DuckDBConnection

    raw_connection = FakeRawConnection()
    connection = DuckDBConnection(connection=raw_connection)
    result = connection.execute("select ? as value", [1])
    connection.close()

    assert result is raw_connection
    assert raw_connection.calls == [("select ? as value", (1,))]
    assert raw_connection.closed


def test_duckdb_connection_adapter_translates_missing_optional_driver() -> None:
    from retl.backends.duckdb.connection import DuckDBConnectionError, _duckdb_driver

    def missing_duckdb_import(name: str) -> Any:
        if name == "duckdb":
            raise ImportError("No module named duckdb")
        raise AssertionError(name)

    with pytest.raises(DuckDBConnectionError, match="optional `duckdb` dependency"):
        _duckdb_driver(import_module=missing_duckdb_import)


def test_duckdb_connection_adapter_executes_parameterized_sql() -> None:
    pytest.importorskip("duckdb")
    from retl.backends.duckdb import DuckDBConnection

    connection = DuckDBConnection(":memory:")
    try:
        record = connection.execute("select ?::integer as value", [7]).fetchone()

        assert record == (7,)
    finally:
        connection.close()


def test_duckdb_capabilities_execute_live_json_hash_and_transaction_sql() -> None:
    pytest.importorskip("duckdb")
    from retl.backends.duckdb import DuckDBConnection

    connection = DuckDBConnection(":memory:")
    try:
        DUCKDB_DIALECT.begin_transaction(connection)
        connection.execute("create table capability_rows (id integer, payload varchar)")
        connection.execute("insert into capability_rows values (?, ?)", [1, "Ada"])
        DUCKDB_DIALECT.commit(connection)

        json_sql = DUCKDB_DIALECT.json_object_sql({"id": '"id"', "payload": '"payload"'})
        record = connection.execute(
            f"select {json_sql}, {DUCKDB_DIALECT.sha256_sql(json_sql)} from capability_rows"
        ).fetchone()

        assert json.loads(record[0]) == {"id": 1, "payload": "Ada"}
        assert len(record[1]) == 64
    finally:
        connection.close()
