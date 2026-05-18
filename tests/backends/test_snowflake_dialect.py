from __future__ import annotations

import importlib
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from sqlglot import exp

from retl.sql import (
    RelationPath,
    SqlDialectCapabilities,
    SqlParamAllocator,
    SqlParameterStyle,
    row_write_input,
    runtime_upsert,
    upsert_assignment,
)
from retl.stores.contracts import SqlRelationSpace

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SNOWFLAKE_PACKAGE = _REPO_ROOT / "src" / "retl" / "backends" / "snowflake"
_DIALECT_MODULE_EXISTS = (_SNOWFLAKE_PACKAGE / "dialect.py").exists()

requires_step5_dialect = pytest.mark.xfail(
    not _DIALECT_MODULE_EXISTS,
    reason="Step 5 adds SnowflakeSqlDialect and Snowflake SQL capability helpers.",
    strict=True,
)


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchone_row: tuple[object, ...] | None = ("RETL_DB", "RETL_RUNTIME")

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        self.calls.append((sql, tuple(parameters)))
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.fetchone_row


def _snowflake_dialect() -> Any:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    return snowflake_module.SNOWFLAKE_DIALECT  # type: ignore[attr-defined]


def _source_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="snowflake",
        database="SOURCE_DB",
        schema="APP",
        access="read_only",
    )


def _runtime_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="snowflake",
        database="RETL_DB",
        schema="RETL_RUNTIME",
        access="read_write",
    )


@requires_step5_dialect
def test_snowflake_dialect_exposes_sqlglot_render_name_and_parameter_style() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SNOWFLAKE_DIALECT = snowflake_module.SNOWFLAKE_DIALECT  # type: ignore[attr-defined]
    SnowflakeSqlDialect = snowflake_module.SnowflakeSqlDialect  # type: ignore[attr-defined]

    assert isinstance(SNOWFLAKE_DIALECT, SnowflakeSqlDialect)
    assert isinstance(SNOWFLAKE_DIALECT, SqlDialectCapabilities)
    assert SNOWFLAKE_DIALECT.name == "snowflake"
    assert SNOWFLAKE_DIALECT.sqlglot_dialect == "snowflake"
    assert SNOWFLAKE_DIALECT.parameter_style is SqlParameterStyle.NUMERIC
    assert SNOWFLAKE_DIALECT.placeholder(2) == ":2"


@requires_step5_dialect
def test_snowflake_relation_space_helpers_render_database_and_schema_qualified_relations() -> None:
    SNOWFLAKE_DIALECT = _snowflake_dialect()

    assert SNOWFLAKE_DIALECT.source_relation(_source_space(), "customers") == RelationPath(
        "customers",
        schema="APP",
        database="SOURCE_DB",
    )
    assert SNOWFLAKE_DIALECT.runtime_relation(_runtime_space(), "ordered_work") == RelationPath(
        "ordered_work",
        schema="RETL_RUNTIME",
        database="RETL_DB",
    )
    assert SNOWFLAKE_DIALECT.render_source_relation(_source_space(), "customers") == (
        '"SOURCE_DB"."APP"."customers"'
    )
    assert SNOWFLAKE_DIALECT.render_runtime_relation(_runtime_space(), "ordered_work") == (
        '"RETL_DB"."RETL_RUNTIME"."ordered_work"'
    )


@requires_step5_dialect
def test_snowflake_relation_space_helpers_validate_backend_access_and_identifiers() -> None:
    SNOWFLAKE_DIALECT = _snowflake_dialect()

    with pytest.raises(ValueError, match="backend must be snowflake"):
        SNOWFLAKE_DIALECT.source_relation(
            SqlRelationSpace(
                backend_name="duckdb",
                database="SOURCE_DB",
                schema="APP",
                access="read_only",
            ),
            "customers",
        )

    with pytest.raises(ValueError, match="access must be read_write"):
        SNOWFLAKE_DIALECT.runtime_relation(
            SqlRelationSpace(
                backend_name="snowflake",
                database="RETL_DB",
                schema="APP",
                access="read_only",
            ),
            "ordered_work",
        )

    with pytest.raises(ValueError, match="simple SQL identifier"):
        SNOWFLAKE_DIALECT.source_relation(
            SqlRelationSpace(
                backend_name="snowflake",
                database="SOURCE-DB",
                schema="APP",
                access="read_only",
            ),
            "customers",
        )


@requires_step5_dialect
def test_snowflake_temp_table_helpers_render_session_scoped_temp_relations() -> None:
    SNOWFLAKE_DIALECT = _snowflake_dialect()

    assert SNOWFLAKE_DIALECT.temp_relation("retl_state_collect_snapshot") == RelationPath(
        "retl_state_collect_snapshot"
    )
    assert SNOWFLAKE_DIALECT.render_temp_relation("retl_state_collect_snapshot") == (
        '"retl_state_collect_snapshot"'
    )
    assert (
        SNOWFLAKE_DIALECT.create_temp_table_as_sql(
            "retl_state_collect_snapshot",
            "select 1",
        )
        == 'create temporary table "retl_state_collect_snapshot" as select 1'
    )
    assert SNOWFLAKE_DIALECT.drop_temp_table_sql("retl_state_collect_snapshot") == (
        'drop table if exists "retl_state_collect_snapshot"'
    )


@requires_step5_dialect
def test_snowflake_transaction_methods_delegate_to_recording_connection() -> None:
    SNOWFLAKE_DIALECT = _snowflake_dialect()

    connection = RecordingConnection()

    SNOWFLAKE_DIALECT.begin_transaction(connection)
    SNOWFLAKE_DIALECT.commit(connection)
    SNOWFLAKE_DIALECT.rollback(connection)

    assert connection.calls == [
        ("begin", ()),
        ("commit", ()),
        ("rollback", ()),
    ]


@requires_step5_dialect
def test_snowflake_source_schema_context_switches_and_restores_database_schema() -> None:
    SNOWFLAKE_DIALECT = _snowflake_dialect()
    connection = RecordingConnection()

    with SNOWFLAKE_DIALECT.source_schema_context(connection, _source_space()):
        connection.execute("select * from customers")

    assert connection.calls == [
        ("select current_database(), current_schema()", ()),
        ('use schema "SOURCE_DB"."APP"', ()),
        ("select * from customers", ()),
        ('use schema "RETL_DB"."RETL_RUNTIME"', ()),
    ]


@requires_step5_dialect
def test_snowflake_capability_helpers_render_backend_specific_sql() -> None:
    SNOWFLAKE_DIALECT = _snowflake_dialect()

    assert SNOWFLAKE_DIALECT.json_object_sql({"id": '"id"', "name": '"name"'}) == (
        "object_construct_keep_null('id', \"id\", 'name', \"name\")"
    )
    assert SNOWFLAKE_DIALECT.json_extract_scalar_sql('"payload"', "$.id") == (
        "get_path(\"payload\", '$.id')::string"
    )
    assert SNOWFLAKE_DIALECT.sha256_sql('"payload"') == 'sha2(cast("payload" as string), 256)'
    assert SNOWFLAKE_DIALECT.limit_sql("select * from rows", ":1") == (
        "select * from rows limit :1"
    )
    assert SNOWFLAKE_DIALECT.json_array_sql(['"id"']) == 'array_construct("id")'
    assert SNOWFLAKE_DIALECT.json_parse_sql(":1") == "parse_json(:1)"
    assert SNOWFLAKE_DIALECT.json_serialize_sql('"payload"') == 'to_json("payload")'
    assert SNOWFLAKE_DIALECT.cast_to_text_sql("7") == "cast(7 as string)"
    assert SNOWFLAKE_DIALECT.concat_sql(["'a'", "'b'"]) == "concat('a', 'b')"


@requires_step5_dialect
def test_snowflake_upsert_sql_uses_merge_with_sqlglot_source_row() -> None:
    SNOWFLAKE_DIALECT = _snowflake_dialect()

    params = SqlParamAllocator(SNOWFLAKE_DIALECT.parameter_style)
    upsert = runtime_upsert(
        RelationPath("destination_progress", schema="RETL_RUNTIME", database="RETL_DB"),
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

    assert SNOWFLAKE_DIALECT.upsert_sql(upsert) == (
        'MERGE INTO "RETL_DB"."RETL_RUNTIME"."destination_progress" AS target '
        'USING (SELECT :1 AS "DECLARATION_NAME", :2 AS "POSITION_JSON", :3 AS "ACTIVE") '
        "AS source "
        'ON target."DECLARATION_NAME" = source."DECLARATION_NAME" '
        "WHEN MATCHED THEN UPDATE SET "
        '"POSITION_JSON" = source."POSITION_JSON", '
        '"ACTIVE" = TRUE, '
        '"LAST_SEEN_AT" = current_timestamp '
        'WHEN NOT MATCHED THEN INSERT ("DECLARATION_NAME", "POSITION_JSON", "ACTIVE") '
        'VALUES (source."DECLARATION_NAME", source."POSITION_JSON", source."ACTIVE")'
    )
    assert params.params == ("customers", '{"id": 7}', True)


@requires_step5_dialect
def test_snowflake_upsert_sql_can_omit_matched_clause_for_noop_conflicts() -> None:
    SNOWFLAKE_DIALECT = _snowflake_dialect()

    params = SqlParamAllocator(SNOWFLAKE_DIALECT.parameter_style)
    upsert = runtime_upsert(
        RelationPath("destination_progress", schema="RETL_RUNTIME", database="RETL_DB"),
        row_write_input([("declaration_name", "customers")], params=params),
        key_columns=["declaration_name"],
    )

    assert SNOWFLAKE_DIALECT.upsert_sql(upsert) == (
        'MERGE INTO "RETL_DB"."RETL_RUNTIME"."destination_progress" AS target '
        'USING (SELECT :1 AS "DECLARATION_NAME") AS source '
        'ON target."DECLARATION_NAME" = source."DECLARATION_NAME" '
        'WHEN NOT MATCHED THEN INSERT ("DECLARATION_NAME") VALUES (source."DECLARATION_NAME")'
    )
    assert params.params == ("customers",)
