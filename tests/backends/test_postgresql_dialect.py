from __future__ import annotations

import importlib
from collections.abc import Sequence
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
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.writes import compile_runtime_update_many


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchone_row: tuple[object, ...] | None = ("retl",)

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        self.calls.append((sql, tuple(parameters)))
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.fetchone_row


def _dialect() -> Any:
    return importlib.import_module("retl.backends.postgresql").POSTGRESQL_DIALECT


def _source_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="postgresql",
        database="app",
        schema="public",
        access="read_only",
    )


def _runtime_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="postgresql",
        database="app",
        schema="retl",
        access="read_write",
    )


def test_postgresql_dialect_exposes_sqlglot_render_name_and_parameter_style() -> None:
    postgresql_module = importlib.import_module("retl.backends.postgresql")
    dialect = postgresql_module.POSTGRESQL_DIALECT

    assert isinstance(dialect, postgresql_module.PostgreSqlDialect)
    assert isinstance(dialect, SqlDialectCapabilities)
    assert dialect.name == "postgresql"
    assert dialect.sqlglot_dialect == "postgres"
    assert dialect.parameter_style is SqlParameterStyle.FORMAT
    assert dialect.placeholder(2) == "%s"


def test_postgresql_relation_space_helpers_render_schema_qualified_relations() -> None:
    dialect = _dialect()

    assert dialect.source_relation(_source_space(), "customers") == RelationPath(
        "customers",
        schema="public",
    )
    assert dialect.runtime_relation(_runtime_space(), "ordered_work") == RelationPath(
        "ordered_work",
        schema="retl",
    )
    assert dialect.render_source_relation(_source_space(), "customers") == '"public"."customers"'
    assert (
        dialect.render_runtime_relation(_runtime_space(), "ordered_work") == '"retl"."ordered_work"'
    )


def test_postgresql_relation_space_helpers_validate_backend_access_and_identifiers() -> None:
    dialect = _dialect()

    with pytest.raises(ValueError, match="backend must be postgresql"):
        dialect.source_relation(
            SqlRelationSpace(
                backend_name="duckdb",
                database="app",
                schema="public",
                access="read_only",
            ),
            "customers",
        )

    with pytest.raises(ValueError, match="access must be read_write"):
        dialect.runtime_relation(
            SqlRelationSpace(
                backend_name="postgresql",
                database="app",
                schema="public",
                access="read_only",
            ),
            "ordered_work",
        )

    with pytest.raises(ValueError, match="simple SQL identifier"):
        dialect.source_relation(
            SqlRelationSpace(
                backend_name="postgresql",
                database="app",
                schema="public-data",
                access="read_only",
            ),
            "customers",
        )


def test_postgresql_source_schema_context_switches_and_restores_schema() -> None:
    dialect = _dialect()
    connection = RecordingConnection()

    with dialect.source_schema_context(connection, _source_space()):
        connection.execute("select * from customers")

    assert connection.calls == [
        ("select current_schema()", ()),
        ('set search_path to "public", public', ()),
        ("select * from customers", ()),
        ('set search_path to "retl", public', ()),
    ]


def test_postgresql_capability_helpers_render_backend_specific_sql() -> None:
    dialect = _dialect()

    assert dialect.json_object_sql({"id": '"id"', "name": '"name"'}) == (
        "jsonb_build_object('id', \"id\", 'name', \"name\")"
    )
    assert dialect.json_extract_scalar_sql('"payload"', "$.id") == (
        "jsonb_path_query_first(\"payload\", '$.id') #>> '{}'"
    )
    assert dialect.sha256_sql('"payload"') == (
        "encode(digest(cast(\"payload\" as text), 'sha256'), 'hex')"
    )
    assert dialect.limit_sql("select * from rows", "%s") == "select * from rows limit %s"
    assert dialect.json_array_sql(['"id"']) == 'jsonb_build_array("id")'
    assert dialect.json_parse_sql("%s") == "(%s)::jsonb"
    assert dialect.json_serialize_sql('"payload"') == '("payload")::text'
    assert dialect.cast_to_text_sql("7") == "cast(7 as text)"
    assert dialect.concat_sql(["'a'", "'b'"]) == "'a' || 'b'"
    assert dialect.create_temp_table_as_sql("retl_state_collect_snapshot", "select 1") == (
        'create temporary table "retl_state_collect_snapshot" as select 1'
    )
    assert dialect.drop_temp_table_sql("retl_state_collect_snapshot") == (
        'drop table if exists "retl_state_collect_snapshot"'
    )


def test_postgresql_upsert_sql_uses_on_conflict_with_sqlglot_insert() -> None:
    dialect = _dialect()

    params = SqlParamAllocator(dialect.parameter_style)
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

    assert dialect.upsert_sql(upsert) == (
        'INSERT INTO "retl"."destination_progress" '
        '("declaration_name", "position_json", "active") '
        'VALUES (%s, %s, %s) ON CONFLICT ("declaration_name") DO UPDATE SET '
        '"position_json" = excluded."position_json", '
        '"active" = TRUE, '
        '"last_seen_at" = current_timestamp'
    )
    assert params.params == ("customers", '{"id": 7}', True)


def test_postgresql_batch_update_casts_runtime_table_source_params() -> None:
    dialect = _dialect()
    context = SqlRuntimeContext(
        connection=RecordingConnection(),
        dialect=dialect,
        runtime_space=_runtime_space(),
    )

    compiled = compile_runtime_update_many(
        context,
        "destination_batches",
        (
            (
                ("batch_id", "batch_a"),
                ("status", "succeeded"),
                ("attempt_count", 1),
                ("state_lower_sequence_order", None),
                ("retry_eligible", False),
            ),
        ),
        key_columns=("batch_id",),
        update_columns=(
            "status",
            "attempt_count",
            "state_lower_sequence_order",
            "retry_eligible",
        ),
        update_assignments=(upsert_assignment("updated_at", exp.CurrentTimestamp()),),
    )

    assert compiled.sql.startswith('UPDATE "retl"."destination_batches" AS target SET')
    assert 'CAST(%s AS TEXT) AS "batch_id"' in compiled.sql
    assert 'CAST(%s AS BIGINT) AS "attempt_count"' in compiled.sql
    assert 'CAST(%s AS BIGINT) AS "state_lower_sequence_order"' in compiled.sql
    assert 'CAST(%s AS BOOLEAN) AS "retry_eligible"' in compiled.sql
    assert '"updated_at" = CURRENT_TIMESTAMP' in compiled.sql
    assert compiled.params == ("batch_a", "succeeded", 1, None, False)
