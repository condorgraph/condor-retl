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
from retl.stores.sql_runtime.writes import compile_runtime_insert, compile_runtime_update


def _bigquery_dialect() -> Any:
    bigquery_module = importlib.import_module("retl.backends.bigquery")
    return bigquery_module.BIGQUERY_DIALECT


def _source_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="bigquery",
        database="example-source-project",
        schema="mart",
        access="read_only",
    )


def _runtime_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="bigquery",
        database="example-runtime-project",
        schema="retl_runtime",
        access="read_write",
    )


def test_bigquery_dialect_exposes_sqlglot_render_name_and_parameter_style() -> None:
    bigquery_module = importlib.import_module("retl.backends.bigquery")
    dialect = bigquery_module.BIGQUERY_DIALECT

    assert isinstance(dialect, bigquery_module.BigQuerySqlDialect)
    assert isinstance(dialect, SqlDialectCapabilities)
    assert dialect.name == "bigquery"
    assert dialect.sqlglot_dialect == "bigquery"
    assert dialect.parameter_style is SqlParameterStyle.QMARK
    assert dialect.placeholder(2) == "?"


def test_bigquery_relation_space_helpers_render_project_and_dataset_relations() -> None:
    dialect = _bigquery_dialect()

    assert dialect.source_relation(_source_space(), "customers") == RelationPath(
        "customers",
        schema="mart",
        database="example-source-project",
    )
    assert dialect.runtime_relation(_runtime_space(), "ordered_work") == RelationPath(
        "ordered_work",
        schema="retl_runtime",
        database="example-runtime-project",
    )
    assert dialect.render_source_relation(_source_space(), "customers") == (
        "`example-source-project`.`mart`.`customers`"
    )
    assert dialect.render_runtime_relation(_runtime_space(), "ordered_work") == (
        "`example-runtime-project`.`retl_runtime`.`ordered_work`"
    )


def test_bigquery_relation_space_helpers_validate_backend_access_and_identifiers() -> None:
    dialect = _bigquery_dialect()

    with pytest.raises(ValueError, match="backend must be bigquery"):
        dialect.source_relation(
            SqlRelationSpace(
                backend_name="duckdb",
                database="example-source-project",
                schema="mart",
                access="read_only",
            ),
            "customers",
        )

    with pytest.raises(ValueError, match="access must be read_write"):
        dialect.runtime_relation(
            SqlRelationSpace(
                backend_name="bigquery",
                database="example-runtime-project",
                schema="retl_runtime",
                access="read_only",
            ),
            "ordered_work",
        )


def test_bigquery_temp_table_and_transaction_helpers_render_google_sql() -> None:
    dialect = _bigquery_dialect()
    connection = RecordingConnection()

    assert dialect.render_temp_relation("retl_state_collect_snapshot") == (
        "`retl_state_collect_snapshot`"
    )
    assert dialect.create_temp_table_as_sql("retl_state_collect_snapshot", "select 1") == (
        "create temp table `retl_state_collect_snapshot` as select 1"
    )
    assert dialect.drop_temp_table_sql("retl_state_collect_snapshot") == (
        "drop table if exists _SESSION.`retl_state_collect_snapshot`"
    )

    dialect.begin_transaction(connection)
    dialect.commit(connection)
    dialect.rollback(connection)
    assert connection.calls == [
        ("begin transaction", ()),
        ("commit transaction", ()),
        ("rollback transaction", ()),
    ]


def test_bigquery_capability_helpers_render_backend_specific_sql() -> None:
    dialect = _bigquery_dialect()

    assert dialect.json_object_sql({"id": "`id`", "name": "`name`"}) == (
        "JSON_OBJECT('id', `id`, 'name', `name`)"
    )
    assert dialect.json_extract_scalar_sql("`payload`", "$.id") == ("JSON_VALUE(`payload`, '$.id')")
    assert dialect.sha256_sql("`payload`") == "TO_HEX(SHA256(CAST(`payload` AS STRING)))"
    assert dialect.limit_sql("select * from rows", "?") == "select * from rows LIMIT ?"
    assert dialect.json_array_sql(["`id`"]) == "JSON_ARRAY(`id`)"
    assert dialect.json_concat_arrays_sql(["JSON_ARRAY(`email`)", "TO_JSON(`phones`)"]) == (
        "TO_JSON(ARRAY_CONCAT(JSON_QUERY_ARRAY(JSON_ARRAY(`email`)), "
        "JSON_QUERY_ARRAY(TO_JSON(`phones`))))"
    )
    assert dialect.json_parse_sql("?") == "PARSE_JSON(?)"
    assert dialect.json_serialize_sql("`payload`") == "TO_JSON_STRING(`payload`)"
    assert dialect.cast_to_text_sql("7") == "CAST(7 AS STRING)"
    assert dialect.concat_sql(["'a'", "'b'"]) == "CONCAT('a', 'b')"


def test_bigquery_upsert_sql_uses_merge_with_sqlglot_source_row() -> None:
    dialect = _bigquery_dialect()
    params = SqlParamAllocator(dialect.parameter_style)
    upsert = runtime_upsert(
        RelationPath(
            "destination_progress",
            schema="retl_runtime",
            database="example-runtime-project",
        ),
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
        "MERGE `example-runtime-project`.`retl_runtime`.`destination_progress` AS target "
        "USING (SELECT CAST(? AS STRING) AS `declaration_name`, "
        "CAST(? AS STRING) AS `position_json`, ? AS `active`) AS source "
        "ON target.`declaration_name` = source.`declaration_name` "
        "WHEN MATCHED THEN UPDATE SET "
        "`position_json` = source.`position_json`, "
        "`active` = TRUE, "
        "`last_seen_at` = CURRENT_TIMESTAMP() "
        "WHEN NOT MATCHED THEN INSERT (`declaration_name`, `position_json`, `active`) "
        "VALUES (source.`declaration_name`, source.`position_json`, source.`active`)"
    )
    assert params.params == ("customers", '{"id": 7}', True)


def test_bigquery_runtime_insert_update_helpers_cast_typed_runtime_params() -> None:
    dialect = _bigquery_dialect()
    context = SqlRuntimeContext(
        connection=RecordingConnection(),
        dialect=dialect,
        runtime_space=_runtime_space(),
    )

    insert = compile_runtime_insert(
        context,
        "sync_reports",
        (
            ("report_id", "report-1"),
            ("report_ref", "sync-report:1"),
            ("run_id", "run-1"),
            ("attempt_id", "attempt-1"),
            ("runner_name", "runner"),
            ("sync_name", "sync"),
            ("declaration_name", "customers"),
            ("declaration_version_id", None),
            ("declaration_kind", "state"),
            ("destination_name", "dest"),
            ("surface", "profile"),
            ("status", "succeeded"),
            ("dry_run", False),
            ("submitted_record_count", 1),
            ("succeeded_record_count", 1),
            ("accepted_record_count", 0),
            ("failed_record_count", 0),
            ("retryable_failure_count", 0),
            ("terminal_failure_count", 0),
            ("pre_acceptance_failure_count", 0),
            ("progress_advanced", True),
            ("failure_category", None),
            ("http_status", None),
            ("last_error_summary", None),
            ("last_error_detail", None),
            ("report_json", "{}"),
        ),
    )
    update = compile_runtime_update(
        context,
        "destination_batches",
        (
            ("attempt_count", 1),
            ("retry_eligible", False),
            ("http_status", None),
        ),
        where_values=(("batch_id", "batch-1"),),
    )

    assert "MERGE" not in insert.sql
    assert "CAST(? AS STRING)" in insert.sql
    assert "CAST(? AS INT64)" in insert.sql
    assert "CAST(? AS BOOL)" in insert.sql
    assert "MERGE" not in update.sql
    assert "`attempt_count` = CAST(? AS INT64)" in update.sql
    assert "`retry_eligible` = CAST(? AS BOOL)" in update.sql
    assert "`batch_id` = CAST(? AS STRING)" in update.sql


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        self.calls.append((sql, tuple(parameters)))
        return self
