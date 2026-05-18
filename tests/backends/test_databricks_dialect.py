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

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        self.calls.append((sql, tuple(parameters)))
        return self


def _dialect() -> Any:
    return importlib.import_module("retl.backends.databricks").DATABRICKS_DIALECT


def _source_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="databricks",
        database="source_catalog",
        schema="source_schema",
        access="read_only",
    )


def _runtime_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="databricks",
        database="runtime_catalog",
        schema="runtime_schema",
        access="read_write",
    )


def test_databricks_dialect_exposes_sqlglot_render_name_and_parameter_style() -> None:
    databricks_module = importlib.import_module("retl.backends.databricks")
    dialect = databricks_module.DATABRICKS_DIALECT

    assert isinstance(dialect, databricks_module.DatabricksSqlDialect)
    assert isinstance(dialect, SqlDialectCapabilities)
    assert dialect.name == "databricks"
    assert dialect.sqlglot_dialect == "databricks"
    assert dialect.parameter_style is SqlParameterStyle.QMARK
    assert dialect.placeholder(2) == "?"


def test_databricks_relation_helpers_render_catalog_schema_table_paths() -> None:
    dialect = _dialect()

    assert dialect.source_relation(_source_space(), "customers") == RelationPath(
        "customers",
        schema="source_schema",
        database="source_catalog",
    )
    assert dialect.runtime_relation(_runtime_space(), "ordered_work") == RelationPath(
        "ordered_work",
        schema="runtime_schema",
        database="runtime_catalog",
    )
    assert dialect.render_source_relation(_source_space(), "customers") == (
        "`source_catalog`.`source_schema`.`customers`"
    )
    assert dialect.render_runtime_relation(_runtime_space(), "ordered_work") == (
        "`runtime_catalog`.`runtime_schema`.`ordered_work`"
    )


def test_databricks_relation_helpers_validate_backend_access_and_hive_metastore() -> None:
    dialect = _dialect()

    with pytest.raises(ValueError, match="backend must be databricks"):
        dialect.source_relation(
            SqlRelationSpace(
                backend_name="duckdb",
                database="source_catalog",
                schema="source_schema",
                access="read_only",
            ),
            "customers",
        )

    with pytest.raises(ValueError, match="access must be read_write"):
        dialect.runtime_relation(
            SqlRelationSpace(
                backend_name="databricks",
                database="runtime_catalog",
                schema="runtime_schema",
                access="read_only",
            ),
            "ordered_work",
        )

    with pytest.raises(ValueError, match="hive_metastore"):
        dialect.source_relation(
            SqlRelationSpace(
                backend_name="databricks",
                database="hive_metastore",
                schema="source_schema",
                access="read_only",
            ),
            "customers",
        )


def test_databricks_source_schema_context_does_not_switch_catalog_or_schema() -> None:
    dialect = _dialect()
    connection = RecordingConnection()

    with dialect.source_schema_context(connection, _source_space()):
        connection.execute("select 1")

    assert connection.calls == [("select 1", ())]


def test_databricks_temp_table_helpers_allow_only_collect_scratch_tables() -> None:
    dialect = _dialect()

    assert dialect.render_temp_relation("retl_state_collect_snapshot") == (
        "`retl_state_collect_snapshot`"
    )
    assert dialect.create_temp_table_as_sql("retl_event_collect_window", "select 1") == (
        "create temporary table `retl_event_collect_window` as select 1"
    )
    assert dialect.drop_temp_table_sql("retl_state_collect_snapshot") == (
        "drop temporary table if exists `retl_state_collect_snapshot`"
    )
    with pytest.raises(ValueError, match="collect scratch"):
        dialect.create_temp_table_as_sql("some_other_temp", "select 1")


def test_databricks_transaction_methods_delegate_to_recording_connection() -> None:
    dialect = _dialect()
    connection = RecordingConnection()

    dialect.begin_transaction(connection)
    dialect.commit(connection)
    dialect.rollback(connection)

    assert connection.calls == [
        ("begin transaction", ()),
        ("commit", ()),
        ("rollback", ()),
    ]


def test_databricks_capability_helpers_render_backend_specific_sql() -> None:
    dialect = _dialect()

    assert dialect.json_object_sql({"id": "`id`", "name": "`name`"}) == (
        "named_struct('id', `id`, 'name', `name`)"
    )
    assert dialect.json_extract_scalar_sql("`payload`", "$.id") == (
        "get_json_object(`payload`, '$.id')"
    )
    assert dialect.sha256_sql("`payload`") == "sha2(cast(`payload` as string), 256)"
    assert dialect.limit_sql("select * from rows", "?") == "select * from rows LIMIT ?"
    assert dialect.json_array_sql(["`id`"]) == "array(`id`)"
    assert dialect.json_parse_sql("?") == "parse_json(?)"
    assert dialect.json_serialize_sql("`payload`") == "to_json(`payload`)"
    assert dialect.cast_to_text_sql("7") == "cast(7 as string)"
    assert dialect.concat_sql(["'a'", "'b'"]) == "concat('a', 'b')"


def test_databricks_upsert_sql_uses_merge_with_qmark_params() -> None:
    dialect = _dialect()
    params = SqlParamAllocator(dialect.parameter_style)
    upsert = runtime_upsert(
        RelationPath("destination_progress", schema="runtime_schema", database="runtime_catalog"),
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
        "MERGE INTO `runtime_catalog`.`runtime_schema`.`destination_progress` AS target "
        "USING (SELECT ? AS `declaration_name`, ? AS `position_json`, ? AS `active`) "
        "AS source "
        "ON target.`declaration_name` = source.`declaration_name` "
        "WHEN MATCHED THEN UPDATE SET "
        "`position_json` = source.`position_json`, "
        "`active` = TRUE, "
        "`last_seen_at` = current_timestamp "
        "WHEN NOT MATCHED THEN INSERT (`declaration_name`, `position_json`, `active`) "
        "VALUES (source.`declaration_name`, source.`position_json`, source.`active`)"
    )
    assert params.params == ("customers", '{"id": 7}', True)


def test_databricks_batch_update_sql_uses_merge_when_matched_only() -> None:
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
            (("batch_id", "batch_a"), ("status", "failed"), ("attempt_count", 1)),
            (("batch_id", "batch_b"), ("status", "succeeded"), ("attempt_count", 2)),
        ),
        key_columns=("batch_id",),
        update_columns=("status", "attempt_count"),
        update_assignments=(upsert_assignment("updated_at", exp.CurrentTimestamp()),),
    )

    assert compiled.sql == (
        "MERGE INTO `runtime_catalog`.`runtime_schema`.`destination_batches` AS target "
        "USING (SELECT ? AS `batch_id`, ? AS `status`, ? AS `attempt_count` "
        "UNION ALL SELECT ? AS `batch_id`, ? AS `status`, ? AS `attempt_count`) AS source "
        "ON target.`batch_id` = source.`batch_id` "
        "WHEN MATCHED THEN UPDATE SET "
        "`status` = source.`status`, "
        "`attempt_count` = source.`attempt_count`, "
        "`updated_at` = CURRENT_TIMESTAMP()"
    )
    assert compiled.params == ("batch_a", "failed", 1, "batch_b", "succeeded", 2)
