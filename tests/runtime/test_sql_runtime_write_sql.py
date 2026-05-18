from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

from retl.backends.bigquery import BIGQUERY_DIALECT
from retl.backends.duckdb import DUCKDB_DIALECT
from retl.backends.snowflake import SNOWFLAKE_DIALECT
from retl.sql import SqlDialectCapabilities
from retl.stores.contracts import (
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    SqlRelationSpace,
    destination_batch_id,
)
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.destination_batches import (
    compile_destination_batch_insert,
    compile_destination_batch_update,
    compile_destination_batch_upsert,
    compile_destination_batches_insert,
    compile_destination_batches_update,
)
from retl.stores.sql_runtime.writes import (
    compile_runtime_insert,
    compile_runtime_update,
    compile_runtime_update_many,
    compile_runtime_upsert,
)


class _Connection:
    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        raise AssertionError("compile-only tests must not execute SQL")


def _context(
    dialect: SqlDialectCapabilities = DUCKDB_DIALECT,
    *,
    database: str = "runtime.duckdb",
    schema: str = "runtime",
) -> SqlRuntimeContext:
    return SqlRuntimeContext(
        connection=_Connection(),
        dialect=dialect,
        runtime_space=SqlRelationSpace(
            backend_name=dialect.name,
            database=database,
            schema=schema,
            access="read_write",
        ),
    )


def _scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="sync_a",
        destination_name="dest_a",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )


def _batch() -> DestinationBatchRecord:
    identity = DestinationBatchIdentity(
        scope=_scope(),
        declaration_version_id="decl_v1",
        source_page_index=0,
        first_collect_id="00000000-0007-7000-8000-000000000000",
        last_collect_id="00000000-0009-7000-8000-000000000000",
        first_sequence_order=4,
        last_sequence_order=12,
        destination_batch_index=2,
        payload_fingerprint="payload_fp",
        target_request_fingerprint="target_fp",
    )
    return DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        run_id="run_a",
        attempt_id="attempt_a",
        record_count=9,
        status="failed",
        completion_state="unresolved",
        attempt_count=1,
        last_error_summary="failed",
        last_error_detail="detail",
        last_failure_category="retryable",
        http_status=503,
        retry_eligible=True,
        first_submitted_at=datetime(2026, 5, 9, 12, 0, 0),
        last_attempted_at=datetime(2026, 5, 9, 12, 1, 0),
    )


def test_runtime_upsert_write_sql_uses_runtime_relation_and_params() -> None:
    compiled = compile_runtime_upsert(
        _context(),
        "runs",
        (
            ("run_id", "run_a"),
            ("runner_name", "runner"),
            ("status", "running"),
            ("dry_run", False),
            ("script_path", "sync.py"),
            ("script_content_hash", "hash_a"),
            ("started_at", datetime(2026, 5, 9, 12, 0, 0)),
        ),
        key_columns=("run_id",),
        update_columns=(
            "runner_name",
            "status",
            "dry_run",
            "script_path",
            "script_content_hash",
            "started_at",
        ),
    )

    assert '"runtime"."runs"' in compiled.sql
    assert "run_a" not in compiled.sql
    assert "ON CONFLICT" in compiled.sql
    assert '"runner_name" = excluded."runner_name"' in compiled.sql
    assert compiled.params[:6] == ("run_a", "runner", "running", False, "sync.py", "hash_a")
    assert compiled.sql.count("?") == len(compiled.params)


def test_runtime_insert_write_sql_uses_runtime_relation_without_upsert_clause() -> None:
    compiled = compile_runtime_insert(
        _context(),
        "runs",
        (
            ("run_id", "run_a"),
            ("runner_name", "runner"),
            ("status", "running"),
            ("dry_run", False),
            ("script_path", "sync.py"),
            ("script_content_hash", "hash_a"),
            ("started_at", datetime(2026, 5, 9, 12, 0, 0)),
        ),
    )

    assert compiled.sql == (
        'INSERT INTO "runtime"."runs" '
        '("run_id", "runner_name", "status", "dry_run", "script_path", '
        '"script_content_hash", "started_at") '
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    assert "ON CONFLICT" not in compiled.sql
    assert "MERGE" not in compiled.sql
    assert compiled.params[:6] == ("run_a", "runner", "running", False, "sync.py", "hash_a")


def test_runtime_update_write_sql_uses_where_bound_params() -> None:
    compiled = compile_runtime_update(
        _context(),
        "destination_progress",
        (("position_json", '{"collect_id": "00000000-0009-7000-8000-000000000000"}'),),
        where_values=(
            ("sync_name", "sync_a"),
            ("destination_name", "dest_a"),
            ("surface", "profile"),
            ("family", "state"),
            ("declaration_name", "customer_state"),
        ),
    )

    assert compiled.sql.startswith('UPDATE "runtime"."destination_progress" SET')
    assert "MERGE" not in compiled.sql
    assert '"position_json" = ?' in compiled.sql
    assert '"sync_name" = ?' in compiled.sql
    assert isinstance(compiled.params[0], str)
    assert compiled.params[0].startswith('{"collect_id"')
    assert compiled.params[-1] == "customer_state"


def test_runtime_batch_update_write_sql_uses_one_update_from_source_rowset() -> None:
    compiled = compile_runtime_update_many(
        _context(),
        "destination_batches",
        (
            (("batch_id", "batch_a"), ("status", "failed"), ("attempt_count", 1)),
            (("batch_id", "batch_b"), ("status", "succeeded"), ("attempt_count", 2)),
        ),
        key_columns=("batch_id",),
        update_columns=("status", "attempt_count"),
    )

    assert compiled.sql.startswith('UPDATE "runtime"."destination_batches" AS target SET')
    assert " FROM (" in compiled.sql
    assert "UNION ALL" in compiled.sql
    assert "MERGE" not in compiled.sql
    assert compiled.sql.count("UPDATE") == 1
    assert compiled.params == ("batch_a", "failed", 1, "batch_b", "succeeded", 2)


def test_destination_batch_upsert_sql_keeps_coalesce_and_timestamp_assignments() -> None:
    compiled = compile_destination_batch_upsert(_context(), _batch())

    assert '"runtime"."destination_batches"' in compiled.sql
    assert "customer_state" not in compiled.sql
    assert "COALESCE" in compiled.sql
    assert '"runtime"."destination_batches"."first_submitted_at"' in compiled.sql
    assert 'excluded."first_submitted_at"' in compiled.sql
    assert '"updated_at" = NOW()' in compiled.sql
    assert compiled.params[7] == "customer_state"
    assert compiled.sql.count("?") == len(compiled.params)


def test_destination_batch_insert_sql_is_plain_insert() -> None:
    compiled = compile_destination_batch_insert(_context(), _batch())

    assert compiled.sql.startswith('INSERT INTO "runtime"."destination_batches"')
    assert "ON CONFLICT" not in compiled.sql
    assert "MERGE" not in compiled.sql
    assert compiled.params[7] == "customer_state"
    assert compiled.sql.count("?") == len(compiled.params)


def test_destination_batch_batch_insert_sql_uses_one_values_statement() -> None:
    first = _batch()
    second_identity = replace(
        first.identity,
        destination_batch_index=3,
        payload_fingerprint="payload_fp_2",
        target_request_fingerprint="target_fp_2",
    )
    second = DestinationBatchRecord(
        batch_id=destination_batch_id(second_identity),
        identity=second_identity,
        record_count=9,
    )

    compiled = compile_destination_batches_insert(_context(), (first, second))

    assert compiled.sql.startswith('INSERT INTO "runtime"."destination_batches"')
    assert compiled.sql.count("VALUES") == 1
    assert "), (" in compiled.sql
    assert compiled.params.count("customer_state") == 2


def test_destination_batch_update_sql_targets_existing_batch_id() -> None:
    compiled = compile_destination_batch_update(_context(), _batch())

    assert compiled.sql.startswith('UPDATE "runtime"."destination_batches" SET')
    assert "ON CONFLICT" not in compiled.sql
    assert "MERGE" not in compiled.sql
    assert '"updated_at" = CURRENT_TIMESTAMP' in compiled.sql
    assert compiled.params[-1] == _batch().batch_id


def test_destination_batch_batch_update_sql_uses_one_statement_for_multiple_rows() -> None:
    first = _batch()
    second_identity = replace(
        first.identity,
        destination_batch_index=3,
        payload_fingerprint="payload_fp_2",
        target_request_fingerprint="target_fp_2",
    )
    second = DestinationBatchRecord(
        batch_id=destination_batch_id(second_identity),
        identity=second_identity,
        run_id="run_b",
        attempt_id="attempt_b",
        record_count=9,
        status="succeeded",
        completion_state="resolved",
        attempt_count=2,
    )

    compiled = compile_destination_batches_update(_context(), (first, second))

    assert compiled.sql.startswith('UPDATE "runtime"."destination_batches" AS target SET')
    assert " FROM (" in compiled.sql
    assert "UNION ALL" in compiled.sql
    assert "MERGE" not in compiled.sql
    assert compiled.sql.count("UPDATE") == 1
    assert '"status" = source."status"' in compiled.sql
    assert 'target."batch_id" = source."batch_id"' in compiled.sql
    assert first.batch_id in compiled.params
    assert second.batch_id in compiled.params
    assert "failed" in compiled.params
    assert "succeeded" in compiled.params


def test_bigquery_runtime_insert_and_update_cast_runtime_table_params() -> None:
    context = _context(
        BIGQUERY_DIALECT,
        database="example-runtime-project",
        schema="retl_runtime",
    )

    insert = compile_runtime_insert(
        context,
        "runs",
        (
            ("run_id", "run_a"),
            ("runner_name", "runner"),
            ("status", "running"),
            ("dry_run", False),
            ("script_path", None),
            ("script_content_hash", None),
            ("started_at", datetime(2026, 5, 9, 12, 0, 0)),
        ),
    )
    update = compile_runtime_update(
        context,
        "destination_progress",
        (("position_json", None),),
        where_values=(
            ("sync_name", "sync_a"),
            ("destination_name", "dest_a"),
            ("surface", "profile"),
            ("family", "state"),
            ("declaration_name", "customer_state"),
        ),
    )

    assert insert.sql.startswith("INSERT INTO `example-runtime-project`.`retl_runtime`.`runs`")
    assert "CAST(? AS STRING)" in insert.sql
    assert "CAST(? AS TIMESTAMP)" in insert.sql
    assert "MERGE" not in insert.sql
    assert update.sql.startswith(
        "UPDATE `example-runtime-project`.`retl_runtime`.`destination_progress` SET"
    )
    assert "`position_json` = CAST(? AS STRING)" in update.sql
    assert "`sync_name` = CAST(? AS STRING)" in update.sql
    assert "MERGE" not in update.sql


def test_bigquery_destination_batch_batch_update_casts_source_params() -> None:
    context = _context(
        BIGQUERY_DIALECT,
        database="example-runtime-project",
        schema="retl_runtime",
    )
    first = _batch()
    second_identity = replace(
        first.identity,
        destination_batch_index=3,
        payload_fingerprint="payload_fp_2",
        target_request_fingerprint="target_fp_2",
    )
    second = DestinationBatchRecord(
        batch_id=destination_batch_id(second_identity),
        identity=second_identity,
        run_id="run_b",
        attempt_id="attempt_b",
        record_count=9,
        status="succeeded",
        completion_state="resolved",
        attempt_count=2,
        retry_eligible=False,
    )

    compiled = compile_destination_batches_update(context, (first, second))

    assert compiled.sql.startswith(
        "UPDATE `example-runtime-project`.`retl_runtime`.`destination_batches` AS target SET"
    )
    assert " FROM (" in compiled.sql
    assert "UNION ALL" in compiled.sql
    assert "MERGE" not in compiled.sql
    assert compiled.sql.count("UPDATE") == 1
    assert "`status` = source.`status`" in compiled.sql
    assert "target.`batch_id` = source.`batch_id`" in compiled.sql
    assert "CAST(? AS STRING) AS `batch_id`" in compiled.sql
    assert "CAST(? AS INT64) AS `attempt_count`" in compiled.sql
    assert "CAST(? AS BOOL) AS `retry_eligible`" in compiled.sql
    assert "CURRENT_TIMESTAMP()" in compiled.sql
    assert first.batch_id in compiled.params
    assert second.batch_id in compiled.params


def test_snowflake_runtime_upsert_write_sql_uses_numeric_params_in_row_order() -> None:
    compiled = compile_runtime_upsert(
        _context(SNOWFLAKE_DIALECT, database="RETL_DB", schema="RETL_RUNTIME"),
        "runs",
        (
            ("run_id", "run_a"),
            ("runner_name", "runner"),
            ("status", "running"),
            ("dry_run", False),
            ("script_path", "sync.py"),
            ("script_content_hash", "hash_a"),
            ("started_at", datetime(2026, 5, 9, 12, 0, 0)),
        ),
        key_columns=("run_id",),
        update_columns=(
            "runner_name",
            "status",
            "dry_run",
            "script_path",
            "script_content_hash",
            "started_at",
        ),
    )

    assert compiled.sql.startswith('MERGE INTO "RETL_DB"."RETL_RUNTIME"."runs" AS target')
    assert "run_a" not in compiled.sql
    assert 'USING (SELECT :1 AS "RUN_ID", :2 AS "RUNNER_NAME"' in compiled.sql
    assert 'target."RUN_ID" = source."RUN_ID"' in compiled.sql
    assert compiled.params[:6] == ("run_a", "runner", "running", False, "sync.py", "hash_a")


def test_snowflake_destination_batch_upsert_sql_uses_source_target_assignment_aliases() -> None:
    compiled = compile_destination_batch_upsert(
        _context(SNOWFLAKE_DIALECT, database="RETL_DB", schema="RETL_RUNTIME"),
        _batch(),
    )

    assert compiled.sql.startswith(
        'MERGE INTO "RETL_DB"."RETL_RUNTIME"."destination_batches" AS target'
    )
    assert "customer_state" not in compiled.sql
    assert (
        '"FIRST_SUBMITTED_AT" = COALESCE(target."FIRST_SUBMITTED_AT", source."FIRST_SUBMITTED_AT")'
    ) in compiled.sql
    assert '"UPDATED_AT" = current_timestamp' in compiled.sql
    assert ":1" in compiled.sql
    assert f":{len(compiled.params)}" in compiled.sql
    assert compiled.params[7] == "customer_state"


def test_snowflake_destination_batch_batch_update_uses_merge() -> None:
    compiled = compile_destination_batches_update(
        _context(SNOWFLAKE_DIALECT, database="RETL_DB", schema="RETL_RUNTIME"),
        (_batch(),),
    )

    assert compiled.sql.startswith(
        'MERGE INTO "RETL_DB"."RETL_RUNTIME"."destination_batches" AS target'
    )
    assert 'USING (SELECT :1 AS "DECLARATION_VERSION_ID"' in compiled.sql
    assert 'ON target."BATCH_ID" = source."BATCH_ID"' in compiled.sql
    assert 'WHEN MATCHED THEN UPDATE SET "DECLARATION_VERSION_ID"' in compiled.sql
    assert '"UPDATED_AT" = CURRENT_TIMESTAMP()' in compiled.sql
    assert ":1" in compiled.sql
    assert f":{len(compiled.params)}" in compiled.sql
