from __future__ import annotations

import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from retl.backends.bigquery import BIGQUERY_DIALECT, BigQueryBackendAuth, BigQuerySqlBackend
from retl.stores.sql_runtime import writes
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.store import SqlRuntimeStore


def _google_bigquery_module_names() -> set[str]:
    return {
        name
        for name in sys.modules
        if name == "google.cloud.bigquery"
        or name.startswith("google.cloud.bigquery.")
        or name == "google.cloud.bigquery_storage_v1"
        or name.startswith("google.cloud.bigquery_storage_v1.")
    }


def _backend() -> BigQuerySqlBackend:
    return BigQuerySqlBackend(
        project="example-analytics-project",
        location="US",
        source_project="example-source-project",
        source_dataset="mart",
        runtime_project="example-runtime-project",
        runtime_dataset="retl_runtime",
        auth=BigQueryBackendAuth.application_default(),
    )


def test_bigquery_runtime_store_constructs_shared_context_from_injected_connection() -> None:
    before = _google_bigquery_module_names()
    backend = _backend()
    connection = RecordingSqlConnection()

    store = backend.runtime_store(client=connection)
    try:
        assert isinstance(store, SqlRuntimeStore)
        assert type(store).begin_attempt is SqlRuntimeStore.begin_attempt
        assert type(store).produce_state_collect is SqlRuntimeStore.produce_state_collect

        context = store._runtime_context  # noqa: SLF001
        assert isinstance(context, SqlRuntimeContext)
        assert context.connection is connection
        assert context.dialect is BIGQUERY_DIALECT
        assert context.sqlglot_dialect == "bigquery"
        assert context.runtime_space == backend.runtime_space
        assert context.collect_placement == backend.placement
        assert store._next_attempt_number == 1  # noqa: SLF001
    finally:
        store.close()

    assert connection.close_count == 1
    assert _google_bigquery_module_names() == before
    assert (
        connection.calls[0][0]
        == "create schema if not exists `example-runtime-project`.`retl_runtime`"
    )
    assert (
        "create table if not exists `example-runtime-project`.`retl_runtime`.`runs`"
        in (connection.calls[1][0])
    )
    assert "string" in connection.calls[1][0]
    assert "primary key" not in connection.calls[1][0].lower()


def test_bigquery_runtime_schema_clusters_large_runtime_tables() -> None:
    backend = _backend()
    connection = RecordingSqlConnection()

    store = backend.runtime_store(client=connection)
    try:
        table_sql = {
            _table_name_from_create_statement(sql): sql
            for sql, _params in connection.calls
            if sql.startswith("create table if not exists")
        }
    finally:
        store.close()

    assert table_sql["ordered_work"].endswith(
        "cluster by `declaration_name`, `family`, `collect_id`, `sequence_order`"
    )
    assert table_sql["state_current"].endswith(
        "cluster by `declaration_name`, `source_name`, `identity_json`, `collect_id`"
    )
    assert table_sql["destination_batches"].endswith(
        "cluster by `sync_name`, `destination_name`, `surface`, `declaration_name`"
    )
    assert "cluster by" not in table_sql["runs"]
    assert "cluster by" not in table_sql["destination_progress"]


def test_bigquery_runtime_store_uses_storage_write_api_for_report_append_tables() -> None:
    backend = _backend()
    connection = RecordingSqlConnection()
    storage_module = RecordingBigQueryStorageWriteModule()
    store = backend.runtime_store(
        client=connection,
        bigquery_storage_module=storage_module,
    )
    try:
        context = store._runtime_context  # noqa: SLF001
        assert context is not None
        initialization_call_count = len(connection.calls)

        writes.execute_runtime_insert_many(
            context,
            "runs",
            (
                (
                    ("run_id", "run-1"),
                    ("runner_name", "runner"),
                    ("status", "succeeded"),
                    ("dry_run", False),
                    ("script_path", None),
                    ("script_content_hash", None),
                    ("started_at", datetime(2026, 1, 1)),
                    ("completed_at", None),
                ),
            ),
        )
    finally:
        store.close()

    assert len(connection.calls) == initialization_call_count
    assert storage_module.client is not None
    request = storage_module.client.requests[0]
    assert (
        request.write_stream == "projects/example-runtime-project/datasets/retl_runtime/tables/"
        "runs/_default"
    )
    assert len(request.proto_rows.rows.serialized_rows) == 1


def test_bigquery_destination_batch_writes_use_sql_for_immediate_readback() -> None:
    backend = _backend()
    connection = RecordingSqlConnection()
    storage_module = RecordingBigQueryStorageWriteModule()
    store = backend.runtime_store(
        client=connection,
        bigquery_storage_module=storage_module,
    )
    try:
        context = store._runtime_context  # noqa: SLF001
        assert context is not None
        initialization_call_count = len(connection.calls)

        writes.execute_runtime_insert_many(
            context,
            "destination_batches",
            (
                (
                    ("batch_id", "batch-1"),
                    ("run_id", "run-1"),
                    ("attempt_id", "attempt-1"),
                    ("sync_name", "sync"),
                    ("destination_name", "dest"),
                    ("surface", "profile"),
                    ("family", "state"),
                    ("declaration_name", "customers"),
                    ("declaration_version_id", "decl-1"),
                    ("source_page_index", 0),
                    ("reconcile_page_index", 0),
                    ("first_collect_id", "00000000-0001-7000-8000-000000000000"),
                    ("last_collect_id", "00000000-0001-7000-8000-000000000000"),
                    ("first_sequence_order", 0),
                    ("last_sequence_order", 0),
                    ("has_source_range", False),
                    ("state_lower_collect_id", None),
                    ("state_lower_sequence_order", None),
                    ("state_first_identity_json", None),
                    ("state_last_identity_json", None),
                    ("state_upper_identity_json", None),
                    ("state_lower_identity_json", None),
                    ("event_lower_cursor_value", None),
                    ("event_lower_primary_key_value", None),
                    ("event_first_cursor_value", None),
                    ("event_first_primary_key_value", None),
                    ("event_last_cursor_value", None),
                    ("event_last_primary_key_value", None),
                    ("event_upper_cursor_value", None),
                    ("event_upper_primary_key_value", None),
                    ("event_cursor_kind", None),
                    ("event_primary_key_kind", None),
                    ("destination_batch_index", 0),
                    ("record_count", 1),
                    ("payload_fingerprint", "payload:1"),
                    ("target_request_fingerprint", "request:1"),
                    ("status", "succeeded"),
                    ("completion_state", "resolved"),
                    ("attempt_count", 1),
                    ("last_error_summary", None),
                    ("last_error_detail", None),
                    ("last_failure_category", None),
                    ("retry_eligible", False),
                    ("http_status", 200),
                    ("first_submitted_at", None),
                    ("last_attempted_at", None),
                    ("completed_at", None),
                ),
            ),
        )
    finally:
        store.close()

    assert len(connection.calls) == initialization_call_count + 1
    assert "`destination_batches`" in connection.calls[-1][0]
    assert storage_module.client is None


class RecordingSqlConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.close_count = 0

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        self.calls.append((sql, tuple(parameters)))
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return []

    def close(self) -> None:
        self.close_count += 1


def _table_name_from_create_statement(sql: str) -> str:
    relation_sql = sql.split("create table if not exists ", 1)[1].split(" ", 1)[0]
    return relation_sql.rsplit(".", 1)[1].strip("`")


class RecordingBigQueryStorageWriteModule:
    def __init__(self) -> None:
        self.types = RecordingBigQueryStorageWriteTypes
        self.client: RecordingBigQueryWriteClient | None = None

    def BigQueryWriteClient(self, **kwargs: object) -> "RecordingBigQueryWriteClient":  # noqa: N802
        self.client = RecordingBigQueryWriteClient()
        return self.client


class RecordingBigQueryStorageWriteTypes:
    @dataclass
    class ProtoSchema:
        proto_descriptor: object

    @dataclass
    class ProtoRows:
        serialized_rows: Sequence[bytes]

    @dataclass
    class AppendRowsResponse:
        error: object | None = None
        row_errors: Sequence[object] = ()

    class AppendRowsRequest:
        class MissingValueInterpretation:
            NULL_VALUE = "NULL_VALUE"

        @dataclass
        class ProtoData:
            writer_schema: "RecordingBigQueryStorageWriteTypes.ProtoSchema"
            rows: "RecordingBigQueryStorageWriteTypes.ProtoRows"

        def __init__(
            self,
            *,
            write_stream: str,
            proto_rows: "RecordingBigQueryStorageWriteTypes.AppendRowsRequest.ProtoData",
            default_missing_value_interpretation: object,
        ) -> None:
            self.write_stream = write_stream
            self.proto_rows = proto_rows
            self.default_missing_value_interpretation = default_missing_value_interpretation


class RecordingBigQueryWriteClient:
    def __init__(self) -> None:
        self.requests: list[Any] = []
        self.close_count = 0

    def append_rows(self, requests: Iterable[object]) -> list[object]:
        self.requests.extend(list(requests))
        return [RecordingBigQueryStorageWriteTypes.AppendRowsResponse()]

    def close(self) -> None:
        self.close_count += 1
