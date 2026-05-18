from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pytest

import retl
from retl.backends.duckdb import DuckDBRuntimeStore, DuckDBSqlBackend
from retl.collect_identity import is_uuidv7
from retl.destinations.targets import RemoteTarget, TargetRegistryKey, TargetRegistryRecord
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    EventKeysetScanPosition,
    EventSourceWindowRequest,
    PendingWorkCursor,
    StateOrderedWorkScanPosition,
    StateSnapshotRequest,
    destination_batch_id,
)
from retl.stores.sql_runtime import SqlRuntimeStore


@dataclass(frozen=True)
class SqlRuntimeBackendFixture:
    backend_name: str
    database: Path
    source_schema: str = "source_data"
    runtime_schema: str = "retl"

    @classmethod
    def duckdb(cls, database: Path) -> SqlRuntimeBackendFixture:
        return cls(backend_name="duckdb", database=database)

    def sql_backend(self) -> DuckDBSqlBackend:
        return DuckDBSqlBackend(
            database=self.database,
            source_schema=self.source_schema,
            runtime_schema=self.runtime_schema,
        )

    def runtime_store(self) -> DuckDBRuntimeStore:
        return self.sql_backend().runtime_store()

    def replace_state_rows(self, rows: list[tuple[str, str, str, str]]) -> None:
        with self.source_connection() as connection:
            connection.execute(f"create schema if not exists {self.source_schema}")
            connection.execute(
                f"""
                create or replace table {self.source_schema}.customers (
                    customer_id varchar,
                    email varchar,
                    plan varchar,
                    audience_key varchar
                )
                """
            )
            connection.executemany(
                f"insert into {self.source_schema}.customers values (?, ?, ?, ?)",
                rows,
            )

    def replace_event_rows(self, rows: list[tuple[str, str, str, str, int, str]]) -> None:
        with self.source_connection() as connection:
            connection.execute(f"create schema if not exists {self.source_schema}")
            connection.execute(
                f"""
                create or replace table {self.source_schema}.purchases (
                    purchase_id varchar,
                    customer_id varchar,
                    email varchar,
                    occurred_at varchar,
                    amount integer,
                    sku varchar
                )
                """
            )
            connection.executemany(
                f"insert into {self.source_schema}.purchases values (?, ?, ?, ?, ?, ?)",
                rows,
            )

    def source_connection(self) -> Any:
        return duckdb.connect(str(self.database))


@pytest.fixture
def sql_runtime_backend(tmp_path: Path) -> SqlRuntimeBackendFixture:
    return SqlRuntimeBackendFixture.duckdb(tmp_path / "warehouse.duckdb")


def test_backend_fixture_initializes_schema_and_reopens(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    store = sql_runtime_backend.runtime_store()

    assert isinstance(store, SqlRuntimeStore)
    first_collect_id = store.allocate_collect_id()
    assert is_uuidv7(first_collect_id)
    store.close()

    reopened = sql_runtime_backend.runtime_store()
    try:
        second_collect_id = reopened.allocate_collect_id()
        assert is_uuidv7(second_collect_id)
        assert second_collect_id != first_collect_id
    finally:
        reopened.close()


def test_duckdb_runtime_store_result_arrow_reader_returns_bounded_batches(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    store = sql_runtime_backend.runtime_store()
    try:
        reader = store._connection.execute(
            "select range::bigint as id from range(5) order by id"
        ).to_arrow_reader(batch_size=2)

        first = reader.read_next_batch()
        second = reader.read_next_batch()

        assert isinstance(first, pa.RecordBatch)
        assert first.column(0).to_pylist() == [0, 1]
        assert second.column(0).to_pylist() == [2, 3]
        assert store._connection.execute("select 7::integer as value").fetchone() == (7,)
        assert store._connection.execute("select 8::integer as value").fetchall() == [(8,)]
    finally:
        store.close()


def test_state_collect_reads_distinct_source_and_runtime_schemas(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    sql_runtime_backend.replace_state_rows(
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_2", "two@example.com", "free", "audience_a"),
            ("cust_3", "three@example.com", "team", "audience_b"),
        ]
    )
    backend = sql_runtime_backend.sql_backend()
    declaration = _state_declaration(backend)
    store = sql_runtime_backend.runtime_store()

    result = store.produce_state_collect(
        declaration=declaration,
        snapshot=backend.source_adapter().prepare_state_snapshot(
            StateSnapshotRequest(
                source_name=declaration.source.name,
                query=declaration.source.query,
            )
        ),
    )

    assert is_uuidv7(result.collect_id)
    assert result.current_row_count == 3
    assert result.upsert_count == 3
    assert (
        store.state_current_summary(
            declaration_name="customer_state",
            source_name="customers",
        ).row_count
        == 3
    )
    assert _json_column_values(
        store.read_pending_work(scope=_state_scope(), max_rows=10).payload,
        "key_json",
    ) == [{"customer": "cust_1"}, {"customer": "cust_2"}, {"customer": "cust_3"}]


def test_state_collect_orders_mixed_work_remove_first_and_groups_by_target(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    sql_runtime_backend.replace_state_rows(
        [
            ("cust_remove_b", "remove-b@example.com", "old", "audience_b"),
            ("cust_update_b", "update-b@example.com", "old", "audience_b"),
            ("cust_remove_a", "remove-a@example.com", "old", "audience_a"),
            ("cust_update_a", "update-a@example.com", "old", "audience_a"),
        ]
    )
    backend = sql_runtime_backend.sql_backend()
    declaration = _state_declaration(backend)
    store = sql_runtime_backend.runtime_store()
    first = store.produce_state_collect(
        declaration=declaration,
        snapshot=backend.source_adapter().prepare_state_snapshot(
            StateSnapshotRequest(
                source_name=declaration.source.name,
                query=declaration.source.query,
            )
        ),
    )
    assert first.work_row_count == 4

    sql_runtime_backend.replace_state_rows(
        [
            ("cust_update_b", "update-b@example.com", "new", "audience_b"),
            ("cust_new_b", "new-b@example.com", "new", "audience_b"),
            ("cust_update_a", "update-a@example.com", "new", "audience_a"),
            ("cust_new_a", "new-a@example.com", "new", "audience_a"),
        ]
    )
    second = store.produce_state_collect(
        declaration=declaration,
        snapshot=backend.source_adapter().prepare_state_snapshot(
            StateSnapshotRequest(
                source_name=declaration.source.name,
                query=declaration.source.query,
            )
        ),
    )
    page = store.read_pending_work(
        scope=_state_scope(),
        source_collect_id=second.collect_id,
        max_rows=10,
    )

    assert second.remove_count == 2
    assert second.upsert_count == 4
    assert _column_values(page.payload, "kind") == [
        "remove",
        "remove",
        "upsert",
        "upsert",
        "upsert",
        "upsert",
    ]
    assert _json_column_values(page.payload, "target_json") == [
        {"value": "audience_a"},
        {"value": "audience_b"},
        {"value": "audience_a"},
        {"value": "audience_a"},
        {"value": "audience_b"},
        {"value": "audience_b"},
    ]
    assert _json_column_values(page.payload, "key_json") == [
        {"customer": "cust_remove_a"},
        {"customer": "cust_remove_b"},
        {"customer": "cust_new_a"},
        {"customer": "cust_update_a"},
        {"customer": "cust_new_b"},
        {"customer": "cust_update_b"},
    ]
    assert _column_values(page.payload, "sequence_order") == [0, 1, 2, 3, 4, 5]


def test_state_collect_expands_list_identifier_mappings(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    with sql_runtime_backend.source_connection() as connection:
        connection.execute(f"create schema if not exists {sql_runtime_backend.source_schema}")
        connection.execute(
            f"""
            create or replace table {sql_runtime_backend.source_schema}.customer_email_lists (
                customer_id varchar,
                emails varchar[],
                audience_key varchar
            )
            """
        )
        connection.executemany(
            f"insert into {sql_runtime_backend.source_schema}.customer_email_lists values (?, ?, ?)",
            [
                ("cust_1", ["b@example.test", "a@example.test"], "audience_a"),
                ("cust_2", [], "audience_a"),
                ("cust_3", None, "audience_a"),
            ],
        )
    backend = sql_runtime_backend.sql_backend()
    declaration = _state_list_identifier_declaration(backend)
    store = sql_runtime_backend.runtime_store()

    store.produce_state_collect(
        declaration=declaration,
        snapshot=backend.source_adapter().prepare_state_snapshot(
            StateSnapshotRequest(
                source_name=declaration.source.name,
                query=declaration.source.query,
            )
        ),
    )
    page = store.read_pending_work(scope=_state_list_identifier_scope(), max_rows=10)

    assert _json_column_values(page.payload, "identifiers_json") == [
        [
            {"type": "email", "value": "a@example.test"},
            {"type": "email", "value": "b@example.test"},
        ],
        [],
        [],
    ]


def test_event_collect_keyset_scans_across_windows(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    sql_runtime_backend.replace_event_rows(
        [
            ("p_2", "cust_2", "two@example.com", "2024-01-02T00:00:00Z", 200, "sku_b"),
            ("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a"),
            ("p_3", "cust_3", "three@example.com", "2024-01-03T00:00:00Z", 300, "sku_c"),
        ]
    )
    backend = sql_runtime_backend.sql_backend()
    declaration = _event_declaration(backend)
    store = sql_runtime_backend.runtime_store()

    first = store.produce_event_collect(
        declaration=declaration,
        window=_event_window(backend, declaration, scan_after=None, limit=2),
    )
    second = store.produce_event_collect(
        declaration=declaration,
        window=_event_window(backend, declaration, scan_after=first.scan_upper_bound, limit=10),
    )
    first_page = store.read_event_source_window(
        declaration=declaration,
        window=_event_window(backend, declaration, scan_after=None, limit=2),
        max_rows=2,
    )
    second_page = store.read_event_source_window(
        declaration=declaration,
        window=_event_window(backend, declaration, scan_after=first.scan_upper_bound, limit=10),
        max_rows=10,
    )

    assert first.scan_upper_bound == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2024-01-02T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("p_2"),
    )
    assert second.scan_after == first.scan_upper_bound
    assert second.scan_upper_bound == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2024-01-03T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("p_3"),
    )
    assert _json_column_values(first_page.payload, "key_json") + _json_column_values(
        second_page.payload,
        "key_json",
    ) == [
        {"purchase": "p_1"},
        {"purchase": "p_2"},
        {"purchase": "p_3"},
    ]
    assert _column_values(second_page.payload, "event_cursor_value")[0] == "2024-01-03T00:00:00Z"
    assert _column_values(second_page.payload, "event_primary_key_value")[0] == "p_3"


def test_ordered_work_and_state_current_paginate_with_store_issued_cursors(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    sql_runtime_backend.replace_state_rows(
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_2", "two@example.com", "free", "audience_a"),
            ("cust_3", "three@example.com", "team", "audience_a"),
            ("cust_4", "four@example.com", "enterprise", "audience_a"),
        ]
    )
    backend = sql_runtime_backend.sql_backend()
    declaration = _state_declaration(backend)
    store = sql_runtime_backend.runtime_store()
    store.produce_state_collect(
        declaration=declaration,
        snapshot=backend.source_adapter().prepare_state_snapshot(
            StateSnapshotRequest(
                source_name=declaration.source.name,
                query=declaration.source.query,
            )
        ),
    )

    first_work_page = store.read_pending_work(scope=_state_scope(), max_rows=2)
    second_work_page = store.read_pending_work(
        scope=_state_scope(),
        max_rows=2,
        cursor=cast(PendingWorkCursor, first_work_page.next_cursor),
    )
    first_current_page = store.read_state_current_upserts(
        declaration_name="customer_state",
        source_name="customers",
        max_rows=2,
    )
    second_current_page = store.read_state_current_upserts(
        declaration_name="customer_state",
        source_name="customers",
        max_rows=2,
        cursor=first_current_page.next_cursor,
    )

    assert first_work_page.next_cursor is not None
    assert _json_column_values(first_work_page.payload, "key_json") == [
        {"customer": "cust_1"},
        {"customer": "cust_2"},
    ]
    assert _json_column_values(second_work_page.payload, "key_json") == [
        {"customer": "cust_3"},
        {"customer": "cust_4"},
    ]
    assert second_work_page.next_cursor is None
    assert first_current_page.next_cursor is not None
    assert _json_column_values(first_current_page.payload, "key_json") == [
        {"customer": "cust_1"},
        {"customer": "cust_2"},
    ]
    assert _json_column_values(second_current_page.payload, "key_json") == [
        {"customer": "cust_3"},
        {"customer": "cust_4"},
    ]
    assert second_current_page.next_cursor is None


def test_destination_progress_reads_updates_and_reopens(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    scope = _state_scope()
    position = StateOrderedWorkScanPosition(
        collect_id="00000000-0003-7000-8000-000000000000", sequence_order=7
    )
    store = sql_runtime_backend.runtime_store()

    missing = store.get_destination_progress(scope)
    registered = store.register_destination_progress(scope)
    updated = store.update_destination_progress(scope=scope, position=position)
    store.close()

    reopened = sql_runtime_backend.runtime_store()
    try:
        assert missing.position is None
        assert registered.position is None
        assert updated.before is None
        assert updated.after == position
        assert updated.advanced is True
        assert reopened.get_destination_progress(scope).position == position
    finally:
        reopened.close()


def test_destination_batch_ledger_round_trips_work_and_attempt_flows(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    store = _store_with_state_collect(
        sql_runtime_backend,
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_2", "two@example.com", "free", "audience_a"),
            ("cust_3", "three@example.com", "team", "audience_a"),
        ],
    )
    collect_id = store.read_pending_work(scope=_state_scope(), max_rows=1).first_collect_id
    assert collect_id is not None
    batch = _destination_batch(
        collect_id=collect_id,
        first_sequence_order=0,
        last_sequence_order=2,
    )

    stored = store.upsert_destination_batch(batch)
    fetched = store.get_destination_batch(batch_id=batch.batch_id)
    fetched_many = store.get_destination_batches(batch_ids=(batch.batch_id,))
    pending_list = store.list_destination_batches(scope=_state_scope(), statuses=("pending",))
    work = store.read_destination_batch_work(batch=batch)
    failed = store.upsert_destination_batch(
        replace(
            batch,
            run_id="run-1",
            attempt_id="attempt-1",
            status="failed",
            completion_state="unresolved",
            attempt_count=1,
            retry_eligible=True,
            http_status=503,
            first_submitted_at=datetime(2026, 5, 6, 10, 0, 0),
            last_attempted_at=datetime(2026, 5, 6, 10, 0, 0),
        )
    )
    retry_candidates = store.list_destination_batch_retry_candidates(
        scope=_state_scope(),
        retry_limit=2,
    )
    succeeded = store.upsert_destination_batch(
        replace(
            failed,
            run_id="run-2",
            attempt_id="attempt-2",
            status="succeeded",
            completion_state="resolved",
            attempt_count=2,
            retry_eligible=False,
            http_status=200,
            last_attempted_at=datetime(2026, 5, 6, 10, 5, 0),
            completed_at=datetime(2026, 5, 6, 10, 5, 0),
        )
    )

    assert stored == batch
    assert fetched == batch
    assert fetched_many == (batch,)
    assert pending_list == (batch,)
    assert work.row_count == 3
    assert failed.status == "failed"
    assert failed.attempt_count == 1
    assert retry_candidates == (failed,)
    assert succeeded.status == "succeeded"
    assert succeeded.completion_state == "resolved"
    assert succeeded.attempt_count == 2
    assert store.list_destination_batches(scope=_state_scope(), statuses=("succeeded",)) == (
        succeeded,
    )


def test_target_registry_round_trips_across_reopen(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    store = sql_runtime_backend.runtime_store()
    key = TargetRegistryKey(
        binding_name="audience_prod",
        destination_ref="retl/audience",
        surface="custom_audience_membership",
        logical_target="vip",
    )
    record = TargetRegistryRecord(
        key=key,
        remote=RemoteTarget(
            remote_id="aud_123",
            display_name="VIP Customers'); drop table target_registry; --",
            metadata={
                "account_id": "act_123",
                "note": "literal select * from target_registry where id = ?",
                "retention_days": 180,
            },
        ),
        source="managed_created",
    )

    store.put(record)
    store.close()

    reopened = sql_runtime_backend.runtime_store()
    try:
        assert reopened.get(key) == record
    finally:
        reopened.close()


def test_report_persists_without_private_store_reads(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    store = sql_runtime_backend.runtime_store()
    report = _SyncReportRecord(
        run_id="run-1",
        attempt_id="attempt-1",
        sync_name="customer_sync",
    )

    store.record_sync_report(report)
    store.close()

    with sql_runtime_backend.source_connection() as connection:
        sync_report = connection.execute(
            f"""
            select report_id, run_id, attempt_id, sync_name, status, succeeded_record_count
            from {sql_runtime_backend.runtime_schema}.sync_reports
            where run_id = 'run-1'
            """
        ).fetchone()

    assert sync_report == (
        "run-1:attempt-1:customer_sync",
        "run-1",
        "attempt-1",
        "customer_sync",
        "succeeded",
        0,
    )


def test_retention_cleanup_uses_progress_watermark(
    sql_runtime_backend: SqlRuntimeBackendFixture,
) -> None:
    store = _store_with_state_collect(
        sql_runtime_backend,
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_2", "two@example.com", "free", "audience_a"),
        ],
    )
    collect_id = store.read_pending_work(scope=_state_scope(), max_rows=1).first_collect_id
    assert collect_id is not None
    store.update_destination_progress(
        scope=_state_scope(),
        position=StateOrderedWorkScanPosition(collect_id=collect_id, sequence_order=1),
    )

    assert (
        store.retention_watermark(family="state", declaration_name="customer_state") == collect_id
    )

    cleanup = store.cleanup_ordered_work(
        family="state",
        declaration_name="customer_state",
        through_collect_id=collect_id,
    )

    assert cleanup.safe_through_collect_id == collect_id
    assert cleanup.deleted_ordered_work_count == 2
    assert cleanup.retained_pending_count == 0
    assert store.read_pending_work(scope=_state_scope(), max_rows=10).row_count == 0


def _store_with_state_collect(
    fixture: SqlRuntimeBackendFixture,
    rows: list[tuple[str, str, str, str]],
) -> DuckDBRuntimeStore:
    fixture.replace_state_rows(rows)
    backend = fixture.sql_backend()
    declaration = _state_declaration(backend)
    store = fixture.runtime_store()
    store.produce_state_collect(
        declaration=declaration,
        snapshot=backend.source_adapter().prepare_state_snapshot(
            StateSnapshotRequest(
                source_name=declaration.source.name,
                query=declaration.source.query,
            )
        ),
    )
    return store


def _state_declaration(backend: DuckDBSqlBackend) -> retl.State:
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="""
                select customer_id, email, plan, audience_key
                from customers
            """,
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _state_list_identifier_declaration(backend: DuckDBSqlBackend) -> retl.State:
    return retl.state(
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


def _event_declaration(backend: DuckDBSqlBackend) -> retl.Event:
    return retl.event(
        name="purchase_event",
        source=retl.source(
            name="purchases",
            query="""
                select purchase_id, customer_id, email, occurred_at, amount, sku
                from purchases
            """,
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
        payload={"amount": "amount", "sku": "sku"},
    )


def _event_window(
    backend: DuckDBSqlBackend,
    declaration: retl.Event,
    *,
    scan_after: EventKeysetScanPosition | None,
    limit: int,
) -> Any:
    return backend.source_adapter().prepare_event_source_window(
        EventSourceWindowRequest(
            source_name=declaration.source.name,
            query=declaration.source.query,
            cursor_column="occurred_at",
            primary_key_column="purchase_id",
            scan_after=scan_after,
            limit=limit,
        )
    )


def _state_scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name="test_destination",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )


def _state_list_identifier_scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name="test_destination",
        surface="profile",
        family="state",
        declaration_name="customer_email_list_state",
    )


def _event_scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="purchase_sync",
        destination_name="test_destination",
        surface="purchase_event",
        family="event",
        declaration_name="purchase_event",
    )


def _destination_batch(
    *,
    collect_id: str,
    first_sequence_order: int,
    last_sequence_order: int,
) -> DestinationBatchRecord:
    identity = DestinationBatchIdentity(
        scope=_state_scope(),
        declaration_version_id="decl:customer_state",
        reconcile_page_index=0,
        first_collect_id=collect_id,
        last_collect_id=collect_id,
        first_sequence_order=first_sequence_order,
        last_sequence_order=last_sequence_order,
        destination_batch_index=0,
        payload_fingerprint="payload:fingerprint",
        target_request_fingerprint="request:fingerprint",
    )
    return DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        record_count=(last_sequence_order - first_sequence_order) + 1,
    )


def _column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return batch.column(column).to_pylist()


def _json_column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return [json.loads(value) if value else None for value in _column_values(batch, column)]


@dataclass(frozen=True)
class _ReportRef:
    ref: str


@dataclass(frozen=True)
class _ReportPhase:
    status: str = "succeeded"


@dataclass(frozen=True)
class _ReportDestination:
    submission_status: str = "confirmed"
    attempted_count: int = 0
    confirmed_count: int = 0
    accepted_count: int = 0
    retryable_failure_count: int = 0
    terminal_failure_count: int = 0
    pre_acceptance_failure_count: int = 0
    failure_category: str | None = None
    http_status: int | None = None
    last_error_summary: str | None = None
    last_error_detail: str | None = None


@dataclass(frozen=True)
class _ReportCommit:
    progress_advanced: bool = True


@dataclass(frozen=True)
class _SyncReportRecord:
    run_id: str
    attempt_id: str
    sync_name: str
    ref: _ReportRef = _ReportRef("sync-report:customer-sync")
    runner_name: str = "runner"
    declaration_name: str = "customer_state"
    declaration_version_id: str = "decl:customer_state"
    declaration_kind: str = "state"
    destination_binding_name: str = "test_destination"
    surface: str = "profile"
    destination: _ReportDestination = _ReportDestination()
    commit: _ReportCommit = _ReportCommit()
    phases: tuple[_ReportPhase, ...] = (_ReportPhase(),)
    status: str = "succeeded"

    @property
    def report_id(self) -> str:
        return f"{self.run_id}:{self.attempt_id}:{self.sync_name}"

    def to_dict(self) -> dict[str, object]:
        return {
            "report_id": self.report_id,
            "attempt_id": self.attempt_id,
            "run_id": self.run_id,
            "sync_name": self.sync_name,
            "surface": self.surface,
        }
