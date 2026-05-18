from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from retl.backends.duckdb import DuckDBRuntimeStore
from retl.errors import DeclarationValidationError
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationBatchStatus,
    DestinationProgressScope,
    DestinationScanRange,
    EventKeysetScanPosition,
    OrderedWorkInput,
    WorkFamily,
    destination_batch_id,
)
from tests.runtime.ordered_work_helpers import append_ordered_work


class _CountingConnection:
    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self.execute_sql: list[str] = []

    def execute(self, sql: object, *args: Any, **kwargs: Any) -> Any:
        self.execute_sql.append(str(sql))
        return self._connection.execute(sql, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")


def _scope(
    *,
    sync_name: str = "customer_sync",
    destination_name: str = "destination",
    surface: str = "events",
    family: WorkFamily = "event",
    declaration_name: str = "purchase_events",
) -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name=sync_name,
        destination_name=destination_name,
        surface=surface,
        family=family,
        declaration_name=declaration_name,
    )


def _identity(
    *,
    scope: DestinationProgressScope | None = None,
    declaration_version_id: str = "decl:supported-one",
    source_range: DestinationScanRange | None = None,
    source_page_index: int | None = None,
    reconcile_page_index: int | None = 0,
    first_collect_id: str = "00000000-0001-7000-8000-000000000000",
    last_collect_id: str = "00000000-0001-7000-8000-000000000000",
    first_sequence_order: int = 0,
    last_sequence_order: int = 4,
    destination_batch_index: int = 0,
    payload_fingerprint: str = "payload:abc",
    target_request_fingerprint: str = "request:redacted:abc",
) -> DestinationBatchIdentity:
    return DestinationBatchIdentity(
        scope=scope or _scope(),
        declaration_version_id=declaration_version_id,
        source_range=source_range,
        source_page_index=source_page_index,
        reconcile_page_index=reconcile_page_index,
        first_collect_id=first_collect_id,
        last_collect_id=last_collect_id,
        first_sequence_order=first_sequence_order,
        last_sequence_order=last_sequence_order,
        destination_batch_index=destination_batch_index,
        payload_fingerprint=payload_fingerprint,
        target_request_fingerprint=target_request_fingerprint,
    )


def _batch(
    identity: DestinationBatchIdentity | None = None, *, record_count: int = 5
) -> DestinationBatchRecord:
    identity = identity or _identity()
    return DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        record_count=record_count,
    )


def _touched(
    batch: DestinationBatchRecord,
    *,
    status: DestinationBatchStatus,
    run_id: str = "run-1",
    attempt_id: str = "attempt-1",
    retry_eligible: bool | None = False,
    http_status: int | None = 200,
    attempted_at: datetime | None = None,
    completed_at: datetime | None = None,
    summary: str | None = None,
    detail: str | None = None,
    category: str | None = None,
) -> DestinationBatchRecord:
    attempted_at = attempted_at or datetime(2026, 5, 6, 10, 0, 0)
    resolved = status in {"accepted", "succeeded", "skipped"}
    return replace(
        batch,
        run_id=run_id,
        attempt_id=attempt_id,
        status=status,
        completion_state="resolved" if resolved else "unresolved",
        attempt_count=batch.attempt_count + 1,
        retry_eligible=retry_eligible,
        http_status=http_status,
        last_error_summary=summary,
        last_error_detail=detail,
        last_failure_category=category,
        first_submitted_at=batch.first_submitted_at or attempted_at,
        last_attempted_at=attempted_at,
        completed_at=completed_at if resolved else None,
    )


def test_destination_batch_ledger_persists_current_state_across_reopen(tmp_path: Path) -> None:
    database = tmp_path / "runtime.duckdb"
    store = DuckDBRuntimeStore(database=database)
    batch = _touched(
        _batch(),
        status="failed",
        retry_eligible=True,
        http_status=503,
        summary="Authorization: Bearer secret-token retry later",
        detail='{"client_secret":"json-secret"}',
        category="retryable",
    )

    stored = store.upsert_destination_batch(batch)
    store.close()
    reopened = DuckDBRuntimeStore(database=database)
    persisted = reopened.get_destination_batch(batch_id=batch.batch_id)

    assert persisted == stored
    assert persisted is not None
    assert persisted.run_id == "run-1"
    assert persisted.attempt_id == "attempt-1"
    assert persisted.record_count == 5
    assert persisted.last_error_summary is not None
    assert "secret-token" not in persisted.last_error_summary
    assert persisted.last_error_detail == "{client_secret=[redacted]}"
    assert reopened.list_destination_batches(scope=_scope()) == (stored,)


def test_destination_batch_source_range_round_trips_through_duckdb(tmp_path: Path) -> None:
    source_range = DestinationScanRange(
        lower_bound_exclusive=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(41),
            primary_key_value=CanonicalKeyScalar.string("purchase-041"),
        ),
        first_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(42),
            primary_key_value=CanonicalKeyScalar.string("purchase-042"),
        ),
        last_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(44),
            primary_key_value=CanonicalKeyScalar.string("purchase-044"),
        ),
        upper_bound_inclusive=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(44),
            primary_key_value=CanonicalKeyScalar.string("purchase-044"),
        ),
    )
    store = _store(tmp_path)
    batch = _batch(_identity(source_range=source_range))

    stored = store.upsert_destination_batch(batch)
    persisted = store.get_destination_batch(batch_id=batch.batch_id)

    assert stored.identity.source_range == source_range
    assert persisted is not None
    assert persisted.identity.source_range == source_range
    assert persisted.batch_id == destination_batch_id(persisted.identity)


def test_destination_batch_bulk_current_state_update_uses_bounded_sql(tmp_path: Path) -> None:
    store = _store(tmp_path)
    batches = tuple(
        _batch(
            _identity(
                first_sequence_order=index * 10,
                last_sequence_order=index * 10 + 9,
                destination_batch_index=index,
                payload_fingerprint=f"payload:{index}",
                target_request_fingerprint=f"request:{index}",
            ),
            record_count=10,
        )
        for index in range(20)
    )
    stored = store.upsert_destination_batches(batches)
    counting = _CountingConnection(store._connection)
    store._connection = counting
    final_rows = tuple(
        _touched(
            batch,
            status="succeeded",
            attempt_id=f"attempt-{index}",
            completed_at=datetime(2026, 5, 6, 10, index, 0),
        )
        for index, batch in enumerate(stored)
    )

    updated = store.upsert_destination_batches(
        final_rows,
        existing_batches=stored,
        read_back=False,
    )
    destination_batch_updates = [
        sql for sql in counting.execute_sql if 'update "retl"."destination_batches"' in sql.lower()
    ]

    assert len(updated) == 20
    assert {batch.status for batch in updated} == {"succeeded"}
    assert len(destination_batch_updates) == 1
    assert "UNION ALL" in destination_batch_updates[0]


def test_bulk_planning_upsert_does_not_reset_existing_batch_evidence(tmp_path: Path) -> None:
    store = _store(tmp_path)
    pending = _batch()
    attempted = store.upsert_destination_batch(
        _touched(
            pending,
            status="failed",
            retry_eligible=True,
            http_status=503,
            summary="preserve this evidence",
            detail="preserve detail",
            category="retryable",
        )
    )

    after_planning = store.upsert_destination_batches((pending,))[0]

    assert after_planning == attempted
    assert after_planning.status == "failed"
    assert after_planning.attempt_count == 1
    assert after_planning.attempt_id == "attempt-1"
    assert after_planning.last_error_summary == "preserve this evidence"


def test_destination_batch_failed_status_can_be_operator_resolved_without_retry(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    failed = store.upsert_destination_batch(
        _touched(
            _batch(),
            status="failed",
            retry_eligible=False,
            http_status=422,
            summary="Invalid record",
            category="terminal_record",
        )
    )
    completed_at = datetime(2026, 5, 6, 11, 0, 0)

    resolved = store.upsert_destination_batch(
        replace(failed, completion_state="resolved", completed_at=completed_at)
    )

    assert resolved.status == "failed"
    assert resolved.completion_state == "resolved"
    assert resolved.completed_at == completed_at


def test_destination_batch_skipped_status_persists_as_terminal_non_retryable(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    skipped = replace(
        _batch(),
        status="skipped",
        completion_state="resolved",
        retry_eligible=False,
        completed_at=datetime(2026, 5, 7, 9, 0, 0),
    )

    stored = store.upsert_destination_batch(skipped)

    assert stored.status == "skipped"
    assert stored.completion_state == "resolved"
    assert stored.retry_eligible is False
    assert store.list_destination_batches(scope=_scope(), statuses=("skipped",)) == (stored,)


def test_destination_batch_skipped_status_rejects_unresolved_or_retryable_rows(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)

    with pytest.raises(DeclarationValidationError, match="completion_state"):
        store.upsert_destination_batch(
            replace(_batch(), status="skipped", completion_state="unresolved")
        )

    with pytest.raises(DeclarationValidationError, match="cannot be retryable"):
        store.upsert_destination_batch(
            replace(
                _batch(),
                status="skipped",
                completion_state="resolved",
                retry_eligible=True,
            )
        )


def test_dismiss_unresolved_destination_batches_skips_pending_and_failed_in_scope(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    scope = _scope()
    pending = store.upsert_destination_batch(
        _batch(
            _identity(
                scope=scope,
                payload_fingerprint="payload:pending-dismiss",
                target_request_fingerprint="request:pending-dismiss",
            )
        )
    )
    failed_identity = _identity(
        scope=scope,
        destination_batch_index=1,
        first_sequence_order=5,
        last_sequence_order=9,
        payload_fingerprint="payload:failed-dismiss",
        target_request_fingerprint="request:failed-dismiss",
    )
    failed = store.upsert_destination_batch(
        _touched(
            _batch(failed_identity),
            status="failed",
            retry_eligible=True,
            http_status=503,
            summary="preserve failed summary",
            detail="preserve failed detail",
            category="retryable",
        )
    )

    dismissed = store.dismiss_unresolved_destination_batches(scope=scope)
    persisted = tuple(
        store.get_destination_batch(batch_id=batch.batch_id) for batch in (pending, failed)
    )

    assert {batch.batch_id for batch in dismissed} == {pending.batch_id, failed.batch_id}
    assert persisted == dismissed
    assert {batch.status for batch in dismissed} == {"skipped"}
    assert {batch.completion_state for batch in dismissed} == {"resolved"}
    assert {batch.retry_eligible for batch in dismissed} == {False}
    assert all(batch.completed_at is not None for batch in dismissed)
    dismissed_failed = next(batch for batch in dismissed if batch.batch_id == failed.batch_id)
    assert dismissed_failed.attempt_count == failed.attempt_count
    assert dismissed_failed.run_id == failed.run_id
    assert dismissed_failed.attempt_id == failed.attempt_id
    assert dismissed_failed.last_error_summary == failed.last_error_summary
    assert dismissed_failed.last_error_detail == failed.last_error_detail
    assert dismissed_failed.last_failure_category == failed.last_failure_category
    assert dismissed_failed.first_submitted_at == failed.first_submitted_at
    assert dismissed_failed.last_attempted_at == failed.last_attempted_at


def test_read_destination_batch_work_drains_all_rows_across_collect_ids(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    scope = _scope(family="state", declaration_name="customer_state")
    append_ordered_work(
        store,
        [
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_1"},
                identifiers=({"type": "email", "value": "one@example.com"},),
                payload={"plan": "pro"},
            ),
            OrderedWorkInput(
                collect_id="00000000-0002-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_2"},
                identifiers=({"type": "email", "value": "two@example.com"},),
                payload={"plan": "free"},
            ),
            OrderedWorkInput(
                collect_id="00000000-0002-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_3"},
                identifiers=({"type": "email", "value": "three@example.com"},),
                payload={"plan": "enterprise"},
            ),
        ],
    )
    batch = _batch(
        _identity(
            scope=scope,
            first_collect_id="00000000-0001-7000-8000-000000000000",
            last_collect_id="00000000-0002-7000-8000-000000000000",
            first_sequence_order=0,
            last_sequence_order=1,
            payload_fingerprint="payload:multi-collect",
            target_request_fingerprint="request:multi-collect",
        )
    )

    page = store.read_destination_batch_work(batch=batch)

    assert page.row_count == 3
    assert page.payload.to_pydict()["sequence_order"] == [0, 0, 1]
