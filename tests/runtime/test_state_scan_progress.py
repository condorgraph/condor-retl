from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.surfaces import DestinationSurface
from retl.errors import DeclarationValidationError
from retl.runtime.progress import advance_progress_after_sync, destination_progress_scope
from retl.runtime.staging import StagePageBoundary
from retl.stores.contracts import (
    CanonicalKey,
    CanonicalKeyScalar,
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    DestinationScanRange,
    ScanPosition,
    StateCurrentSnapshotScanPosition,
    StateOrderedWorkScanPosition,
    destination_batch_id,
)


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")


def _state() -> retl.State:
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="select customer_id, email, plan from customers",
        ),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _sync(
    *,
    destination_name: str = "crm",
    on_failure: retl.FailureHandlingMode = "continue_on_any",
) -> retl.Sync:
    return retl.sync(
        name="customer_sync",
        declaration=_state(),
        destination=retl.DestinationBinding(
            binding_name=destination_name,
            destination_ref="retl/mock",
        ),
        surface="profile",
        on_failure=on_failure,
    )


def _surface() -> DestinationSurface:
    return DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        target_mode="optional",
    )


def _submission(
    *,
    status: str = "planned",
    request_batch_count: int = 1,
) -> DestinationSubmissionEvidence:
    if status == "retryable_failure":
        return DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=2,
            retryable_failure_count=2,
            request_batch_count=request_batch_count,
            summary="destination attempt failed after ledger persistence",
        )
    return DestinationSubmissionEvidence(
        status="planned",
        attempted_count=2,
        request_batch_count=request_batch_count,
        summary="destination batch ledger persisted",
    )


def _pending_reconciled(
    *,
    collect_id: str,
    last_sequence_order: int,
) -> SimpleNamespace:
    boundary = StagePageBoundary(
        first_collect_id=collect_id,
        last_collect_id=collect_id,
        first_sequence_order=0,
        last_sequence_order=last_sequence_order,
        complete_through_collect_id=None,
    )
    return SimpleNamespace(
        mode="pending",
        operation_count=last_sequence_order + 1,
        progress_boundary=boundary,
        next_cursor=None,
        dry_run=False,
    )


def _current_snapshot_reconciled(*, identities: tuple[str, ...]) -> SimpleNamespace:
    payload = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "identity_json": identity,
            }
            for identity in identities
        ]
    ).to_batches()[0]
    page = SimpleNamespace(payload=payload)
    return SimpleNamespace(
        mode="resend_all",
        operation_count=len(identities),
        operation_pages=(page,),
        next_cursor=None,
        dry_run=False,
    )


def _scan_range(
    *,
    first: ScanPosition,
    last: ScanPosition,
) -> DestinationScanRange:
    return DestinationScanRange(
        first_record_position=first,
        last_record_position=last,
        upper_bound_inclusive=last,
    )


def _batch(
    *,
    scope: DestinationProgressScope,
    source_range: DestinationScanRange,
    index: int = 0,
    status: str = "pending",
) -> DestinationBatchRecord:
    first = source_range.first_record_position
    last = source_range.last_record_position
    identity = DestinationBatchIdentity(
        scope=scope,
        declaration_version_id="decl:customer_state",
        source_range=source_range,
        reconcile_page_index=1,
        first_collect_id=str(getattr(first, "collect_id", "00000000-0001-7000-8000-000000000000")),
        last_collect_id=str(getattr(last, "collect_id", "00000000-0001-7000-8000-000000000000")),
        first_sequence_order=int(getattr(first, "sequence_order", 0)),
        last_sequence_order=int(getattr(last, "sequence_order", 0)),
        destination_batch_index=index,
        payload_fingerprint=f"payload:{index}",
        target_request_fingerprint=f"request:{index}",
    )
    return DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        record_count=(identity.last_sequence_order - identity.first_sequence_order) + 1,
        status=status,  # type: ignore[arg-type]
    )


def _failed_batch(
    batch: DestinationBatchRecord,
    *,
    retry_eligible: bool,
    http_status: int,
) -> DestinationBatchRecord:
    return DestinationBatchRecord(
        batch_id=batch.batch_id,
        identity=batch.identity,
        run_id="run-1",
        attempt_id="attempt-1",
        record_count=batch.record_count,
        status="failed",
        completion_state="unresolved",
        attempt_count=1,
        retry_eligible=retry_eligible,
        http_status=http_status,
    )


def test_state_ordered_scan_progress_advances_after_durable_ledger_rows(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _sync()
    scope = destination_progress_scope(sync)
    upper = StateOrderedWorkScanPosition(
        collect_id="00000000-0003-7000-8000-000000000000", sequence_order=4
    )
    batch = _batch(
        scope=scope,
        source_range=_scan_range(
            first=StateOrderedWorkScanPosition(
                collect_id="00000000-0003-7000-8000-000000000000", sequence_order=0
            ),
            last=upper,
        ),
    )
    durable = store.upsert_destination_batch(batch)

    advanced = advance_progress_after_sync(
        store=store,
        sync=sync,
        surface=_surface(),
        reconciled=_pending_reconciled(
            collect_id="00000000-0003-7000-8000-000000000000", last_sequence_order=4
        ),
        evidence=_submission(),
        destination_batches=(durable,),
    )

    assert advanced.progress.after == upper
    assert advanced.progress.advanced is True
    assert store.get_destination_progress(scope).position == upper


def test_state_scan_progress_requires_matching_destination_scope(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _sync(destination_name="crm")
    other_sync = _sync(destination_name="ads")
    scope = destination_progress_scope(sync)
    upper = StateOrderedWorkScanPosition(
        collect_id="00000000-0003-7000-8000-000000000000", sequence_order=4
    )
    other_batch = _batch(
        scope=destination_progress_scope(other_sync),
        source_range=_scan_range(
            first=StateOrderedWorkScanPosition(
                collect_id="00000000-0003-7000-8000-000000000000", sequence_order=0
            ),
            last=upper,
        ),
    )
    durable = store.upsert_destination_batch(other_batch)

    advanced = advance_progress_after_sync(
        store=store,
        sync=sync,
        surface=_surface(),
        reconciled=_pending_reconciled(
            collect_id="00000000-0003-7000-8000-000000000000", last_sequence_order=4
        ),
        evidence=_submission(),
        destination_batches=(durable,),
    )

    assert advanced.progress.advanced is False
    assert "waits for destination batch ledger records" in advanced.progress_decision.reason
    assert store.get_destination_progress(scope).position is None


def test_state_scan_progress_rejects_rollback(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _sync()
    scope = destination_progress_scope(sync)
    store.update_destination_progress(
        scope=scope,
        position=StateOrderedWorkScanPosition(
            collect_id="00000000-0005-7000-8000-000000000000", sequence_order=10
        ),
    )
    rollback = StateOrderedWorkScanPosition(
        collect_id="00000000-0005-7000-8000-000000000000", sequence_order=9
    )
    batch = _batch(
        scope=scope,
        source_range=_scan_range(
            first=StateOrderedWorkScanPosition(
                collect_id="00000000-0005-7000-8000-000000000000", sequence_order=0
            ),
            last=rollback,
        ),
    )
    durable = store.upsert_destination_batch(batch)

    with pytest.raises(DeclarationValidationError, match="cannot move behind"):
        advance_progress_after_sync(
            store=store,
            sync=sync,
            surface=_surface(),
            reconciled=_pending_reconciled(
                collect_id="00000000-0005-7000-8000-000000000000", last_sequence_order=9
            ),
            evidence=_submission(),
            destination_batches=(durable,),
        )


def test_state_scan_progress_ignores_destination_outcome_after_ledger_exists(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _sync()
    scope = destination_progress_scope(sync)
    upper = StateOrderedWorkScanPosition(
        collect_id="00000000-0003-7000-8000-000000000000", sequence_order=4
    )
    batch = _batch(
        scope=scope,
        source_range=_scan_range(
            first=StateOrderedWorkScanPosition(
                collect_id="00000000-0003-7000-8000-000000000000", sequence_order=0
            ),
            last=upper,
        ),
    )
    failed = store.upsert_destination_batch(
        _failed_batch(batch, retry_eligible=True, http_status=503)
    )

    advanced = advance_progress_after_sync(
        store=store,
        sync=sync,
        surface=_surface(),
        reconciled=_pending_reconciled(
            collect_id="00000000-0003-7000-8000-000000000000", last_sequence_order=4
        ),
        evidence=_submission(status="retryable_failure"),
        destination_batches=(failed,),
    )

    assert advanced.progress.after == upper
    assert advanced.progress.advanced is True
    assert store.get_destination_progress(scope).position == upper


def test_state_scan_progress_stop_on_any_blocks_failed_durable_batches(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _sync(on_failure="stop_on_any")
    scope = destination_progress_scope(sync)
    upper = StateOrderedWorkScanPosition(
        collect_id="00000000-0003-7000-8000-000000000000", sequence_order=4
    )
    batch = _batch(
        scope=scope,
        source_range=_scan_range(
            first=StateOrderedWorkScanPosition(
                collect_id="00000000-0003-7000-8000-000000000000", sequence_order=0
            ),
            last=upper,
        ),
    )
    failed = store.upsert_destination_batch(
        _failed_batch(batch, retry_eligible=True, http_status=429)
    )

    advanced = advance_progress_after_sync(
        store=store,
        sync=sync,
        surface=_surface(),
        reconciled=_pending_reconciled(
            collect_id="00000000-0003-7000-8000-000000000000", last_sequence_order=4
        ),
        evidence=_submission(status="retryable_failure"),
        destination_batches=(failed,),
    )

    assert advanced.progress.advanced is False
    assert advanced.progress.after is None
    assert store.get_destination_progress(scope).position is None
    assert "on_failure=stop_on_any blocks progress" in advanced.progress_decision.reason


@pytest.mark.parametrize(
    ("retry_eligible", "advanced"),
    (
        (True, True),
        (False, False),
    ),
)
def test_state_scan_progress_stop_on_terminal_uses_failed_batch_retry_metadata(
    tmp_path: Path,
    retry_eligible: bool,
    advanced: bool,
) -> None:
    store = _store(tmp_path)
    sync = _sync(on_failure="stop_on_terminal")
    scope = destination_progress_scope(sync)
    upper = StateOrderedWorkScanPosition(
        collect_id="00000000-0003-7000-8000-000000000000", sequence_order=4
    )
    batch = _batch(
        scope=scope,
        source_range=_scan_range(
            first=StateOrderedWorkScanPosition(
                collect_id="00000000-0003-7000-8000-000000000000", sequence_order=0
            ),
            last=upper,
        ),
    )
    failed = store.upsert_destination_batch(
        _failed_batch(
            batch, retry_eligible=retry_eligible, http_status=429 if retry_eligible else 401
        )
    )

    result = advance_progress_after_sync(
        store=store,
        sync=sync,
        surface=_surface(),
        reconciled=_pending_reconciled(
            collect_id="00000000-0003-7000-8000-000000000000", last_sequence_order=4
        ),
        evidence=_submission(status="retryable_failure"),
        destination_batches=(failed,),
    )

    assert result.progress.advanced is advanced
    assert store.get_destination_progress(scope).position == (upper if advanced else None)


def test_state_scan_progress_uses_passed_destination_batches_without_batch_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    sync = _sync()
    scope = destination_progress_scope(sync)
    upper = StateOrderedWorkScanPosition(
        collect_id="00000000-0003-7000-8000-000000000000", sequence_order=4
    )
    batch = _batch(
        scope=scope,
        source_range=_scan_range(
            first=StateOrderedWorkScanPosition(
                collect_id="00000000-0003-7000-8000-000000000000", sequence_order=0
            ),
            last=upper,
        ),
    )
    durable = store.upsert_destination_batch(batch)

    def fail_batch_read(*_: object, **__: object) -> None:
        raise AssertionError("progress validation should use the in-memory batch records")

    monkeypatch.setattr(store, "get_destination_batch", fail_batch_read)

    advanced = advance_progress_after_sync(
        store=store,
        sync=sync,
        surface=_surface(),
        reconciled=_pending_reconciled(
            collect_id="00000000-0003-7000-8000-000000000000", last_sequence_order=4
        ),
        evidence=_submission(),
        destination_batches=(durable,),
    )

    assert advanced.progress.after == upper
    assert advanced.progress.advanced is True


def test_state_current_snapshot_last_page_advances_from_bounded_page_evidence(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _sync()
    scope = destination_progress_scope(sync)
    first = StateCurrentSnapshotScanPosition(
        key=CanonicalKey.of(CanonicalKeyScalar.string('{"key":{"customer":"cust_1"}}'))
    )
    upper = StateCurrentSnapshotScanPosition(
        key=CanonicalKey.of(CanonicalKeyScalar.string('{"key":{"customer":"cust_2"}}'))
    )
    batch = _batch(
        scope=scope,
        source_range=_scan_range(first=first, last=upper),
    )
    durable = store.upsert_destination_batch(batch)

    advanced = advance_progress_after_sync(
        store=store,
        sync=sync,
        surface=_surface(),
        reconciled=_current_snapshot_reconciled(
            identities=(
                '{"key":{"customer":"cust_1"}}',
                '{"key":{"customer":"cust_2"}}',
            )
        ),
        evidence=_submission(),
        destination_batches=(durable,),
    )

    assert advanced.progress.after == upper
    assert advanced.progress.advanced is True
    assert store.get_destination_progress(scope).position == upper
