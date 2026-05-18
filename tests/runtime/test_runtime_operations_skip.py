from __future__ import annotations

from pathlib import Path

import pytest

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.errors import DeclarationValidationError
from retl.operations import OrderedWorkRange
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationBatchCompletionState,
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationBatchStatus,
    DestinationProgressScope,
    DestinationScanRange,
    EventKeysetScanPosition,
    OrderedWorkInput,
    StateOrderedWorkScanPosition,
    destination_batch_id,
)
from tests.runtime.ordered_work_helpers import append_ordered_work


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")


def test_skip_ordered_work_range_creates_skipped_batch_and_advances_only_scope(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    append_ordered_work(
        store,
        [
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_1"},
            ),
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_2"},
            ),
        ],
    )
    other_scope = DestinationProgressScope(
        sync_name="other_sync",
        destination_name="crm",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )
    store.register_destination_progress(other_scope)

    result = retl.runner(name="ops", runtime_store=store).operations.skip_ordered_work_range(
        _sync(),
        OrderedWorkRange(
            first_collect_id="00000000-0001-7000-8000-000000000000",
            first_sequence_order=0,
            last_collect_id="00000000-0001-7000-8000-000000000000",
            last_sequence_order=1,
        ),
    )

    batches = store.list_destination_batches(scope=_scope(), statuses=("skipped",))
    assert result["progress_advanced"] is True
    assert len(batches) == 1
    assert batches[0].completion_state == "resolved"
    assert batches[0].status == "skipped"
    assert store.get_destination_progress(_scope()).position == StateOrderedWorkScanPosition(
        "00000000-0001-7000-8000-000000000000",
        1,
    )
    assert store.get_destination_progress(other_scope).position is None


def test_dismiss_unresolved_delegates_through_runner_operations(tmp_path: Path) -> None:
    store = _store(tmp_path)
    pending = _batch(status="pending")
    failed = _batch(status="failed", index=1)
    succeeded = _batch(status="succeeded", index=2)
    store.upsert_destination_batches((pending, failed, succeeded))

    dismissed = retl.runner(name="ops", runtime_store=store).dismiss_unresolved(_sync())

    assert {batch.batch_id for batch in dismissed} == {pending.batch_id, failed.batch_id}
    assert {batch.status for batch in dismissed} == {"skipped"}
    persisted = store.get_destination_batch(batch_id=succeeded.batch_id)
    assert persisted is not None
    assert persisted.status == "succeeded"


def test_backwards_skip_attempt_does_not_leave_skipped_batch(tmp_path: Path) -> None:
    store = _store(tmp_path)
    append_ordered_work(
        store,
        [
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_1"},
            ),
            OrderedWorkInput(
                collect_id="00000000-0002-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_2"},
            ),
        ],
    )
    store.update_destination_progress(
        scope=_scope(),
        position=StateOrderedWorkScanPosition(
            collect_id="00000000-0002-7000-8000-000000000000", sequence_order=0
        ),
    )

    with pytest.raises(DeclarationValidationError, match="cannot move behind"):
        retl.runner(name="ops", runtime_store=store).operations.skip_ordered_work_range(
            _sync(),
            OrderedWorkRange(
                first_collect_id="00000000-0001-7000-8000-000000000000",
                first_sequence_order=0,
                last_collect_id="00000000-0001-7000-8000-000000000000",
                last_sequence_order=0,
            ),
        )

    assert store.list_destination_batches(scope=_scope(), statuses=("skipped",)) == ()


def test_multi_collect_skip_batch_uses_ordered_work_edge_rows(tmp_path: Path) -> None:
    store = _store(tmp_path)
    append_ordered_work(
        store,
        [
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_0"},
            ),
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_1"},
            ),
            OrderedWorkInput(
                collect_id="00000000-0002-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
                key={"customer": "cust_2"},
            ),
        ],
    )

    retl.runner(name="ops", runtime_store=store).operations.skip_ordered_work_range(
        _sync(),
        OrderedWorkRange(
            first_collect_id="00000000-0001-7000-8000-000000000000",
            first_sequence_order=1,
            last_collect_id="00000000-0002-7000-8000-000000000000",
            last_sequence_order=0,
        ),
    )

    batch = store.list_destination_batches(scope=_scope(), statuses=("skipped",))[0]
    assert batch.identity.first_collect_id == "00000000-0001-7000-8000-000000000000"
    assert batch.identity.first_sequence_order == 1
    assert batch.identity.last_collect_id == "00000000-0002-7000-8000-000000000000"
    assert batch.identity.last_sequence_order == 0


def test_skip_event_keyset_range_creates_skipped_range_without_ordered_work(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    first = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-01T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("purchase_1"),
    )
    upper = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-02T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("purchase_2"),
    )
    scan_range = DestinationScanRange(
        first_record_position=first,
        last_record_position=upper,
        upper_bound_inclusive=upper,
    )

    result = retl.runner(name="ops", runtime_store=store).operations.skip_event_keyset_range(
        _event_sync(),
        scan_range,
    )

    batches = store.list_destination_batches(scope=_event_scope(), statuses=("skipped",))
    assert result["progress_advanced"] is True
    assert len(batches) == 1
    assert batches[0].identity.source_range == scan_range
    assert store.get_destination_progress(_event_scope()).position == upper
    assert store.inspect_runtime_store()["tables"]["ordered_work"] == 0


def test_skip_ordered_work_range_rejects_event_keyset_range(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-01T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("purchase_1"),
    )
    upper = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-02T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("purchase_2"),
    )
    scan_range = DestinationScanRange(
        first_record_position=first,
        last_record_position=upper,
        upper_bound_inclusive=upper,
    )

    with pytest.raises(
        DeclarationValidationError,
        match="skip_ordered_work_range.*State-only.*skip_event_keyset_range",
    ):
        retl.runner(name="ops", runtime_store=store).operations.skip_ordered_work_range(
            _event_sync(),
            scan_range,
        )


def _sync() -> retl.Sync:
    state = retl.state(
        name="customer_state",
        source=retl.source(name="customers", query="select * from customers"),
        key={"customer": "customer_id"},
        identifiers=[],
        payload={},
    )
    return retl.sync(
        name="customer_sync",
        declaration=state,
        destination=retl.DestinationBinding(binding_name="crm", destination_ref="retl/mock"),
        surface="profile",
    )


def _event_sync() -> retl.Sync:
    event = retl.event(
        name="purchase",
        source=retl.source(
            name="purchases",
            mode="checkpointed",
            query="select * from purchases",
            checkpoint={
                "cursor": "purchased_at",
                "primary_key": "purchase_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
        ),
        key={"purchase": "purchase_id"},
        occurred_at="purchased_at",
        identifiers=[],
        payload={},
    )
    return retl.sync(
        name="purchase_sync",
        declaration=event,
        destination=retl.DestinationBinding(binding_name="crm", destination_ref="retl/mock"),
        surface="purchase_event",
    )


def _scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name="crm",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )


def _event_scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="purchase_sync",
        destination_name="crm",
        surface="purchase_event",
        family="event",
        declaration_name="purchase",
    )


def _batch(
    *,
    status: DestinationBatchStatus,
    index: int = 0,
) -> DestinationBatchRecord:
    identity = DestinationBatchIdentity(
        scope=_scope(),
        declaration_version_id="declaration-version-one",
        source_page_index=index,
        reconcile_page_index=index,
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=index,
        last_sequence_order=index,
        destination_batch_index=index,
        payload_fingerprint=f"payload:{index}",
        target_request_fingerprint=f"request:{index}",
    )
    completion: DestinationBatchCompletionState = (
        "resolved" if status in {"accepted", "succeeded", "skipped"} else "unresolved"
    )
    return DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        status=status,
        completion_state=completion,
        retry_eligible=status == "failed",
    )
