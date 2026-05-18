from __future__ import annotations

import pytest

from retl.stores.contracts import (
    CanonicalKey,
    CanonicalKeyScalar,
    DestinationBatchIdentity,
    DestinationProgressScope,
    DestinationScanRange,
    EventKeysetScanPosition,
    StateCurrentSnapshotScanPosition,
    StateOrderedWorkScanPosition,
    compare_scan_positions,
    destination_batch_id,
    scan_position_from_jsonable,
    scan_position_to_jsonable,
)


def _state_scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name="crm",
        surface="profile_properties",
        family="state",
        declaration_name="customer_state",
    )


def _event_scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="purchase_sync",
        destination_name="warehouse",
        surface="purchase_events",
        family="event",
        declaration_name="purchase_event",
    )


def test_state_ordered_work_position_serializes_as_structured_mode() -> None:
    position = StateOrderedWorkScanPosition(
        collect_id="00000000-002a-7000-8000-000000000000", sequence_order=918
    )

    encoded = scan_position_to_jsonable(position)
    decoded = scan_position_from_jsonable(encoded)

    assert encoded == {
        "collect_id": "00000000-002a-7000-8000-000000000000",
        "family": "state",
        "mode": "ordered_work",
        "sequence_order": 918,
    }
    assert decoded == position
    assert (
        compare_scan_positions(
            StateOrderedWorkScanPosition(
                collect_id="00000000-002a-7000-8000-000000000000", sequence_order=918
            ),
            StateOrderedWorkScanPosition(
                collect_id="00000000-002a-7000-8000-000000000000", sequence_order=919
            ),
        )
        < 0
    )
    assert (
        compare_scan_positions(
            StateOrderedWorkScanPosition(
                collect_id="00000000-002a-7000-8000-000000000000", sequence_order=999
            ),
            StateOrderedWorkScanPosition(
                collect_id="00000000-002b-7000-8000-000000000000", sequence_order=0
            ),
        )
        < 0
    )


def test_state_current_snapshot_position_uses_structured_canonical_key() -> None:
    first = StateCurrentSnapshotScanPosition(
        key=CanonicalKey.of(CanonicalKeyScalar.string("acct_199"))
    )
    second = StateCurrentSnapshotScanPosition(
        key=CanonicalKey.of(CanonicalKeyScalar.string("acct_200"))
    )

    encoded = scan_position_to_jsonable(first)
    decoded = scan_position_from_jsonable(encoded)

    assert encoded == {
        "family": "state",
        "key": {"parts": [{"kind": "string", "value": "acct_199"}]},
        "mode": "current_snapshot",
    }
    assert decoded == first
    assert compare_scan_positions(first, second) < 0


def test_event_position_compares_by_source_native_keyset() -> None:
    before = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-05-06T10:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("purchase_001"),
    )
    after = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-05-06T10:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("purchase_002"),
    )

    encoded = scan_position_to_jsonable(before)
    decoded = scan_position_from_jsonable(encoded)

    assert encoded == {
        "cursor_value": {"kind": "string", "value": "2026-05-06T10:00:00Z"},
        "family": "event",
        "mode": "keyset",
        "primary_key_value": {"kind": "string", "value": "purchase_001"},
    }
    assert decoded == before
    assert compare_scan_positions(before, after) < 0


def test_scan_position_comparison_rejects_family_and_mode_mixing() -> None:
    ordered = StateOrderedWorkScanPosition(
        collect_id="00000000-0007-7000-8000-000000000000", sequence_order=1
    )
    snapshot = StateCurrentSnapshotScanPosition(
        key=CanonicalKey.of(CanonicalKeyScalar.string("acct_1"))
    )
    event = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.integer(7),
        primary_key_value=CanonicalKeyScalar.string("evt_1"),
    )

    with pytest.raises(ValueError, match="modes cannot be mixed"):
        compare_scan_positions(ordered, snapshot)

    with pytest.raises(ValueError, match="families cannot be mixed"):
        compare_scan_positions(ordered, event)


def test_state_scan_position_rejects_packed_integer_shortcuts() -> None:
    with pytest.raises(ValueError, match="scan position must be a mapping"):
        scan_position_from_jsonable(42)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="require collect_id and sequence_order"):
        scan_position_from_jsonable(
            {"family": "state", "mode": "ordered_work", "packed_position": 42_000_918}
        )

    with pytest.raises(ValueError, match="nonnegative integer"):
        StateOrderedWorkScanPosition(
            collect_id="00000000-002a-7000-8000-000000000000", sequence_order=-1
        )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_canonical_key_scalar_number_rejects_non_finite_values(value: float) -> None:
    with pytest.raises(ValueError, match="number canonical key scalar value must be finite"):
        CanonicalKeyScalar.number(value)


def test_destination_batch_identity_includes_source_range_evidence() -> None:
    lower = StateOrderedWorkScanPosition(
        collect_id="00000000-002a-7000-8000-000000000000", sequence_order=917
    )
    first = StateOrderedWorkScanPosition(
        collect_id="00000000-002a-7000-8000-000000000000", sequence_order=918
    )
    last = StateOrderedWorkScanPosition(
        collect_id="00000000-002a-7000-8000-000000000000", sequence_order=921
    )
    upper = StateOrderedWorkScanPosition(
        collect_id="00000000-002a-7000-8000-000000000000", sequence_order=921
    )
    identity = DestinationBatchIdentity(
        scope=_state_scope(),
        declaration_version_id="decl:supported-state-range",
        source_range=DestinationScanRange(
            lower_bound_exclusive=lower,
            first_record_position=first,
            last_record_position=last,
            upper_bound_inclusive=upper,
        ),
        reconcile_page_index=0,
        first_collect_id="00000000-002a-7000-8000-000000000000",
        last_collect_id="00000000-002a-7000-8000-000000000000",
        first_sequence_order=918,
        last_sequence_order=921,
        payload_fingerprint="payload:abc",
        target_request_fingerprint="request:abc",
    )
    changed_upper = DestinationBatchIdentity(
        scope=_state_scope(),
        declaration_version_id="decl:supported-state-range",
        source_range=DestinationScanRange(
            lower_bound_exclusive=lower,
            first_record_position=first,
            last_record_position=last,
            upper_bound_inclusive=StateOrderedWorkScanPosition(
                collect_id="00000000-002a-7000-8000-000000000000",
                sequence_order=922,
            ),
        ),
        reconcile_page_index=0,
        first_collect_id="00000000-002a-7000-8000-000000000000",
        last_collect_id="00000000-002a-7000-8000-000000000000",
        first_sequence_order=918,
        last_sequence_order=921,
        payload_fingerprint="payload:abc",
        target_request_fingerprint="request:abc",
    )

    assert identity.source_range is not None
    assert identity.source_range.family == "state"
    assert destination_batch_id(identity) != destination_batch_id(changed_upper)


def test_event_destination_batch_id_uses_source_range_not_synthetic_collect_coordinates() -> None:
    source_range = DestinationScanRange(
        lower_bound_exclusive=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-01T00:00:00Z"),
            primary_key_value=CanonicalKeyScalar.string("purchase_1"),
        ),
        first_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-02T00:00:00Z"),
            primary_key_value=CanonicalKeyScalar.string("purchase_2"),
        ),
        last_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-03T00:00:00Z"),
            primary_key_value=CanonicalKeyScalar.string("purchase_3"),
        ),
        upper_bound_inclusive=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-03T00:00:00Z"),
            primary_key_value=CanonicalKeyScalar.string("purchase_3"),
        ),
    )
    first = DestinationBatchIdentity(
        scope=_event_scope(),
        declaration_version_id="decl:event-range",
        source_range=source_range,
        reconcile_page_index=0,
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=1,
        payload_fingerprint="payload:event",
        target_request_fingerprint="request:event",
    )
    replayed = DestinationBatchIdentity(
        scope=_event_scope(),
        declaration_version_id="decl:event-range",
        source_range=source_range,
        reconcile_page_index=0,
        first_collect_id="00000000-0099-7000-8000-000000000000",
        last_collect_id="00000000-0099-7000-8000-000000000000",
        first_sequence_order=10,
        last_sequence_order=11,
        payload_fingerprint="payload:event",
        target_request_fingerprint="request:event",
    )

    assert destination_batch_id(first) == destination_batch_id(replayed)


def test_event_destination_batch_id_uses_source_range_not_content_fingerprints() -> None:
    source_range = DestinationScanRange(
        first_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-02T00:00:00Z"),
            primary_key_value=CanonicalKeyScalar.string("purchase_2"),
        ),
        last_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-03T00:00:00Z"),
            primary_key_value=CanonicalKeyScalar.string("purchase_3"),
        ),
        upper_bound_inclusive=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-03T00:00:00Z"),
            primary_key_value=CanonicalKeyScalar.string("purchase_3"),
        ),
    )
    first = DestinationBatchIdentity(
        scope=_event_scope(),
        declaration_version_id="decl:event-range-a",
        source_range=source_range,
        source_page_index=0,
        reconcile_page_index=0,
        destination_batch_index=7,
        payload_fingerprint="payload:first",
        target_request_fingerprint="request:first",
    )
    replayed = DestinationBatchIdentity(
        scope=_event_scope(),
        declaration_version_id="decl:event-range-b",
        source_range=source_range,
        source_page_index=99,
        reconcile_page_index=99,
        destination_batch_index=7,
        payload_fingerprint="payload:changed",
        target_request_fingerprint="request:changed",
    )

    assert destination_batch_id(first) == destination_batch_id(replayed)


def test_state_destination_batch_id_still_uses_collect_coordinates() -> None:
    source_range = DestinationScanRange(
        first_record_position=StateOrderedWorkScanPosition(
            collect_id="00000000-0001-7000-8000-000000000000",
            sequence_order=0,
        ),
        last_record_position=StateOrderedWorkScanPosition(
            collect_id="00000000-0001-7000-8000-000000000000",
            sequence_order=1,
        ),
        upper_bound_inclusive=StateOrderedWorkScanPosition(
            collect_id="00000000-0001-7000-8000-000000000000",
            sequence_order=1,
        ),
    )
    first = DestinationBatchIdentity(
        scope=_state_scope(),
        declaration_version_id="decl:state-range",
        source_range=source_range,
        reconcile_page_index=0,
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=1,
        payload_fingerprint="payload:state",
        target_request_fingerprint="request:state",
    )
    changed = DestinationBatchIdentity(
        scope=_state_scope(),
        declaration_version_id="decl:state-range",
        source_range=source_range,
        reconcile_page_index=0,
        first_collect_id="00000000-0002-7000-8000-000000000000",
        last_collect_id="00000000-0002-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=1,
        payload_fingerprint="payload:state",
        target_request_fingerprint="request:state",
    )

    assert destination_batch_id(first) != destination_batch_id(changed)


def test_state_destination_batch_id_still_uses_content_fingerprints() -> None:
    source_range = DestinationScanRange(
        first_record_position=StateOrderedWorkScanPosition(
            collect_id="00000000-0001-7000-8000-000000000000",
            sequence_order=0,
        ),
        last_record_position=StateOrderedWorkScanPosition(
            collect_id="00000000-0001-7000-8000-000000000000",
            sequence_order=1,
        ),
        upper_bound_inclusive=StateOrderedWorkScanPosition(
            collect_id="00000000-0001-7000-8000-000000000000",
            sequence_order=1,
        ),
    )
    first = DestinationBatchIdentity(
        scope=_state_scope(),
        declaration_version_id="decl:state-range",
        source_range=source_range,
        reconcile_page_index=0,
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=1,
        payload_fingerprint="payload:state",
        target_request_fingerprint="request:state",
    )
    changed = DestinationBatchIdentity(
        scope=_state_scope(),
        declaration_version_id="decl:state-range",
        source_range=source_range,
        reconcile_page_index=0,
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=1,
        payload_fingerprint="payload:changed",
        target_request_fingerprint="request:state",
    )

    assert destination_batch_id(first) != destination_batch_id(changed)


def test_destination_batch_identity_rejects_scope_family_mismatch() -> None:
    event_range = DestinationScanRange(
        first_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(10),
            primary_key_value=CanonicalKeyScalar.string("evt_10"),
        ),
        last_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(11),
            primary_key_value=CanonicalKeyScalar.string("evt_11"),
        ),
        upper_bound_inclusive=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(11),
            primary_key_value=CanonicalKeyScalar.string("evt_11"),
        ),
    )

    with pytest.raises(ValueError, match="source range family must match"):
        DestinationBatchIdentity(
            scope=_state_scope(),
            declaration_version_id="decl:supported-state-range",
            source_range=event_range,
            reconcile_page_index=0,
            first_collect_id="00000000-0001-7000-8000-000000000000",
            last_collect_id="00000000-0001-7000-8000-000000000000",
            first_sequence_order=0,
            last_sequence_order=1,
            payload_fingerprint="payload:event",
            target_request_fingerprint="request:event",
        )


def test_destination_scan_range_rejects_mode_mixing() -> None:
    with pytest.raises(ValueError, match="modes cannot be mixed"):
        DestinationScanRange(
            first_record_position=StateOrderedWorkScanPosition(
                collect_id="00000000-0001-7000-8000-000000000000",
                sequence_order=0,
            ),
            last_record_position=StateCurrentSnapshotScanPosition(
                key=CanonicalKey.of(CanonicalKeyScalar.string("acct_1"))
            ),
            upper_bound_inclusive=StateCurrentSnapshotScanPosition(
                key=CanonicalKey.of(CanonicalKeyScalar.string("acct_1"))
            ),
        )


def test_destination_scan_range_rejects_lower_bound_at_or_after_first_record() -> None:
    with pytest.raises(ValueError, match="lower_bound_exclusive must be before first_record"):
        DestinationScanRange(
            lower_bound_exclusive=StateOrderedWorkScanPosition(
                collect_id="00000000-0007-7000-8000-000000000000",
                sequence_order=3,
            ),
            first_record_position=StateOrderedWorkScanPosition(
                collect_id="00000000-0007-7000-8000-000000000000",
                sequence_order=3,
            ),
            last_record_position=StateOrderedWorkScanPosition(
                collect_id="00000000-0007-7000-8000-000000000000",
                sequence_order=4,
            ),
            upper_bound_inclusive=StateOrderedWorkScanPosition(
                collect_id="00000000-0007-7000-8000-000000000000",
                sequence_order=4,
            ),
        )
