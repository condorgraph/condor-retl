from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from typing import cast

import pyarrow as pa
import pytest

from retl.declarations import JSONValue
from retl.destinations.request_batch import (
    DestinationWorkRecord,
    RequestBatchContext,
    RequestBatchingPolicy,
    plan_request_batches,
)
from retl.stores.contracts import (
    CanonicalKeyScalar,
    EventKeysetScanPosition,
    StateOrderedWorkScanPosition,
)


def _operation_page(start: int, count: int) -> pa.RecordBatch:
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"customer-{index}",
            "state_key": {"customer_id": str(index)},
            "identifiers": [{"type": "email", "value": f"user{index}@example.test"}],
            "payload": {"status": "active"},
        }
        for index in range(start, start + count)
    ]
    return pa.Table.from_pylist(rows).to_batches()[0]


def _coordinated_operation_page(start: int, count: int) -> pa.RecordBatch:
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"customer-{index}",
            "collect_id": "00000000-0007-7000-8000-000000000000",
            "sequence_order": index,
            "state_key": {"customer_id": str(index)},
            "identifiers": [{"type": "email", "value": f"user{index}@example.test"}],
            "payload": {"status": "active"},
        }
        for index in range(start, start + count)
    ]
    return pa.Table.from_pylist(rows).to_batches()[0]


def _payload_page(rows: Sequence[Mapping[str, object]]) -> pa.RecordBatch:
    return pa.Table.from_pylist(rows).to_batches()[0]


def _sized_body(context: RequestBatchContext) -> JSONValue:
    return {"records": [record.to_json() for record in context.records]}


def _request_body_size(records: Sequence[DestinationWorkRecord]) -> int:
    return len(
        json.dumps(
            _plain_json(
                _sized_body(
                    RequestBatchContext(
                        sync_name="customer_sync",
                        surface_name="state_records",
                        family="state_operations",
                        index=0,
                        operation="upsert",
                        records=tuple(records),
                    )
                )
            ),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str):
        return [_plain_json(item) for item in value]
    return value


def test_destination_payload_batch_size_is_independent_from_operation_page_size() -> None:
    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_operation_page(0, 3), _operation_page(3, 2)),
        request_template={"method": "POST", "path": "/state-records"},
        batching_policy=RequestBatchingPolicy(max_rows=2),
        body_hook=lambda context: {"records": [record.to_json() for record in context.records]},
    )

    assert plan.record_count == 5
    assert [request.row_count for request in plan.plans] == [2, 2, 1]
    assert [request.record_identities for request in plan.plans] == [
        ("customer-0", "customer-1"),
        ("customer-2", "customer-3"),
        ("customer-4",),
    ]
    assert [request.request_item_count for request in plan.plans] == [2, 2, 1]
    assert [request.request_item_counts for request in plan.plans] == [(1, 1), (1, 1), (1,)]


def test_request_item_count_hook_batches_by_cumulative_count() -> None:
    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_operation_page(0, 4),),
        request_template={"method": "POST", "path": "/state-records"},
        batching_policy=RequestBatchingPolicy(max_rows=5),
        request_item_counts=lambda page: pa.array([1, 2, 3, 4], type=pa.int64()),
    )

    assert [request.record_identities for request in plan.plans] == [
        ("customer-0", "customer-1"),
        ("customer-2",),
        ("customer-3",),
    ]
    assert [request.row_count for request in plan.plans] == [2, 1, 1]
    assert [request.request_item_count for request in plan.plans] == [3, 3, 4]


@pytest.mark.parametrize(
    ("counts", "error", "match"),
    [
        (pa.array([1], type=pa.int64()), ValueError, "one count per work row"),
        (pa.array([1, None], type=pa.int64()), ValueError, "must not return null"),
        (pa.array([1.0, 1.0], type=pa.float64()), TypeError, "integer Arrow array"),
        (pa.array([1, -1], type=pa.int64()), ValueError, "non-negative"),
        (
            pa.array([1, 2**63], type=pa.uint64()),
            OverflowError,
            "signed 64-bit integer",
        ),
    ],
)
def test_request_item_count_hook_rejects_invalid_arrays(
    counts: pa.Array,
    error: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error, match=match):
        plan_request_batches(
            sync_name="customer_sync",
            surface_name="state_records",
            work=(_operation_page(0, 2),),
            request_template={"method": "POST", "path": "/state-records"},
            request_item_counts=lambda page: counts,
        )


def test_request_item_count_hook_rejects_non_arrow_array() -> None:
    with pytest.raises(TypeError, match="pyarrow.Array"):
        plan_request_batches(
            sync_name="customer_sync",
            surface_name="state_records",
            work=(_operation_page(0, 2),),
            request_template={"method": "POST", "path": "/state-records"},
            request_item_counts=lambda page: cast(pa.Array, [1, 1]),
        )


def test_request_item_count_hook_preserves_partition_boundaries() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": "audience_a-0",
            "target": "audience_a",
            "state_key": {"customer_id": "a-0"},
            "identifiers": [],
            "payload": {},
        },
        {
            "operation": "upsert",
            "record_identity": "audience_a-1",
            "target": "audience_a",
            "state_key": {"customer_id": "a-1"},
            "identifiers": [],
            "payload": {},
        },
        {
            "operation": "upsert",
            "record_identity": "audience_a-2",
            "target": "audience_a",
            "state_key": {"customer_id": "a-2"},
            "identifiers": [],
            "payload": {},
        },
        {
            "operation": "upsert",
            "record_identity": "audience_b-0",
            "target": "audience_b",
            "state_key": {"customer_id": "b-0"},
            "identifiers": [],
            "payload": {},
        },
    ]

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_payload_page(rows),),
        request_template={"method": "POST", "path": "/{{ target }}/state-records"},
        batching_policy=RequestBatchingPolicy(max_rows=4),
        request_item_counts=lambda page: pa.array([2, 2, 2, 1], type=pa.int64()),
        partition_key=lambda record: record.target,
    )

    assert [request.record_identities for request in plan.plans] == [
        ("audience_a-0", "audience_a-1"),
        ("audience_a-2",),
        ("audience_b-0",),
    ]
    assert [request.request_item_count for request in plan.plans] == [4, 2, 1]
    assert [request.request.path for request in plan.plans] == [
        "/audience_a/state-records",
        "/audience_a/state-records",
        "/audience_b/state-records",
    ]


def test_request_item_count_hook_preserves_byte_limit_splitting() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"customer-{index}",
            "state_key": {"customer_id": str(index)},
            "identifiers": [],
            "payload": {"blob": "x" * 32},
        }
        for index in range(3)
    ]
    expected_records = [
        DestinationWorkRecord(
            operation="upsert",
            record_identity=f"customer-{index}",
            key={"customer_id": str(index)},
            identifiers=[],
            payload={"blob": "x" * 32},
        )
        for index in range(3)
    ]

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_payload_page(rows),),
        request_template={"method": "POST", "path": "/state-records"},
        batching_policy=RequestBatchingPolicy(
            max_rows=10,
            max_bytes=_request_body_size(expected_records[:2]),
        ),
        body_hook=_sized_body,
        request_item_counts=lambda page: pa.array([2, 2, 2], type=pa.int64()),
    )

    assert [request.row_count for request in plan.plans] == [2, 1]
    assert [request.request_item_count for request in plan.plans] == [4, 2]


def test_request_item_count_hook_rejects_single_record_over_limit() -> None:
    with pytest.raises(ValueError, match="exceeds request batching `max_rows`"):
        plan_request_batches(
            sync_name="customer_sync",
            surface_name="state_records",
            work=(_operation_page(0, 1),),
            request_template={"method": "POST", "path": "/state-records"},
            batching_policy=RequestBatchingPolicy(max_rows=2),
            request_item_counts=lambda page: pa.array([3], type=pa.int64()),
        )


def test_request_plans_carry_ledger_coordinates_and_target_request_fingerprint() -> None:
    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_coordinated_operation_page(0, 3),),
        request_template={
            "method": "POST",
            "path": "/state-records",
            "query": {"mode": "{{ operation }}"},
            "headers": {"X-Sync": "{{ sync }}"},
        },
        batching_policy=RequestBatchingPolicy(max_rows=2),
    )

    first, second = plan.plans
    assert first.first_collect_id == "00000000-0007-7000-8000-000000000000"
    assert first.last_collect_id == "00000000-0007-7000-8000-000000000000"
    assert first.first_sequence_order == 0
    assert first.last_sequence_order == 1
    assert second.first_sequence_order == 2
    assert first.source_range is not None
    assert first.source_range.upper_bound_inclusive == StateOrderedWorkScanPosition(
        collect_id="00000000-0007-7000-8000-000000000000",
        sequence_order=1,
    )
    assert first.target_request_fingerprint
    assert first.target_request_fingerprint != second.target_request_fingerprint


def test_event_request_plans_carry_source_native_keyset_range() -> None:
    first_position = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-01T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("purchase_1"),
    )
    second_position = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-02T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("purchase_2"),
    )
    page = _payload_page(
        [
            {
                "record_identity": "purchase_1",
                "collect_id": "00000000-0003-7000-8000-000000000000",
                "sequence_order": 0,
                "key_json": {"purchase": "purchase_1"},
                "identifiers_json": [],
                "payload_json": {"total": 100},
                "event_cursor_value": "2026-01-01T00:00:00Z",
                "event_primary_key_value": "purchase_1",
            },
            {
                "record_identity": "purchase_2",
                "collect_id": "00000000-0003-7000-8000-000000000000",
                "sequence_order": 1,
                "key_json": {"purchase": "purchase_2"},
                "identifiers_json": [],
                "payload_json": {"total": 200},
                "event_cursor_value": "2026-01-02T00:00:00Z",
                "event_primary_key_value": "purchase_2",
            },
        ]
    )

    plan = plan_request_batches(
        sync_name="purchase_sync",
        surface_name="purchase_event",
        family="event_imports",
        work=(page,),
        request_template={"method": "POST", "path": "/events"},
        batching_policy=RequestBatchingPolicy(max_rows=10),
        event_cursor_kind="string",
        event_primary_key_kind="string",
    )

    assert plan.plans[0].source_range is not None
    assert plan.plans[0].source_range.first_record_position == first_position
    assert plan.plans[0].source_range.upper_bound_inclusive == second_position


def test_target_request_fingerprint_changes_with_destination_visible_route() -> None:
    base_row = {
        "operation": "upsert",
        "record_identity": "customer-1",
        "collect_id": "00000000-0007-7000-8000-000000000000",
        "sequence_order": 0,
        "state_key": {"customer_id": "1"},
        "identifiers": [{"type": "email", "value": "one@example.test"}],
        "payload": {"status": "active"},
    }

    audience_a = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_payload_page([{**base_row, "target": "audience_a"}]),),
        request_template={"method": "POST", "path": "/{{ target }}/state-records"},
        partition_key=lambda record: record.target,
    ).plans[0]
    audience_b = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_payload_page([{**base_row, "target": "audience_b"}]),),
        request_template={"method": "POST", "path": "/{{ target }}/state-records"},
        partition_key=lambda record: record.target,
    ).plans[0]

    assert audience_a.request.path == "/audience_a/state-records"
    assert audience_b.request.path == "/audience_b/state-records"
    assert audience_a.target_request_fingerprint
    assert audience_a.target_request_fingerprint != audience_b.target_request_fingerprint


def test_request_max_bytes_splits_oversized_payload_groups() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"customer-{index}",
            "state_key": {"customer_id": str(index)},
            "identifiers": [],
            "payload": {"blob": "x" * 32},
        }
        for index in range(3)
    ]
    page = _payload_page(rows)
    expected_records = [
        DestinationWorkRecord(
            operation="upsert",
            record_identity=f"customer-{index}",
            key={"customer_id": str(index)},
            identifiers=[],
            payload={"blob": "x" * 32},
        )
        for index in range(3)
    ]

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(page,),
        request_template={"method": "POST", "path": "/state-records"},
        batching_policy=RequestBatchingPolicy(
            max_rows=100,
            max_bytes=_request_body_size(expected_records[:2]),
        ),
        body_hook=_sized_body,
    )

    assert [request.row_count for request in plan.plans] == [2, 1]
    assert [request.record_identities for request in plan.plans] == [
        ("customer-0", "customer-1"),
        ("customer-2",),
    ]


def test_request_max_bytes_respects_partitions_and_row_limits() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"{target}-{index}",
            "target": target,
            "state_key": {"customer_id": f"{target}-{index}"},
            "identifiers": [],
            "payload": {"blob": "x" * 8},
        }
        for target in ("audience_a", "audience_b")
        for index in range(3)
    ]

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_payload_page(rows),),
        request_template={"method": "POST", "path": "/{{ target }}/state-records"},
        batching_policy=RequestBatchingPolicy(max_rows=2, max_bytes=10_000),
        body_hook=_sized_body,
        partition_key=lambda record: record.target,
    )

    assert [request.record_identities for request in plan.plans] == [
        ("audience_a-0", "audience_a-1"),
        ("audience_a-2",),
        ("audience_b-0", "audience_b-1"),
        ("audience_b-2",),
    ]
    assert [request.request.path for request in plan.plans] == [
        "/audience_a/state-records",
        "/audience_a/state-records",
        "/audience_b/state-records",
        "/audience_b/state-records",
    ]


def test_request_max_bytes_keeps_single_oversized_record_atomic() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": "customer-0",
            "state_key": {"customer_id": "0"},
            "identifiers": [],
            "payload": {"blob": "x" * 128},
        }
    ]

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_payload_page(rows),),
        request_template={"method": "POST", "path": "/state-records"},
        batching_policy=RequestBatchingPolicy(max_rows=100, max_bytes=1),
        body_hook=_sized_body,
    )

    assert [request.record_identities for request in plan.plans] == [("customer-0",)]


def test_request_max_bytes_does_not_measure_every_appended_record() -> None:
    body_calls = 0

    def body(context: RequestBatchContext) -> JSONValue:
        nonlocal body_calls
        body_calls += 1
        return _sized_body(context)

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_operation_page(0, 250),),
        request_template={"method": "POST", "path": "/state-records"},
        batching_policy=RequestBatchingPolicy(max_rows=1000, max_bytes=1_000_000),
        body_hook=body,
    )

    assert plan.record_count == 250
    assert [request.row_count for request in plan.plans] == [250]
    assert body_calls <= 5


def test_request_max_bytes_splits_after_periodic_overshoot() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"customer-{index:03d}",
            "state_key": {"customer_id": f"{index:03d}"},
            "identifiers": [],
            "payload": {"blob": "x" * 24},
        }
        for index in range(150)
    ]
    expected_records = [
        DestinationWorkRecord(
            operation="upsert",
            record_identity=f"customer-{index:03d}",
            key={"customer_id": f"{index:03d}"},
            identifiers=[],
            payload={"blob": "x" * 24},
        )
        for index in range(150)
    ]
    max_bytes = _request_body_size(expected_records[:40])

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_payload_page(rows),),
        request_template={"method": "POST", "path": "/state-records"},
        batching_policy=RequestBatchingPolicy(max_rows=1000, max_bytes=max_bytes),
        body_hook=_sized_body,
    )

    assert [request.row_count for request in plan.plans] == [40, 40, 40, 30]
    for request_plan in plan.plans:
        body = request_plan.request.json_body
        assert body is not None
        assert (
            len(json.dumps(_plain_json(body), sort_keys=True, separators=(",", ":")).encode())
            <= max_bytes
        )


def test_request_planning_iterates_arrow_pages_without_requiring_a_table() -> None:
    visited: list[int] = []

    def pages() -> Iterator[pa.RecordBatch]:
        for index in range(3):
            visited.append(index)
            yield _operation_page(index, 1)

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=pages(),
        request_template={"method": "POST", "path": "/state-records"},
        batching_policy=RequestBatchingPolicy(max_rows=2),
    )

    assert visited == [0, 1, 2]
    assert plan.request_count == 2


def test_request_planning_maps_canonical_reconcile_json_columns() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "work_id": "work-1",
                "operation": "upsert",
                "key_json": json.dumps({"customer": "cust_1"}),
                "target_json": json.dumps("audience_a"),
                "identifiers_json": json.dumps([{"type": "email", "value": "one@example.test"}]),
                "payload_json": json.dumps({"plan": "pro"}),
                "occurred_at": "2024-01-01T00:00:00Z",
            }
        ]
    ).to_batches()[0]

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(page,),
        request_template={"method": "POST", "path": "/{{ target }}/state-records"},
    )

    body = plan.plans[0].request.json_body
    assert isinstance(body, Mapping)
    records = body["records"]
    assert isinstance(records, tuple)
    record = records[0]
    assert isinstance(record, Mapping)
    assert record["record_identity"] == "work-1"
    assert record["key"] == {"customer": "cust_1"}
    assert record["target"] == "audience_a"
    assert record["identifiers"] == [{"type": "email", "value": "one@example.test"}]
    assert record["payload"] == {"plan": "pro"}
    assert record["occurred_at"] == "2024-01-01T00:00:00Z"
    assert plan.plans[0].request.path == "/audience_a/state-records"


def test_request_planning_normalizes_nested_identifier_json_strings_only() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "work_id": "work-1",
                "operation": "upsert",
                "key_json": {"customer": "cust_1"},
                "target_json": {"value": "audience_a"},
                "identifiers_json": [
                    json.dumps({"type": "email", "value": "one@example.test"}),
                    "not-json",
                ],
                "payload_json": {
                    "custom_data": json.dumps({"plan": "pro"}),
                    "label": "ordinary string",
                },
            }
        ]
    ).to_batches()[0]

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(page,),
        request_template={"method": "POST", "path": "/{{ target }}/state-records"},
    )

    body = plan.plans[0].request.json_body
    assert isinstance(body, Mapping)
    records = body["records"]
    assert isinstance(records, tuple)
    record = records[0]
    assert isinstance(record, Mapping)
    assert record["identifiers"] == [
        {"type": "email", "value": "one@example.test"},
        "not-json",
    ]
    assert record["payload"] == {
        "custom_data": json.dumps({"plan": "pro"}),
        "label": "ordinary string",
    }


def test_request_planning_unwraps_canonical_target_value_object() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "work_id": "work-1",
                "operation": "upsert",
                "key_json": json.dumps({"customer": "cust_1"}),
                "target_json": json.dumps({"value": "audience_a"}),
                "identifiers_json": json.dumps([{"type": "email", "value": "one@example.test"}]),
                "payload_json": json.dumps({"plan": "pro"}),
            }
        ]
    ).to_batches()[0]

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(page,),
        request_template={"method": "POST", "path": "/{{ target }}/state-records"},
        partition_key=lambda record: record.target,
    )

    body = plan.plans[0].request.json_body
    assert isinstance(body, Mapping)
    records = body["records"]
    assert isinstance(records, tuple)
    record = records[0]
    assert isinstance(record, Mapping)
    assert record["target"] == "audience_a"
    assert plan.plans[0].request.path == "/audience_a/state-records"


def test_request_planning_rejects_full_arrow_table_work() -> None:
    table = pa.Table.from_batches([_operation_page(0, 2)])

    with pytest.raises(TypeError, match="bounded Arrow work pages"):
        plan_request_batches(
            sync_name="customer_sync",
            surface_name="state_records",
            work=table,
            request_template={"method": "POST", "path": "/state-records"},
        )


def test_request_planning_rejects_table_provider_work() -> None:
    class TableProvider:
        table = pa.Table.from_batches([_operation_page(0, 2)])

    with pytest.raises(TypeError, match="bounded Arrow work pages"):
        plan_request_batches(
            sync_name="customer_sync",
            surface_name="state_records",
            work=TableProvider(),
            request_template={"method": "POST", "path": "/state-records"},
        )


def test_request_planning_accepts_iter_record_batches_provider() -> None:
    class PageProvider:
        def iter_record_batches(self) -> Iterator[pa.RecordBatch]:
            yield _operation_page(0, 1)
            yield _operation_page(1, 1)

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=PageProvider(),
        request_template={"method": "POST", "path": "/state-records"},
        batching_policy=RequestBatchingPolicy(max_rows=1),
    )

    assert plan.request_count == 2
    assert [request.record_identities for request in plan.plans] == [
        ("customer-0",),
        ("customer-1",),
    ]


def test_request_template_uses_bounded_batch_context_and_public_config() -> None:
    def body(context: RequestBatchContext) -> JSONValue:
        return {
            "sync": context.sync_name,
            "operation": context.operation,
            "count": context.row_count,
        }

    plan = plan_request_batches(
        sync_name="customer_sync",
        surface_name="state_records",
        work=(_operation_page(0, 1),),
        request_template={
            "method": "post",
            "path": "/{{ config.version }}/{{ surface }}/{{ operation }}",
            "headers": {"X-Sync": "{{ sync }}"},
        },
        public_config={"version": "supported-one"},
        body_hook=body,
    )

    request = plan.plans[0].request
    assert request.method == "POST"
    assert request.path == "/supported-one/state_records/upsert"
    assert request.headers == {"X-Sync": "customer_sync"}
    assert request.json_body == {"sync": "customer_sync", "operation": "upsert", "count": 1}


def test_partition_changes_force_new_bounded_payload_batch() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": "a",
            "target": "audience_1",
            "identifiers": [],
            "payload": {},
            "state_key": {"id": "a"},
        },
        {
            "operation": "remove",
            "record_identity": "b",
            "target": "audience_1",
            "identifiers": [],
            "payload": {},
            "state_key": {"id": "b"},
        },
    ]
    page = pa.Table.from_pylist(rows).to_batches()[0]

    plan = plan_request_batches(
        sync_name="audience_sync",
        surface_name="custom_audiences",
        work=(page,),
        request_template={"method": "POST", "path": "/{{ target }}/users"},
        batching_policy=RequestBatchingPolicy(max_rows=100),
        partition_key=lambda record: (record.target, record.operation),
    )

    assert [request.operation for request in plan.plans] == ["upsert", "remove"]
    assert [request.request.path for request in plan.plans] == [
        "/audience_1/users",
        "/audience_1/users",
    ]
