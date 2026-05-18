from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.stores.contracts import (
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    DestinationScanRange,
    OrderedWorkInput,
    StateOrderedWorkScanPosition,
    WorkFamily,
    destination_batch_id,
)
from tests.runtime.ordered_work_helpers import append_ordered_work


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")


def _scope(
    *,
    sync_name: str = "sync_a",
    destination_name: str = "dest_a",
    surface: str = "profile",
    family: WorkFamily = "state",
    declaration_name: str = "customer_state",
) -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name=sync_name,
        destination_name=destination_name,
        surface=surface,
        family=family,
        declaration_name=declaration_name,
    )


def _work(
    *,
    collect_id: str,
    key: str,
    family: WorkFamily = "state",
    declaration_name: str = "customer_state",
) -> OrderedWorkInput:
    return OrderedWorkInput(
        collect_id=collect_id,
        family=family,
        kind="upsert" if family == "state" else "import",
        declaration_name=declaration_name,
        key={"customer": key} if family == "state" else {"purchase": key},
        identifiers=({"type": "email", "value": f"{key}@example.com"},),
        payload={"plan": "pro"} if family == "state" else {"amount": 100},
    )


def _append_sequences(
    store: DuckDBRuntimeStore,
    keys: list[str],
    *,
    family: WorkFamily = "state",
    declaration_name: str = "customer_state",
) -> list[str]:
    sequences = [
        f"00000000-{index:04x}-7000-8000-000000000000" for index in range(1, len(keys) + 1)
    ]
    append_ordered_work(
        store,
        [
            _work(
                collect_id=sequence,
                key=key,
                family=family,
                declaration_name=declaration_name,
            )
            for sequence, key in zip(sequences, keys, strict=True)
        ],
    )
    return sequences


def _pending_keys(store: DuckDBRuntimeStore, scope: DestinationProgressScope) -> list[str]:
    page = store.read_pending_work(scope=scope, max_rows=100)
    key_column = "customer" if scope.family == "state" else "purchase"
    return [str(value[key_column]) for value in _json_column_values(page.payload, "key_json")]


def _column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return batch.column(column).to_pylist()


def _json_column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return [json.loads(value) if value else None for value in _column_values(batch, column)]


def test_cleanup_does_not_delete_when_no_relevant_consumers_are_registered(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _append_sequences(store, ["cust_1"])

    evidence = store.cleanup_ordered_work(
        family="state",
        declaration_name="customer_state",
    )

    assert evidence.safe_through_collect_id is None
    assert evidence.deleted_ordered_work_count == 0
    assert evidence.retained_pending_count == 1
    assert _pending_keys(store, _scope()) == ["cust_1"]


def test_cleanup_uses_scan_positions_and_unresolved_batch_blockers(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequences = _append_sequences(store, ["cust_1", "cust_2", "cust_3"])
    scope = _scope()
    store.update_destination_progress(
        scope=scope,
        position=StateOrderedWorkScanPosition(
            collect_id=sequences[2],
            sequence_order=0,
        ),
    )
    unresolved = _destination_batch(
        scope=scope,
        first=StateOrderedWorkScanPosition(collect_id=sequences[1], sequence_order=0),
        last=StateOrderedWorkScanPosition(collect_id=sequences[1], sequence_order=0),
        status="failed",
        retry_eligible=True,
    )
    store.upsert_destination_batch(unresolved)

    evidence = store.cleanup_ordered_work(
        family="state",
        declaration_name="customer_state",
    )

    assert evidence.safe_through_collect_id == sequences[0]
    assert evidence.deleted_ordered_work_count == 1
    assert evidence.retained_pending_count == 2
    assert _pending_keys(store, _scope(destination_name="observer")) == ["cust_2", "cust_3"]


def test_cleanup_ordered_work_operation_reports_requested_and_safe_boundary_dry_run(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequences = _append_sequences(store, ["cust_1", "cust_2", "cust_3"])
    scope = _scope()
    store.update_destination_progress(
        scope=scope,
        position=StateOrderedWorkScanPosition(
            collect_id=sequences[2],
            sequence_order=0,
        ),
    )
    store.upsert_destination_batch(
        _destination_batch(
            scope=scope,
            first=StateOrderedWorkScanPosition(collect_id=sequences[1], sequence_order=0),
            last=StateOrderedWorkScanPosition(collect_id=sequences[1], sequence_order=0),
            status="failed",
            retry_eligible=True,
        )
    )

    result = retl.runner(name="ops", runtime_store=store).operations.cleanup_ordered_work(
        family="state",
        declaration_name="customer_state",
        through_collect_id=sequences[2],
        dry_run=True,
    )

    assert result["requested_through_collect_id"] == sequences[2]
    assert result["safe_through_collect_id"] == sequences[0]
    assert result["deleted_rows"]["ordered_work"] == 1
    assert result["dry_run"] is True
    assert _pending_keys(store, _scope(destination_name="observer")) == [
        "cust_1",
        "cust_2",
        "cust_3",
    ]


def test_cleanup_ordered_work_age_boundary_skips_mixed_age_collect(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    first_collect = "00000000-0001-7000-8000-000000000000"
    mixed_collect = "00000000-0002-7000-8000-000000000000"
    append_ordered_work(
        store,
        [
            _work(collect_id=first_collect, key="cust_1"),
            _work(collect_id=mixed_collect, key="cust_2"),
            _work(collect_id=mixed_collect, key="cust_3"),
        ],
    )
    store._connection.execute(
        """
        update retl.ordered_work
        set created_at = timestamp '2000-01-01'
        where collect_id = ?
           or (collect_id = ? and sequence_order = 0)
        """,
        (first_collect, mixed_collect),
    )
    store._connection.execute(
        """
        update retl.ordered_work
        set created_at = timestamp '2999-01-01'
        where collect_id = ? and sequence_order = 1
        """,
        (mixed_collect,),
    )
    store.update_destination_progress(
        scope=_scope(),
        position=StateOrderedWorkScanPosition(
            collect_id=mixed_collect,
            sequence_order=1,
        ),
    )

    result = retl.runner(name="ops", runtime_store=store).operations.cleanup_ordered_work(
        family="state",
        declaration_name="customer_state",
        older_than_seconds=1,
    )

    assert result["age_boundary_collect_id"] == first_collect
    assert result["requested_through_collect_id"] == first_collect
    assert result["safe_through_collect_id"] == first_collect
    assert result["deleted_rows"]["ordered_work"] == 1
    assert _pending_keys(store, _scope(destination_name="observer")) == ["cust_2", "cust_3"]


def test_cleanup_does_not_delete_later_rows_in_partly_scanned_collect(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    first_collect = "00000000-0001-7000-8000-000000000000"
    partial_collect = "00000000-0002-7000-8000-000000000000"
    append_ordered_work(
        store,
        [
            _work(collect_id=first_collect, key="cust_1"),
            _work(collect_id=partial_collect, key="cust_2"),
            _work(collect_id=partial_collect, key="cust_3"),
            _work(collect_id=partial_collect, key="cust_4"),
        ],
    )
    store.update_destination_progress(
        scope=_scope(),
        position=StateOrderedWorkScanPosition(
            collect_id=partial_collect,
            sequence_order=1,
        ),
    )

    evidence = store.cleanup_ordered_work(
        family="state",
        declaration_name="customer_state",
    )

    assert evidence.safe_through_collect_id == first_collect
    assert evidence.deleted_ordered_work_count == 1
    assert evidence.retained_pending_count == 3
    assert _pending_keys(store, _scope(destination_name="observer")) == [
        "cust_2",
        "cust_3",
        "cust_4",
    ]


def test_cleanup_deletes_through_min_destination_scan_position_when_unblocked(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequences = _append_sequences(store, ["cust_1", "cust_2", "cust_3"])
    crm = _scope(destination_name="crm")
    ads = _scope(destination_name="ads")
    store.update_destination_progress(
        scope=crm,
        position=StateOrderedWorkScanPosition(
            collect_id=sequences[2],
            sequence_order=0,
        ),
    )
    store.update_destination_progress(
        scope=ads,
        position=StateOrderedWorkScanPosition(
            collect_id=sequences[1],
            sequence_order=0,
        ),
    )

    evidence = store.cleanup_ordered_work(
        family="state",
        declaration_name="customer_state",
    )

    assert evidence.safe_through_collect_id == sequences[1]
    assert evidence.deleted_ordered_work_count == 2
    assert evidence.retained_pending_count == 1
    assert _pending_keys(store, _scope(destination_name="observer")) == ["cust_3"]


def _destination_batch(
    *,
    scope: DestinationProgressScope,
    first: StateOrderedWorkScanPosition,
    last: StateOrderedWorkScanPosition,
    status: str,
    retry_eligible: bool,
) -> DestinationBatchRecord:
    identity = DestinationBatchIdentity(
        scope=scope,
        declaration_version_id="decl:customer_state",
        source_range=DestinationScanRange(
            first_record_position=first,
            last_record_position=last,
            upper_bound_inclusive=last,
        ),
        reconcile_page_index=0,
        first_collect_id=first.collect_id,
        last_collect_id=last.collect_id,
        first_sequence_order=first.sequence_order,
        last_sequence_order=last.sequence_order,
        destination_batch_index=0,
        payload_fingerprint=f"payload:{first.collect_id}",
        target_request_fingerprint=f"request:{first.collect_id}",
    )
    return DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        status=status,  # type: ignore[arg-type]
        retry_eligible=retry_eligible,
    )
