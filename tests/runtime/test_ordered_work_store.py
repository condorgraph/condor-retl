from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from retl.backends.duckdb import DuckDBRuntimeStore
from retl.collect_identity import is_uuidv7, uuidv7_from_unix_ms
from retl.errors import DeclarationValidationError
from retl.stores.contracts import (
    DestinationProgressScope,
    OrderedWorkInput,
    OrderedWorkStore,
    PendingWorkCursor,
    StateOrderedWorkScanPosition,
    WorkKind,
)
from tests.runtime.ordered_work_helpers import append_ordered_work


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")


def _scope(
    *,
    sync_name: str = "sync_a",
    destination_name: str = "dest_a",
    surface: str = "profile",
    declaration_name: str = "customer_state",
) -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name=sync_name,
        destination_name=destination_name,
        surface=surface,
        family="state",
        declaration_name=declaration_name,
    )


def _state_work(
    *,
    collect_id: str,
    customer_id: str,
    kind: WorkKind = "upsert",
    plan: str = "pro",
    declaration_name: str = "customer_state",
) -> OrderedWorkInput:
    return OrderedWorkInput(
        collect_id=collect_id,
        family="state",
        kind=kind,
        declaration_name=declaration_name,
        key={"customer": customer_id},
        identifiers=({"type": "email", "value": f"{customer_id}@example.com"},),
        payload={"plan": plan},
    )


def _pending_customer_ids(
    store: OrderedWorkStore,
    scope: DestinationProgressScope,
    *,
    max_rows: int = 100,
) -> list[str]:
    page = store.read_pending_work(scope=scope, max_rows=max_rows)
    return [str(key["customer"]) for key in _json_column_values(page.payload, "key_json")]


def _column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return batch.column(column).to_pylist()


def _json_column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return [json.loads(value) if value else None for value in _column_values(batch, column)]


def _collect_id(index: int) -> str:
    return f"00000000-{index:04x}-7000-8000-000000000000"


def test_collect_id_allocation_returns_uuidv7_without_runtime_state_reads(tmp_path: Path) -> None:
    store = _store(tmp_path)

    first = store.allocate_collect_id()
    second = store.allocate_collect_id()
    third = store.allocate_collect_id()

    assert all(is_uuidv7(value) for value in (first, second, third))
    assert len({first, second, third}) == 3
    assert [first, second, third] == sorted((first, second, third))


def test_uuidv7_collect_id_sorts_lexically_by_timestamp() -> None:
    first = uuidv7_from_unix_ms(1)
    second = uuidv7_from_unix_ms(2)
    third = uuidv7_from_unix_ms(3)

    assert sorted((third, first, second)) == [first, second, third]


def test_ordered_work_rows_are_stored_durably(tmp_path: Path) -> None:
    database = tmp_path / "runtime.duckdb"
    store = DuckDBRuntimeStore(database=database)
    sequence = store.allocate_collect_id()
    stored = append_ordered_work(store, [_state_work(collect_id=sequence, customer_id="cust_1")])
    store.close()

    reopened = DuckDBRuntimeStore(database=database)
    page = reopened.read_pending_work(scope=_scope(), max_rows=10)

    assert isinstance(page.payload, pa.RecordBatch)
    assert page.payload.num_rows == page.row_count == 1
    assert _column_values(page.payload, "work_id") == [stored[0].work_id]
    assert _column_values(page.payload, "collect_id") == [sequence]
    assert _json_column_values(page.payload, "payload_json") == [{"plan": "pro"}]


def test_pending_work_is_returned_in_collect_id_order(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence_1 = _collect_id(1)
    sequence_2 = _collect_id(2)
    sequence_3 = _collect_id(3)
    append_ordered_work(
        store,
        [
            _state_work(collect_id=sequence_2, customer_id="cust_2"),
            _state_work(collect_id=sequence_1, customer_id="cust_1"),
            _state_work(collect_id=sequence_3, customer_id="cust_3"),
        ],
    )

    page = store.read_pending_work(scope=_scope(), max_rows=2)

    assert _column_values(page.payload, "collect_id") == [sequence_1, sequence_2]
    assert [key["customer"] for key in _json_column_values(page.payload, "key_json")] == [
        "cust_1",
        "cust_2",
    ]


def test_pending_work_page_uses_cursor_for_large_collect_id(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence_1 = _collect_id(1)
    sequence_2 = _collect_id(2)
    append_ordered_work(
        store,
        [
            _state_work(collect_id=sequence_1, customer_id="cust_1"),
            _state_work(collect_id=sequence_1, customer_id="cust_2"),
            _state_work(collect_id=sequence_1, customer_id="cust_3"),
            _state_work(collect_id=sequence_2, customer_id="cust_4"),
        ],
    )

    first_page = store.read_pending_work(scope=_scope(), max_rows=2)
    next_cursor = cast(PendingWorkCursor, first_page.next_cursor)
    second_page = store.read_pending_work(
        scope=_scope(),
        max_rows=2,
        cursor=next_cursor,
    )

    assert [
        str(key["customer"]) for key in _json_column_values(first_page.payload, "key_json")
    ] == [
        "cust_1",
        "cust_2",
    ]
    assert first_page.next_cursor is not None
    assert next_cursor.collect_id == sequence_1
    assert next_cursor.sequence_order == 1
    assert [
        str(key["customer"]) for key in _json_column_values(second_page.payload, "key_json")
    ] == ["cust_3"]
    assert second_page.next_cursor is None


def test_pending_work_resumes_inside_committed_state_ordered_work_position(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequence_1 = _collect_id(1)
    sequence_2 = _collect_id(2)
    append_ordered_work(
        store,
        [
            _state_work(collect_id=sequence_1, customer_id="cust_1"),
            _state_work(collect_id=sequence_1, customer_id="cust_2"),
            _state_work(collect_id=sequence_1, customer_id="cust_3"),
            _state_work(collect_id=sequence_2, customer_id="cust_4"),
        ],
    )
    store.update_destination_progress(
        scope=_scope(),
        position=StateOrderedWorkScanPosition(
            collect_id=sequence_1,
            sequence_order=1,
        ),
    )

    page = store.read_pending_work(scope=_scope(), max_rows=10)

    assert [str(key["customer"]) for key in _json_column_values(page.payload, "key_json")] == [
        "cust_3",
        "cust_4",
    ]
    assert _column_values(page.payload, "collect_id") == [sequence_1, sequence_2]
    assert _column_values(page.payload, "sequence_order") == [2, 0]


def test_pending_work_cursor_must_be_store_issued(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    append_ordered_work(
        store,
        [
            _state_work(collect_id=sequence, customer_id="cust_1"),
            _state_work(collect_id=sequence, customer_id="cust_2"),
        ],
    )

    with pytest.raises(DeclarationValidationError):
        store.read_pending_work(
            scope=_scope(),
            max_rows=1,
            cursor=PendingWorkCursor(
                token="fabricated",
                collect_id=sequence,
                sequence_order=0,
            ),
        )


def test_append_ordered_work_is_atomic_when_later_row_is_invalid(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    valid = _state_work(collect_id=sequence, customer_id="cust_1")
    invalid = OrderedWorkInput(
        collect_id=sequence,
        family="state",
        kind="upsert",
        declaration_name="customer_state",
        key={"customer": "cust_2"},
        payload={"unserializable": object()},
    )

    with pytest.raises(DeclarationValidationError):
        append_ordered_work(store, [valid, invalid])

    assert store.read_pending_work(scope=_scope(), max_rows=10).row_count == 0


def test_append_ordered_work_helper_rejects_event_rows(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()

    with pytest.raises(DeclarationValidationError, match="State-only.*Event runtime"):
        append_ordered_work(
            store,
            [
                OrderedWorkInput(
                    collect_id=sequence,
                    family="event",
                    kind="import",
                    declaration_name="purchase_event",
                )
            ],
        )


def test_core_contract_is_backend_neutral_from_callers_perspective(tmp_path: Path) -> None:
    duckdb_store = _store(tmp_path)
    sequence = duckdb_store.allocate_collect_id()
    append_ordered_work(
        duckdb_store,
        [_state_work(collect_id=sequence, customer_id="cust_1")],
    )
    store: OrderedWorkStore = duckdb_store

    assert _pending_customer_ids(store, _scope()) == ["cust_1"]


def test_active_runtime_code_does_not_reintroduce_removed_state_vocabulary() -> None:
    removed_names = (
        "state_strategy",
        "send_snapshot",
        "delete_policy",
        "StateBasisStore",
        "collect_state_snapshot",
        "commit_collect_as_basis",
    )
    active_python = Path("src/retl").rglob("*.py")
    offenders = {
        str(path): [name for name in removed_names if name in path.read_text(encoding="utf-8")]
        for path in active_python
    }
    offenders = {path: names for path, names in offenders.items() if names}

    assert offenders == {}
