from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pytest

import retl
from retl.backends.duckdb import DuckDBRuntimeStore, DuckDBSqlBackend
from retl.errors import DeclarationValidationError
from retl.events.staging import stage_event_declaration
from retl.runtime.progress import destination_progress_scope
from retl.runtime.staging import stage_sync_pending_work, stage_sync_resend_all_state
from retl.state_runtime.producer import produce_state_collect
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationProgress,
    DestinationProgressScope,
    DestinationProgressUpdate,
    EventKeysetScanPosition,
    EventSourceCursor,
    OrderedWorkInput,
    OrderedWorkRetentionCleanup,
    PendingWorkCursor,
    PendingWorkPage,
    ScanPosition,
    StateCurrentCursor,
    StateCurrentPage,
    StateCurrentSnapshotScanPosition,
    StateCurrentSummary,
    StateOrderedWorkScanPosition,
    StateProductionResult,
    StateSnapshotHandle,
    WorkFamily,
    WorkKind,
)
from tests.runtime.ordered_work_helpers import append_ordered_work


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return _backend(tmp_path).runtime_store()


def _backend(tmp_path: Path) -> DuckDBSqlBackend:
    return DuckDBSqlBackend(
        database=tmp_path / "warehouse.duckdb",
        source_schema="main",
        runtime_schema="retl",
    )


def _destination(name: str = "test_destination") -> retl.DestinationBinding:
    return retl.DestinationBinding(binding_name=name, destination_ref="retl/mock")


def _state(source: retl.Source | None = None, name: str = "customer_state") -> retl.State:
    return retl.state(
        name=name,
        source=source
        or retl.source(
            name="customers",
            query="select customer_id, email, plan, audience_key from customers",
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _event(source: retl.Source | None = None, name: str = "purchase_event") -> retl.Event:
    return retl.event(
        name=name,
        source=source
        or retl.source(
            name="purchases",
            query="select purchase_id, email, occurred_at, amount from purchases",
            mode="checkpointed",
            checkpoint={
                "cursor": "occurred_at",
                "primary_key": "purchase_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
        ),
        key={"purchase": "purchase_id"},
        occurred_at="occurred_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"amount": "amount"},
    )


def _sync(
    *,
    declaration: retl.State | retl.Event | None = None,
    name: str = "customer_sync",
    destination: str = "test_destination",
    surface: str = "profile",
) -> retl.Sync:
    return retl.sync(
        name=name,
        declaration=declaration or _state(),
        destination=_destination(destination),
        surface=surface,
    )


def _work(
    *,
    collect_id: str,
    key: str,
    family: WorkFamily = "state",
    kind: WorkKind = "upsert",
    declaration_name: str = "customer_state",
) -> OrderedWorkInput:
    return OrderedWorkInput(
        collect_id=collect_id,
        family=family,
        kind=kind,
        declaration_name=declaration_name,
        key={"id": key} if family == "state" else {"purchase": key},
        identifiers=({"type": "email", "value": f"{key}@example.com"},),
        payload={"plan": "pro"} if family == "state" else {"amount": 100},
    )


def _column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return batch.column(column).to_pylist()


def _json_column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return [json.loads(value) if value else None for value in _column_values(batch, column)]


def _collect_id(index: int) -> str:
    return f"00000000-{index:04x}-7000-8000-000000000000"


def _record_batch(*, row_count: int = 1, omit: str | None = None) -> pa.RecordBatch:
    values: dict[str, list[Any]] = {
        "work_id": [f"work_{index}" for index in range(row_count)],
        "collect_id": ["00000000-0001-7000-8000-000000000000" for _ in range(row_count)],
        "sequence_order": list(range(row_count)),
        "family": ["state" for _ in range(row_count)],
        "kind": ["upsert" for _ in range(row_count)],
        "declaration_name": ["customer_state" for _ in range(row_count)],
        "key_json": [json.dumps({"id": f"cust_{index}"}) for index in range(row_count)],
        "target_json": [None for _ in range(row_count)],
        "identifiers_json": [json.dumps([]) for _ in range(row_count)],
        "payload_json": [json.dumps({"plan": "pro"}) for _ in range(row_count)],
    }
    if omit is not None:
        del values[omit]
    return pa.RecordBatch.from_pydict(values)


class _PendingPageStore:
    def __init__(
        self,
        page: PendingWorkPage,
        *,
        progress: str | None = None,
    ) -> None:
        self.page = page
        self.progress = progress

    def allocate_collect_id(self) -> str:
        return "00000000-0001-7000-8000-000000000000"

    def read_pending_work(
        self,
        *,
        scope: DestinationProgressScope,
        max_rows: int,
        cursor: PendingWorkCursor | None = None,
        source_collect_id: str | None = None,
        progress_position: ScanPosition | None = None,
        progress_position_loaded: bool = False,
    ) -> PendingWorkPage:
        _ = (scope, max_rows, cursor, source_collect_id, progress_position)
        _ = progress_position_loaded
        return self.page

    def register_destination_progress(
        self,
        scope: DestinationProgressScope,
    ) -> DestinationProgress:
        return self.get_destination_progress(scope)

    def get_destination_progress(
        self,
        scope: DestinationProgressScope,
    ) -> DestinationProgress:
        position = (
            StateOrderedWorkScanPosition(collect_id=self.progress, sequence_order=0)
            if self.progress
            else None
        )
        return DestinationProgress(scope=scope, position=position)

    def update_destination_progress(
        self,
        *,
        scope: DestinationProgressScope,
        position: ScanPosition | None,
        advance: bool = True,
        current_position: ScanPosition | None = None,
        current_position_loaded: bool = False,
    ) -> DestinationProgressUpdate:
        before = (
            current_position
            if current_position_loaded
            else self.get_destination_progress(scope).position
        )
        if advance:
            collect_id = getattr(position, "collect_id", None)
            self.progress = str(collect_id) if collect_id is not None else None
        after = self.get_destination_progress(scope).position
        return DestinationProgressUpdate(
            scope=scope,
            before=before,
            after=after,
            advanced=advance and before != after,
        )

    def retention_watermark(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        progress_positions: tuple[ScanPosition | None, ...] | None = None,
    ) -> str | None:
        _ = (family, declaration_name, progress_positions)
        return None

    def cleanup_ordered_work(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        through_collect_id: str | None = None,
        dry_run: bool = False,
    ) -> OrderedWorkRetentionCleanup:
        return OrderedWorkRetentionCleanup(
            family=family,
            declaration_name=declaration_name,
            requested_through_collect_id=through_collect_id,
            safe_through_collect_id=None,
            deleted_ordered_work_count=0,
            retained_pending_count=0,
            dry_run=dry_run,
        )


class _StateCurrentPageStore(_PendingPageStore):
    def __init__(self, page: StateCurrentPage, *, progress: str | None = None) -> None:
        super().__init__(
            PendingWorkPage(
                payload=_record_batch(row_count=0),
                row_count=0,
            ),
            progress=progress,
        )
        self.current_page = page

    def produce_state_collect(
        self,
        *,
        declaration: object,
        snapshot: StateSnapshotHandle,
    ) -> StateProductionResult:
        _ = (declaration, snapshot)
        raise NotImplementedError

    def state_current_summary(
        self,
        *,
        declaration_name: str,
        source_name: str,
    ) -> StateCurrentSummary:
        return StateCurrentSummary(
            declaration_name=declaration_name,
            source_name=source_name,
            collect_id=self.current_page.collect_id,
            row_count=self.current_page.row_count,
        )

    def read_state_current_upserts(
        self,
        *,
        declaration_name: str,
        source_name: str,
        max_rows: int,
        cursor: StateCurrentCursor | None = None,
        position: StateCurrentSnapshotScanPosition | None = None,
    ) -> StateCurrentPage:
        _ = (declaration_name, source_name, max_rows, cursor, position)
        return self.current_page


def test_pending_stage_page_exposes_record_batch_payload(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync()
    append_ordered_work(store, [_work(collect_id=sequence, key="cust_1")])

    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    assert isinstance(staged.page.payload, pa.RecordBatch)
    assert staged.page.payload.num_rows == staged.page.row_count == staged.row_count
    assert staged.page.scope == staged.scope
    assert set(staged.page.payload.schema.names) >= {
        "work_id",
        "collect_id",
        "sequence_order",
        "family",
        "kind",
        "declaration_name",
        "key_json",
        "target_json",
        "identifiers_json",
        "payload_json",
    }


def test_stage_rejects_store_page_with_row_count_mismatch() -> None:
    page = PendingWorkPage(
        payload=_record_batch(row_count=1),
        row_count=2,
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=0,
        complete_through_collect_id="00000000-0001-7000-8000-000000000000",
    )

    with pytest.raises(DeclarationValidationError, match="row_count must match"):
        stage_sync_pending_work(sync=_sync(), store=_PendingPageStore(page), max_rows=10)


def test_stage_rejects_store_page_with_missing_required_payload_column() -> None:
    page = PendingWorkPage(
        payload=_record_batch(row_count=1, omit="payload_json"),
        row_count=1,
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=0,
        complete_through_collect_id="00000000-0001-7000-8000-000000000000",
    )

    with pytest.raises(DeclarationValidationError, match="payload_json"):
        stage_sync_pending_work(sync=_sync(), store=_PendingPageStore(page), max_rows=10)


def test_empty_pending_page_metadata_is_valid() -> None:
    page = PendingWorkPage(payload=_record_batch(row_count=0), row_count=0)

    staged = stage_sync_pending_work(sync=_sync(), store=_PendingPageStore(page), max_rows=10)

    assert staged.row_count == 0
    assert staged.boundary.first_collect_id is None
    assert staged.boundary.last_collect_id is None
    assert staged.boundary.first_sequence_order is None
    assert staged.boundary.last_sequence_order is None


def test_empty_pending_page_rejects_boundary_metadata() -> None:
    page = PendingWorkPage(
        payload=_record_batch(row_count=0),
        row_count=0,
        first_collect_id="00000000-0001-7000-8000-000000000000",
    )

    with pytest.raises(DeclarationValidationError, match="Empty PendingWorkPage"):
        stage_sync_pending_work(sync=_sync(), store=_PendingPageStore(page), max_rows=10)


def test_non_empty_pending_page_requires_boundary_metadata() -> None:
    page = PendingWorkPage(payload=_record_batch(row_count=1), row_count=1)

    with pytest.raises(DeclarationValidationError, match="Non-empty PendingWorkPage"):
        stage_sync_pending_work(sync=_sync(), store=_PendingPageStore(page), max_rows=10)


def test_pending_staging_pages_deterministically(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence_1 = _collect_id(1)
    sequence_2 = _collect_id(2)
    sync = _sync()
    append_ordered_work(
        store,
        [
            _work(collect_id=sequence_2, key="cust_3"),
            _work(collect_id=sequence_1, key="cust_1"),
            _work(collect_id=sequence_2, key="cust_2"),
        ],
    )

    first = stage_sync_pending_work(sync=sync, store=store, max_rows=3)
    second = stage_sync_pending_work(sync=sync, store=store, max_rows=3)

    assert [key["id"] for key in _json_column_values(first.page.payload, "key_json")] == [
        "cust_1",
        "cust_3",
        "cust_2",
    ]
    assert _column_values(first.page.payload, "work_id") == _column_values(
        second.page.payload,
        "work_id",
    )
    assert first.next_cursor is None


def test_cursor_paging_works_for_collect_id_larger_than_max_rows(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync()
    append_ordered_work(
        store,
        [
            _work(collect_id=sequence, key="cust_1"),
            _work(collect_id=sequence, key="cust_2"),
            _work(collect_id=sequence, key="cust_3"),
        ],
    )

    first = stage_sync_pending_work(sync=sync, store=store, max_rows=2)
    assert isinstance(first.next_cursor, PendingWorkCursor)
    second = stage_sync_pending_work(
        sync=sync,
        store=store,
        max_rows=2,
        cursor=first.next_cursor,
    )

    assert [key["id"] for key in _json_column_values(first.page.payload, "key_json")] == [
        "cust_1",
        "cust_2",
    ]
    assert [key["id"] for key in _json_column_values(second.page.payload, "key_json")] == ["cust_3"]


def test_state_resend_all_staging_reads_current_state_as_bounded_upserts(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    source_database = _create_state_source(backend)
    _replace_state_rows(
        source_database,
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_2", "two@example.com", "free", "audience_a"),
            ("cust_3", "three@example.com", "pro", "audience_a"),
        ],
    )
    declaration = _state(_duckdb_state_source(backend))
    produce_state_collect(declaration=declaration, store=store)
    sync = _sync(declaration=declaration)

    first = stage_sync_resend_all_state(sync=sync, store=store, max_rows=2)
    assert isinstance(first.next_cursor, StateCurrentCursor)
    second = stage_sync_resend_all_state(
        sync=sync,
        store=store,
        max_rows=2,
        cursor=first.next_cursor,
    )

    assert first.mode == "resend_all"
    assert isinstance(first.page.payload, pa.RecordBatch)
    assert first.page.payload.num_rows == first.page.row_count
    assert _column_values(first.page.payload, "kind") == ["upsert", "upsert"]
    assert [key["customer"] for key in _json_column_values(first.page.payload, "key_json")] == [
        "cust_1",
        "cust_2",
    ]
    assert [key["customer"] for key in _json_column_values(second.page.payload, "key_json")] == [
        "cust_3"
    ]


def test_state_current_snapshot_position_resumes_deterministic_key_scan(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    source_database = _create_state_source(backend)
    _replace_state_rows(
        source_database,
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_2", "two@example.com", "free", "audience_a"),
            ("cust_3", "three@example.com", "pro", "audience_a"),
        ],
    )
    declaration = _state(_duckdb_state_source(backend))
    produce_state_collect(declaration=declaration, store=store)
    first = store.read_state_current_upserts(
        declaration_name=declaration.name,
        source_name=declaration.source.name,
        max_rows=2,
    )
    assert isinstance(first.next_cursor, StateCurrentCursor)
    assert isinstance(first.next_cursor.position, StateCurrentSnapshotScanPosition)

    resumed = store.read_state_current_upserts(
        declaration_name=declaration.name,
        source_name=declaration.source.name,
        max_rows=2,
        position=first.next_cursor.position,
    )

    assert [key["customer"] for key in _json_column_values(resumed.payload, "key_json")] == [
        "cust_3"
    ]
    assert resumed.next_cursor is None


def test_state_resend_all_uses_committed_current_snapshot_position(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    source_database = _create_state_source(backend)
    _replace_state_rows(
        source_database,
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_2", "two@example.com", "free", "audience_a"),
            ("cust_3", "three@example.com", "pro", "audience_a"),
        ],
    )
    declaration = _state(_duckdb_state_source(backend))
    produce_state_collect(declaration=declaration, store=store)
    sync = _sync(declaration=declaration)
    first = stage_sync_resend_all_state(sync=sync, store=store, max_rows=2)
    assert isinstance(first.next_cursor, StateCurrentCursor)
    store.update_destination_progress(
        scope=destination_progress_scope(sync),
        position=first.next_cursor.position,
    )

    resumed = stage_sync_resend_all_state(sync=sync, store=store, max_rows=2)

    assert [key["customer"] for key in _json_column_values(resumed.page.payload, "key_json")] == [
        "cust_3"
    ]
    assert resumed.next_cursor is None


def test_event_staging_reads_source_keyset_window(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    source_database = _create_event_source(backend)
    _replace_event_rows(
        source_database,
        [
            ("p_1", "one@example.com", "2024-01-01T00:00:00Z", 100),
            ("p_2", "two@example.com", "2024-01-02T00:00:00Z", 200),
        ],
    )
    declaration = _event(_duckdb_event_source(backend))
    sync = _sync(declaration=declaration, name="purchase_sync", surface="purchase_event")

    staged = stage_event_declaration(sync=sync, store=store, max_rows=10)

    assert staged.scope.family == "event"
    assert staged.mode == "pending"
    assert isinstance(staged.page.payload, pa.RecordBatch)
    assert _column_values(staged.page.payload, "kind") == ["import", "import"]


def test_pending_ordered_work_staging_rejects_event_sync(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    _create_event_source(backend)
    declaration = _event(_duckdb_event_source(backend))
    sync = _sync(declaration=declaration, name="purchase_sync", surface="purchase_event")

    with pytest.raises(
        DeclarationValidationError,
        match="Event pending-work staging from ordered_work.*stage_event_declaration",
    ):
        stage_sync_pending_work(sync=sync, store=store, max_rows=10)


def test_event_staging_with_cursor_loads_progress_before_page_wrapper(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    source_database = _create_event_source(backend)
    _replace_event_rows(
        source_database,
        [
            ("p_1", "one@example.com", "2024-01-01T00:00:00Z", 100),
            ("p_2", "two@example.com", "2024-01-02T00:00:00Z", 200),
        ],
    )
    declaration = _event(_duckdb_event_source(backend))
    sync = _sync(declaration=declaration, name="purchase_sync", surface="purchase_event")
    cursor = EventSourceCursor(
        position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2024-01-01T00:00:00Z"),
            primary_key_value=CanonicalKeyScalar.string("p_1"),
        )
    )

    staged = stage_event_declaration(
        sync=sync,
        store=store,
        max_rows=10,
        cursor=cursor,
        progress=None,
    )

    assert staged.scope.family == "event"
    assert _column_values(staged.page.payload, "event_primary_key_value") == ["p_2"]


def _create_state_source(backend: DuckDBSqlBackend) -> Path:
    database = Path(backend.database)
    connection = duckdb.connect(str(database))
    connection.execute(
        """
        create table customers (
            customer_id varchar,
            email varchar,
            plan varchar,
            audience_key varchar
        )
        """
    )
    connection.close()
    return database


def _replace_state_rows(
    database: Path,
    rows: list[tuple[str, str, str, str]],
) -> None:
    connection = duckdb.connect(str(database))
    connection.execute("delete from customers")
    connection.executemany("insert into customers values (?, ?, ?, ?)", rows)
    connection.close()


def _duckdb_state_source(backend: DuckDBSqlBackend) -> retl.Source:
    return retl.source(
        name="customers",
        query="select customer_id, email, plan, audience_key from customers",
        backend=backend.source_backend(),
    )


def _create_event_source(backend: DuckDBSqlBackend) -> Path:
    database = Path(backend.database)
    connection = duckdb.connect(str(database))
    connection.execute(
        """
        create table purchases (
            purchase_id varchar,
            email varchar,
            occurred_at varchar,
            amount integer
        )
        """
    )
    connection.close()
    return database


def _replace_event_rows(
    database: Path,
    rows: list[tuple[str, str, str, int]],
) -> None:
    connection = duckdb.connect(str(database))
    connection.execute("delete from purchases")
    connection.executemany("insert into purchases values (?, ?, ?, ?)", rows)
    connection.close()


def _duckdb_event_source(backend: DuckDBSqlBackend) -> retl.Source:
    return retl.source(
        name="purchases",
        query="select purchase_id, email, occurred_at, amount from purchases",
        mode="checkpointed",
        checkpoint={
            "cursor": "occurred_at",
            "primary_key": "purchase_id",
            "cursor_type": "string",
            "primary_key_type": "string",
        },
        backend=backend.source_backend(),
    )
