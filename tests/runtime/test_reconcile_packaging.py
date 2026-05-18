from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

import retl
from retl.auth import none
from retl.backends.duckdb import DuckDBRuntimeStore, DuckDBSqlBackend
from retl.destinations.surfaces import DestinationConnector, DestinationSurface
from retl.events.reconcile import reconcile_event_imports
from retl.runtime.results import PhaseEvidence, PhaseStatus
from retl.runtime.staging import (
    StageEvidence,
    StagePageBoundary,
    StageWorkPage,
    stage_sync_pending_work,
    stage_sync_resend_all_state,
)
from retl.state_runtime.producer import produce_state_collect
from retl.state_runtime.reconcile import reconcile_sync
from retl.stores.contracts import OrderedWorkInput
from tests.runtime.ordered_work_helpers import append_ordered_work


def test_state_upsert_staged_work_emits_columnar_operation_page(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync(surface="state_with_remove", destination=_destination())
    append_ordered_work(store, [_state_work(sequence=sequence, key="cust_1", kind="upsert")])
    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    reconciled = reconcile_sync(sync=sync, staged=staged)

    page = reconciled.operation_pages[0]
    assert isinstance(page.payload, pa.RecordBatch)
    assert reconciled.operation_count == 1
    assert reconciled.upsert_count == 1
    assert reconciled.remove_count == 0
    assert reconciled.skipped_remove_count == 0
    assert page.row_count == 1
    assert _column_values(page.payload, "operation") == ["upsert"]
    assert "key_json" in page.payload.schema.names


def test_state_upsert_only_page_does_not_require_surface_lookup(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync(surface="missing_surface", destination=_destination())
    append_ordered_work(store, [_state_work(sequence=sequence, key="cust_1", kind="upsert")])
    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    reconciled = reconcile_sync(sync=sync, staged=staged)

    assert reconciled.operation_count == 1
    assert reconciled.upsert_count == 1
    assert reconciled.remove_count == 0


def test_state_remove_staged_work_is_packaged_when_surface_allows_removes(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync(surface="state_with_remove", destination=_destination())
    append_ordered_work(store, [_state_work(sequence=sequence, key="cust_1", kind="remove")])
    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    reconciled = reconcile_sync(sync=sync, staged=staged)

    page = reconciled.operation_pages[0]
    assert page.row_count == 1
    assert reconciled.upsert_count == 0
    assert reconciled.remove_count == 1
    assert reconciled.skipped_remove_count == 0
    assert _column_values(page.payload, "operation") == ["remove"]


def test_state_remove_staged_work_is_skipped_when_policy_suppresses_removes(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync(surface="state_upsert_only", destination=_destination())
    append_ordered_work(store, [_state_work(sequence=sequence, key="cust_1", kind="remove")])
    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    reconciled = reconcile_sync(sync=sync, staged=staged)

    page = reconciled.operation_pages[0]
    assert page.row_count == 0
    assert reconciled.operation_count == 0
    assert reconciled.upsert_count == 0
    assert reconciled.remove_count == 0
    assert reconciled.skipped_remove_count == 1
    assert (
        reconciled.skipped_removes[0].work_id
        == _column_values(
            staged.page.payload,
            "work_id",
        )[0]
    )


def test_state_remove_staged_work_is_skipped_when_sync_excludes_removes(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync(
        surface="state_with_remove",
        destination=_destination(),
        operations=("upsert",),
    )
    append_ordered_work(store, [_state_work(sequence=sequence, key="cust_1", kind="remove")])
    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    reconciled = reconcile_sync(sync=sync, staged=staged)

    page = reconciled.operation_pages[0]
    assert page.row_count == 0
    assert reconciled.operation_count == 0
    assert reconciled.remove_count == 0
    assert reconciled.skipped_remove_count == 1


def test_state_remove_reconcile_rejects_unknown_selected_surface(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync(surface="missing_surface", destination=_destination())
    append_ordered_work(store, [_state_work(sequence=sequence, key="cust_1", kind="remove")])
    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    with pytest.raises(retl.DeclarationValidationError, match="could not resolve"):
        reconcile_sync(sync=sync, staged=staged)

    upsert_store_path = tmp_path / "upsert_unknown"
    upsert_store_path.mkdir()
    upsert_store = _store(upsert_store_path)
    upsert_sync = _sync(surface="missing_surface", destination=_destination())
    upsert_sequence = upsert_store.allocate_collect_id()
    append_ordered_work(
        upsert_store, [_state_work(sequence=upsert_sequence, key="cust_2", kind="upsert")]
    )
    upsert_staged = stage_sync_pending_work(sync=upsert_sync, store=upsert_store, max_rows=10)
    assert reconcile_sync(sync=upsert_sync, staged=upsert_staged).operation_count == 1


def test_state_remove_reconcile_rejects_surface_without_operation_capabilities(
    tmp_path: Path,
) -> None:
    class DestinationWithoutSurfaceCapabilities:
        binding_name = "test_destination"

        def surface(self, name: str) -> object:
            return object()

    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = retl.sync(
        name="customer_sync",
        declaration=_state(),
        destination=DestinationWithoutSurfaceCapabilities(),
        surface="state_unknown_capabilities",
    )
    append_ordered_work(store, [_state_work(sequence=sequence, key="cust_1", kind="remove")])
    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    with pytest.raises(retl.DeclarationValidationError, match="cannot prove"):
        reconcile_sync(sync=sync, staged=staged)

    upsert_store_path = tmp_path / "upsert_capabilities"
    upsert_store_path.mkdir()
    upsert_store = _store(upsert_store_path)
    upsert_sequence = upsert_store.allocate_collect_id()
    append_ordered_work(
        upsert_store, [_state_work(sequence=upsert_sequence, key="cust_2", kind="upsert")]
    )
    upsert_staged = stage_sync_pending_work(sync=sync, store=upsert_store, max_rows=10)
    assert reconcile_sync(sync=sync, staged=upsert_staged).operation_count == 1


def test_skipping_removes_does_not_mutate_ordered_work(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync(surface="state_upsert_only", destination=_destination())
    append_ordered_work(store, [_state_work(sequence=sequence, key="cust_1", kind="remove")])
    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    reconciled = reconcile_sync(sync=sync, staged=staged)
    retry = stage_sync_pending_work(sync=sync, store=store, max_rows=10)

    assert reconciled.skipped_remove_count == 1
    assert _column_values(retry.page.payload, "work_id") == _column_values(
        staged.page.payload,
        "work_id",
    )
    assert _column_values(retry.page.payload, "kind") == ["remove"]


def test_state_resend_all_staged_rows_package_as_upsert_operations(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    source_database = _create_state_source(backend)
    _replace_state_rows(source_database, [("cust_1", "one@example.com", "pro", "audience_a")])
    declaration = _state(_duckdb_state_source(backend))
    produce_state_collect(declaration=declaration, store=store)
    sync = _sync(declaration=declaration, surface="state_with_remove", destination=_destination())
    staged = stage_sync_resend_all_state(sync=sync, store=store, max_rows=10)

    reconciled = reconcile_sync(sync=sync, staged=staged)

    page = reconciled.operation_pages[0]
    assert reconciled.mode == "resend_all"
    assert reconciled.operation_count == 1
    assert reconciled.upsert_count == 1
    assert reconciled.remove_count == 0
    assert _column_values(page.payload, "operation") == ["upsert"]


def test_event_staged_import_work_becomes_columnar_event_import_page(tmp_path: Path) -> None:
    _ = tmp_path
    declaration = _event()
    sync = _sync(declaration=declaration, surface="event_import", destination=_destination())
    payload = pa.RecordBatch.from_pydict(
        {
            "work_id": ["work-event-1"],
            "collect_id": ["00000000-0001-7000-8000-000000000000"],
            "sequence_order": [0],
            "family": ["event"],
            "kind": ["import"],
            "declaration_name": [declaration.name],
            "key_json": ['{"purchase":"p_1"}'],
            "target_json": [None],
            "identifiers_json": ['[{"type":"email","value":"one@example.com"}]'],
            "payload_json": ['{"amount":100}'],
            "event_occurred_at": ["2024-01-01T00:00:00Z"],
            "event_cursor_value": ["2024-01-01T00:00:00Z"],
            "event_primary_key_value": ["p_1"],
        }
    )
    scope = retl.runtime.destination_progress_scope(sync)
    boundary = StagePageBoundary(
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=0,
        complete_through_collect_id=None,
    )
    staged_page = StageWorkPage(
        phase="stage",
        scope=scope,
        mode="pending",
        payload=payload,
        row_count=1,
        progress_before=None,
        boundary=boundary,
        next_cursor=None,
        safe_to_advance_collect_id=False,
    )
    staged = StageEvidence(
        phase="stage",
        status="succeeded",
        phase_status=PhaseStatus(
            name="stage",
            status="succeeded",
            evidence=PhaseEvidence(
                kind="planned",
                message="Staged 1 event row(s) in pending mode.",
                dry_run=False,
            ),
        ),
        scope=scope,
        mode="pending",
        row_count=1,
        progress_before=None,
        boundary=boundary,
        next_cursor=None,
        safe_to_advance_collect_id=False,
        page=staged_page,
        dry_run=False,
    )

    reconciled = reconcile_event_imports(sync=sync, staged=staged)

    import_page = reconciled.import_pages[0]
    assert isinstance(import_page.payload, pa.RecordBatch)
    assert reconciled.import_count == 1
    assert import_page.import_count == 1
    assert import_page.row_count == 1
    assert _column_values(import_page.payload, "kind") == ["import"]


def test_reconcile_preserves_staged_boundary_cursor_and_stage_metadata(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    sync = _sync(surface="state_with_remove", destination=_destination())
    append_ordered_work(
        store,
        [
            _state_work(sequence=sequence, key="cust_1", kind="upsert"),
            _state_work(sequence=sequence, key="cust_2", kind="upsert"),
            _state_work(sequence=sequence, key="cust_3", kind="upsert"),
        ],
    )
    staged = stage_sync_pending_work(sync=sync, store=store, max_rows=2)

    reconciled = reconcile_sync(sync=sync, staged=staged)
    page = reconciled.operation_pages[0]

    assert reconciled.input_stage_boundary == staged.boundary
    assert reconciled.progress_boundary == staged.boundary
    assert reconciled.progress_before == staged.progress_before
    assert reconciled.next_cursor == staged.next_cursor
    assert reconciled.mode == staged.mode
    assert page.input_stage_boundary == staged.boundary
    assert page.progress_before == staged.progress_before
    assert page.next_cursor == staged.next_cursor


def test_reconcile_rejects_mismatched_sync_declaration_or_family(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sequence = store.allocate_collect_id()
    state_sync = _sync(surface="state_with_remove", destination=_destination())
    other_sync = _sync(
        declaration=_state(name="other_state"),
        surface="state_with_remove",
        destination=_destination(),
    )
    event_sync = _sync(declaration=_event(), surface="event_import", destination=_destination())
    append_ordered_work(store, [_state_work(sequence=sequence, key="cust_1", kind="upsert")])
    staged = stage_sync_pending_work(sync=state_sync, store=store, max_rows=10)

    with pytest.raises(retl.DeclarationValidationError, match="scope does not match"):
        reconcile_sync(sync=other_sync, staged=staged)
    with pytest.raises(retl.DeclarationValidationError, match="Event reconcile"):
        reconcile_event_imports(sync=state_sync, staged=staged)
    with pytest.raises(retl.DeclarationValidationError, match="scope does not match"):
        reconcile_event_imports(sync=event_sync, staged=staged)

    mismatched_payload = staged.page.payload.set_column(
        staged.page.payload.schema.get_field_index("family"),
        "family",
        pa.array(["event"]),
    )
    mismatched_page = StageWorkPage(
        phase=staged.page.phase,
        scope=staged.page.scope,
        mode=staged.page.mode,
        payload=mismatched_payload,
        row_count=staged.page.row_count,
        progress_before=staged.page.progress_before,
        boundary=staged.page.boundary,
        next_cursor=staged.page.next_cursor,
        safe_to_advance_collect_id=staged.page.safe_to_advance_collect_id,
    )
    with pytest.raises(retl.DeclarationValidationError, match="family values"):
        reconcile_sync(sync=state_sync, staged=mismatched_page)


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return _backend(tmp_path).runtime_store()


def _backend(tmp_path: Path) -> DuckDBSqlBackend:
    return DuckDBSqlBackend(
        database=tmp_path / "warehouse.duckdb",
        source_schema="main",
        runtime_schema="retl",
    )


def _destination() -> retl.DestinationBinding:
    connector = DestinationConnector(
        name="test",
        auth_modes=(none(),),
        surfaces=(
            DestinationSurface(
                name="state_with_remove",
                declaration_family="state",
                supported_operations=("upsert", "remove"),
                target_mode="optional",
            ),
            DestinationSurface(
                name="state_upsert_only",
                declaration_family="state",
                supported_operations=("upsert",),
                target_mode="optional",
            ),
            DestinationSurface(
                name="event_import",
                declaration_family="event",
                supported_operations=("import",),
            ),
        ),
    )
    return retl.DestinationBinding(
        binding_name="test_destination",
        destination_ref="test",
        connector=connector,
    )


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


def _event(source: retl.Source | None = None) -> retl.Event:
    return retl.event(
        name="purchase_event",
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
    surface: str,
    destination: retl.DestinationBinding,
    operations: tuple[retl.StateOperation, ...] | None = None,
) -> retl.Sync:
    return retl.sync(
        name="customer_sync" if not isinstance(declaration, retl.Event) else "purchase_sync",
        declaration=declaration or _state(),
        destination=destination,
        surface=surface,
        operations=operations,
    )


def _state_work(*, sequence: str, key: str, kind: str) -> OrderedWorkInput:
    return OrderedWorkInput(
        collect_id=sequence,
        family="state",
        kind=kind,  # type: ignore[arg-type]
        declaration_name="customer_state",
        key={"customer": key},
        target={"value": "audience_a"},
        identifiers=({"type": "email", "value": f"{key}@example.com"},),
        payload={"plan": "pro"},
    )


def _column_values(page: pa.RecordBatch, column: str) -> list[Any]:
    return page.column(column).to_pylist()


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
