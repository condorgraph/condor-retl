from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pyarrow as pa  # type: ignore[import-untyped]

from retl.declarations import Event, State, Sync
from retl.errors import DeclarationValidationError
from retl.runtime.progress import destination_progress_scope, read_destination_progress
from retl.runtime.results import PhaseEvidence, PhaseStatus
from retl.stores.contracts import (
    DestinationProgress,
    DestinationProgressScope,
    EventSourceCursor,
    OrderedWorkStore,
    PendingWorkCursor,
    PendingWorkPage,
    ScanPosition,
    StateCurrentCursor,
    StateCurrentPage,
    StateCurrentSnapshotScanPosition,
    StateProductionStore,
)

StageMode = Literal["pending", "resend_all"]
StageCursor = PendingWorkCursor | StateCurrentCursor | EventSourceCursor

_REQUIRED_STAGED_PAYLOAD_COLUMNS = frozenset(
    {
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
)


@dataclass(frozen=True)
class StagePageBoundary:
    first_collect_id: str | None
    last_collect_id: str | None
    first_sequence_order: int | None
    last_sequence_order: int | None
    complete_through_collect_id: str | None


@dataclass(frozen=True)
class StageWorkPage:
    phase: Literal["stage"]
    scope: DestinationProgressScope
    mode: StageMode
    payload: pa.RecordBatch
    row_count: int
    progress_before: str | None
    boundary: StagePageBoundary
    next_cursor: StageCursor | None
    safe_to_advance_collect_id: bool


@dataclass(frozen=True)
class StageEvidence:
    phase: Literal["stage"]
    status: Literal["succeeded"]
    phase_status: PhaseStatus
    scope: DestinationProgressScope
    mode: StageMode
    row_count: int
    progress_before: str | None
    boundary: StagePageBoundary
    next_cursor: StageCursor | None
    safe_to_advance_collect_id: bool
    page: StageWorkPage
    dry_run: bool = False


def stage_sync_pending_work(
    *,
    sync: Sync,
    store: OrderedWorkStore,
    max_rows: int,
    cursor: PendingWorkCursor | None = None,
    source_collect_id: str | None = None,
    progress: DestinationProgress | None = None,
    dry_run: bool = False,
) -> StageEvidence:
    _validate_sync(sync)
    if isinstance(sync.declaration, Event):
        raise DeclarationValidationError(
            "Event pending-work staging from ordered_work is not supported; use "
            "retl.events.stage_event_declaration for Event source keyset range staging."
        )
    scope = destination_progress_scope(sync)
    if progress is None:
        progress = read_destination_progress(store=store, sync=sync)
    if source_collect_id is None:
        page = store.read_pending_work(
            scope=scope,
            max_rows=max_rows,
            cursor=cursor,
            progress_position=progress.position,
            progress_position_loaded=True,
        )
    else:
        page = store.read_pending_work(
            scope=scope,
            max_rows=max_rows,
            cursor=cursor,
            source_collect_id=source_collect_id,
            progress_position=progress.position,
            progress_position_loaded=True,
        )
    _validate_pending_store_page(page)
    boundary = _pending_page_boundary(page)
    safe_to_advance_collect_id = (
        False if isinstance(sync.declaration, Event) else _pending_page_safe_to_advance(page)
    )
    return _stage_evidence(
        sync=sync,
        store=store,
        scope=scope,
        mode="pending",
        payload=page.payload,
        row_count=page.row_count,
        progress_before=_temporary_collect_progress_evidence(progress.position),
        boundary=boundary,
        next_cursor=page.next_cursor,
        safe_to_advance_collect_id=safe_to_advance_collect_id,
        dry_run=dry_run,
    )


def stage_sync_resend_all_state(
    *,
    sync: Sync,
    store: StateProductionStore,
    max_rows: int,
    cursor: StateCurrentCursor | None = None,
    progress: DestinationProgress | None = None,
    dry_run: bool = False,
) -> StageEvidence:
    _validate_sync(sync)
    if not isinstance(sync.declaration, State):
        raise DeclarationValidationError("State resend-all staging requires a State Sync.")
    scope = destination_progress_scope(sync)
    if progress is None:
        progress = read_destination_progress(store=store, sync=sync)
    page = store.read_state_current_upserts(
        declaration_name=sync.declaration.name,
        source_name=sync.declaration.source.name,
        max_rows=max_rows,
        cursor=cursor,
        position=(
            progress.position
            if isinstance(progress.position, StateCurrentSnapshotScanPosition)
            else None
        ),
    )
    _validate_state_current_store_page(page)
    boundary = _state_current_page_boundary(page)
    return _stage_evidence(
        sync=sync,
        store=store,
        scope=scope,
        mode="resend_all",
        payload=page.payload,
        row_count=page.row_count,
        progress_before=_temporary_collect_progress_evidence(progress.position),
        boundary=boundary,
        next_cursor=page.next_cursor,
        safe_to_advance_collect_id=False,
        dry_run=dry_run,
    )


def _stage_evidence(
    *,
    sync: Sync,
    store: OrderedWorkStore,
    scope: DestinationProgressScope,
    mode: StageMode,
    payload: pa.RecordBatch,
    row_count: int,
    progress_before: str | None,
    boundary: StagePageBoundary,
    next_cursor: StageCursor | None,
    safe_to_advance_collect_id: bool,
    dry_run: bool,
) -> StageEvidence:
    _ = (sync, store)
    page = StageWorkPage(
        phase="stage",
        scope=scope,
        mode=mode,
        payload=payload,
        row_count=row_count,
        progress_before=progress_before,
        boundary=boundary,
        next_cursor=next_cursor,
        safe_to_advance_collect_id=safe_to_advance_collect_id,
    )
    return StageEvidence(
        phase="stage",
        status="succeeded",
        phase_status=PhaseStatus(
            name="stage",
            status="succeeded",
            evidence=PhaseEvidence(
                kind="planned",
                message=f"Staged {row_count} {scope.family} row(s) in {mode} mode.",
                dry_run=dry_run,
            ),
        ),
        scope=scope,
        mode=mode,
        row_count=row_count,
        progress_before=progress_before,
        boundary=boundary,
        next_cursor=next_cursor,
        safe_to_advance_collect_id=safe_to_advance_collect_id,
        page=page,
        dry_run=dry_run,
    )


def _temporary_collect_progress_evidence(position: ScanPosition | None) -> str | None:
    collect_id = getattr(position, "collect_id", None)
    return str(collect_id) if collect_id is not None else None


def _pending_page_boundary(page: PendingWorkPage) -> StagePageBoundary:
    return StagePageBoundary(
        first_collect_id=page.first_collect_id,
        last_collect_id=page.last_collect_id,
        first_sequence_order=page.first_sequence_order,
        last_sequence_order=page.last_sequence_order,
        complete_through_collect_id=page.complete_through_collect_id,
    )


def _state_current_page_boundary(page: StateCurrentPage) -> StagePageBoundary:
    return StagePageBoundary(
        first_collect_id=page.first_collect_id,
        last_collect_id=page.last_collect_id,
        first_sequence_order=page.first_sequence_order,
        last_sequence_order=page.last_sequence_order,
        complete_through_collect_id=None,
    )


def _validate_pending_store_page(page: PendingWorkPage) -> None:
    _validate_staged_payload(
        payload=page.payload,
        row_count=page.row_count,
        page_name="PendingWorkPage",
    )
    _validate_boundary_metadata(
        row_count=page.row_count,
        first_collect_id=page.first_collect_id,
        last_collect_id=page.last_collect_id,
        first_sequence_order=page.first_sequence_order,
        last_sequence_order=page.last_sequence_order,
        page_name="PendingWorkPage",
    )
    if page.next_cursor is not None and page.complete_through_collect_id is not None:
        raise DeclarationValidationError(
            "PendingWorkPage cannot be both cursor-paged and complete through a collect ID."
        )
    if page.complete_through_collect_id is not None and page.row_count == 0:
        raise DeclarationValidationError(
            "Empty PendingWorkPage cannot be complete through a collect ID."
        )


def _validate_state_current_store_page(page: StateCurrentPage) -> None:
    _validate_staged_payload(
        payload=page.payload,
        row_count=page.row_count,
        page_name="StateCurrentPage",
    )
    _validate_boundary_metadata(
        row_count=page.row_count,
        first_collect_id=page.first_collect_id,
        last_collect_id=page.last_collect_id,
        first_sequence_order=page.first_sequence_order,
        last_sequence_order=page.last_sequence_order,
        page_name="StateCurrentPage",
    )


def _validate_staged_payload(
    *,
    payload: pa.RecordBatch,
    row_count: int,
    page_name: str,
) -> None:
    if not isinstance(payload, pa.RecordBatch):
        raise DeclarationValidationError(f"{page_name}.payload must be a pyarrow.RecordBatch.")
    if payload.num_rows != row_count:
        raise DeclarationValidationError(
            f"{page_name}.row_count must match payload.num_rows "
            f"({row_count} != {payload.num_rows})."
        )
    missing_columns = sorted(_REQUIRED_STAGED_PAYLOAD_COLUMNS - set(payload.schema.names))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise DeclarationValidationError(
            f"{page_name}.payload missing required staged column(s): {missing}."
        )


def _validate_boundary_metadata(
    *,
    row_count: int,
    first_collect_id: str | None,
    last_collect_id: str | None,
    first_sequence_order: int | None,
    last_sequence_order: int | None,
    page_name: str,
) -> None:
    boundary_values = (
        first_collect_id,
        last_collect_id,
        first_sequence_order,
        last_sequence_order,
    )
    if row_count == 0:
        if any(value is not None for value in boundary_values):
            raise DeclarationValidationError(
                f"Empty {page_name} must not expose collect or sequence-order boundaries."
            )
        return
    if any(value is None for value in boundary_values):
        raise DeclarationValidationError(
            f"Non-empty {page_name} must expose first/last collect and sequence-order boundaries."
        )
    if first_collect_id is not None and last_collect_id is not None:
        if first_collect_id > last_collect_id:
            raise DeclarationValidationError(
                f"{page_name} collect-id boundaries are not internally consistent."
            )
    if first_sequence_order is not None and last_sequence_order is not None:
        if first_collect_id == last_collect_id and first_sequence_order > last_sequence_order:
            raise DeclarationValidationError(
                f"{page_name} sequence-order boundaries are not internally consistent."
            )


def _pending_page_safe_to_advance(page: PendingWorkPage) -> bool:
    if page.complete_through_collect_id is None:
        return False
    if page.last_collect_id is None:
        raise DeclarationValidationError(
            "PendingWorkPage complete-through metadata requires a last collect ID."
        )
    if page.complete_through_collect_id != page.last_collect_id:
        raise DeclarationValidationError(
            "PendingWorkPage complete-through metadata must match the staged boundary."
        )
    return True


def _validate_sync(sync: Sync) -> None:
    if not isinstance(sync, Sync):
        raise DeclarationValidationError("Staging requires a Sync.")
    if not isinstance(sync.declaration, State | Event):
        raise DeclarationValidationError("Staging requires a State or Event Sync declaration.")


__all__ = [
    "StageEvidence",
    "StageMode",
    "StagePageBoundary",
    "StageWorkPage",
    "stage_sync_pending_work",
    "stage_sync_resend_all_state",
]
