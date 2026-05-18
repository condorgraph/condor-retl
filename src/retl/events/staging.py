from __future__ import annotations

from typing import Any, cast

from retl.declarations import Event, Sync
from retl.errors import DeclarationValidationError
from retl.runtime.progress import destination_progress_scope
from retl.runtime.results import PhaseEvidence, PhaseStatus
from retl.runtime.staging import StageEvidence, StagePageBoundary, StageWorkPage
from retl.stores.contracts import (
    DestinationProgress,
    EventKeysetScanPosition,
    EventProductionStore,
    EventSourceCursor,
    EventSourceWindowRequest,
    EventSourceWindowSource,
)


def stage_event_declaration(
    *,
    sync: Sync,
    store: EventProductionStore,
    max_rows: int,
    cursor: EventSourceCursor | None = None,
    source_collect_id: str | None = None,
    progress: DestinationProgress | None = None,
    dry_run: bool = False,
) -> StageEvidence:
    if not isinstance(sync.declaration, Event):
        raise DeclarationValidationError("Event staging requires an Event Sync.")
    _ = source_collect_id
    checkpoint = sync.declaration.source.checkpoint
    if checkpoint is None:
        raise DeclarationValidationError("Event staging requires a checkpointed Source.")
    backend = sync.declaration.source.backend
    if backend is None:
        raise DeclarationValidationError("Event staging requires a Source backend.")
    adapter = cast(Any, backend).adapter()
    if not isinstance(adapter, EventSourceWindowSource):
        raise DeclarationValidationError(
            "Event staging requires a Source adapter that can prepare Event source windows."
        )
    if progress is None:
        progress = store.get_destination_progress(destination_progress_scope(sync))
    scan_after: EventKeysetScanPosition | None
    if cursor is not None:
        scan_after = cursor.position
    else:
        scan_after = (
            progress.position
            if progress is not None and isinstance(progress.position, EventKeysetScanPosition)
            else None
        )
    page = store.read_event_source_window(
        declaration=sync.declaration,
        window=adapter.prepare_event_source_window(
            EventSourceWindowRequest(
                source_name=sync.declaration.source.name,
                query=sync.declaration.source.query,
                cursor_column=checkpoint["cursor"],
                primary_key_column=checkpoint["primary_key"],
                scan_after=scan_after,
                limit=max_rows + 1,
            )
        ),
        max_rows=max_rows,
    )
    boundary = StagePageBoundary(
        first_collect_id=page.first_collect_id,
        last_collect_id=page.last_collect_id,
        first_sequence_order=page.first_sequence_order,
        last_sequence_order=page.last_sequence_order,
        complete_through_collect_id=page.complete_through_collect_id,
    )
    scope = destination_progress_scope(sync)
    work_page = StageWorkPage(
        phase="stage",
        scope=scope,
        mode="pending",
        payload=page.payload,
        row_count=page.row_count,
        progress_before=None,
        boundary=boundary,
        next_cursor=page.next_cursor,
        safe_to_advance_collect_id=False,
    )
    return StageEvidence(
        phase="stage",
        status="succeeded",
        phase_status=PhaseStatus(
            name="stage",
            status="succeeded",
            evidence=PhaseEvidence(
                kind="planned",
                message=f"Staged {page.row_count} event row(s) in pending mode.",
                dry_run=dry_run,
            ),
        ),
        scope=scope,
        mode="pending",
        row_count=page.row_count,
        progress_before=None,
        boundary=boundary,
        next_cursor=page.next_cursor,
        safe_to_advance_collect_id=False,
        page=work_page,
        dry_run=dry_run,
    )


__all__ = ["stage_event_declaration"]
