from __future__ import annotations

from retl.declarations import Sync
from retl.runtime.reconcile import (
    EventImportPage,
    EventReconcilePageEvidence,
)
from retl.runtime.reconcile import (
    reconcile_event_imports as reconcile_staged_event_imports,
)
from retl.runtime.staging import StageEvidence, StageWorkPage


class EventReconcileEvidence(EventReconcilePageEvidence):
    pass


def reconcile_event_imports(
    *,
    sync: Sync,
    staged: StageEvidence | StageWorkPage,
    dry_run: bool | None = None,
) -> EventReconcileEvidence:
    evidence = reconcile_staged_event_imports(sync=sync, staged=staged, dry_run=dry_run)
    return EventReconcileEvidence(
        phase=evidence.phase,
        status=evidence.status,
        phase_status=evidence.phase_status,
        sync_name=evidence.sync_name,
        scope=evidence.scope,
        mode=evidence.mode,
        input_stage_boundary=evidence.input_stage_boundary,
        import_pages=evidence.import_pages,
        pages=evidence.pages,
        import_count=evidence.import_count,
        progress_before=evidence.progress_before,
        progress_boundary=evidence.progress_boundary,
        next_cursor=evidence.next_cursor,
        safe_to_advance_collect_id=evidence.safe_to_advance_collect_id,
        event_cursor_kind=evidence.event_cursor_kind,
        event_primary_key_kind=evidence.event_primary_key_kind,
        dry_run=evidence.dry_run,
    )


__all__ = [
    "EventImportPage",
    "EventReconcileEvidence",
    "reconcile_event_imports",
]
