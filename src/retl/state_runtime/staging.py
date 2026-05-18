from __future__ import annotations

import hashlib
import json

from retl.declarations import State, Sync
from retl.declarations.provenance import canonical_state_target
from retl.errors import DeclarationValidationError
from retl.runtime.staging import (
    StageEvidence as SyncStageEvidence,
)
from retl.runtime.staging import (
    stage_sync_pending_work,
    stage_sync_resend_all_state,
)
from retl.sources.contracts import source_identity
from retl.stores.contracts import (
    DestinationProgress,
    OrderedWorkStore,
    PendingWorkCursor,
    StateCurrentCursor,
    StateProductionStore,
)


def declaration_identity(declaration: State) -> str:
    payload = {
        "name": declaration.name,
        "source_identity": source_identity(declaration.source),
        "key": dict(declaration.key),
        "identifiers": tuple(dict(identifier) for identifier in declaration.identifiers),
        "payload": dict(declaration.payload),
        "target": canonical_state_target(declaration.target),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def stage_declaration(
    *,
    sync: Sync,
    store: OrderedWorkStore,
    max_rows: int,
    cursor: PendingWorkCursor | None = None,
    progress: DestinationProgress | None = None,
    dry_run: bool = False,
) -> SyncStageEvidence:
    if not isinstance(sync.declaration, State):
        raise DeclarationValidationError("State staging requires a State Sync.")
    return stage_sync_pending_work(
        sync=sync,
        store=store,
        max_rows=max_rows,
        cursor=cursor,
        progress=progress,
        dry_run=dry_run,
    )


def stage_resend_all(
    *,
    sync: Sync,
    store: StateProductionStore,
    max_rows: int,
    cursor: StateCurrentCursor | None = None,
    progress: DestinationProgress | None = None,
    dry_run: bool = False,
) -> SyncStageEvidence:
    return stage_sync_resend_all_state(
        sync=sync,
        store=store,
        max_rows=max_rows,
        cursor=cursor,
        progress=progress,
        dry_run=dry_run,
    )


__all__ = [
    "declaration_identity",
    "stage_declaration",
    "stage_resend_all",
]
