from __future__ import annotations

from retl.declarations import Sync
from retl.runtime.reconcile import (
    SkippedRemoveEvidence,
    StateOperationPage,
    StateReconcileEvidence,
    reconcile_state_operations,
)
from retl.runtime.staging import StageEvidence, StageWorkPage


def reconcile_sync(
    *,
    sync: Sync,
    staged: StageEvidence | StageWorkPage,
    dry_run: bool | None = None,
) -> StateReconcileEvidence:
    return reconcile_state_operations(sync=sync, staged=staged, dry_run=dry_run)


__all__ = [
    "SkippedRemoveEvidence",
    "StateOperationPage",
    "StateReconcileEvidence",
    "reconcile_sync",
]
