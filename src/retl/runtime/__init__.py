from collections.abc import Sequence

from retl.console import ConsoleInput
from retl.declarations import Sync
from retl.runtime import executor as _executor
from retl.runtime.defaults import (
    DEFAULT_RECONCILE_BATCH_MAX_BYTES,
    DEFAULT_RECONCILE_BATCH_MAX_ROWS,
    DEFAULT_STAGE_BATCH_MAX_ROWS,
)
from retl.runtime.progress import (
    ProgressAdvance,
    SyncProgressAdvance,
    advance_progress_after_sync,
    decide_destination_progress,
    destination_progress_scope,
    read_destination_progress,
    register_destination_progress,
)
from retl.runtime.reconcile import (
    EventImportPage,
    EventReconcilePageEvidence,
    SkippedRemoveEvidence,
    StateOperationPage,
    StateReconcileEvidence,
    reconcile_event_imports,
    reconcile_state_operations,
)
from retl.runtime.recovery import (
    AttemptIdentity,
    AttemptRecord,
    CommitDecisionRecord,
    InMemoryAttemptRecoveryStore,
)
from retl.runtime.reports import (
    CommitSummary,
    DestinationSummary,
    FailureSummary,
    PhaseSummary,
    ProgressSummary,
    ReconcileSummary,
    ReportReference,
    RetentionSummary,
    RunIndex,
    RunIndexEntry,
    SyncReport,
)
from retl.runtime.results import (
    DeclarationStageResult,
    PhaseEvidence,
    PhaseName,
    PhaseStatus,
    PhaseStatusValue,
    RunResult,
    RunStatus,
    SourceGroupResult,
    SyncResult,
)
from retl.runtime.runner import Runner, runner
from retl.runtime.staging import (
    StageEvidence,
    StageMode,
    StagePageBoundary,
    StageWorkPage,
    stage_sync_pending_work,
    stage_sync_resend_all_state,
)
from retl.stores.contracts import (
    RecoveryStore,
    RuntimeStore,
)


def run_syncs(
    *,
    runner_name: str,
    syncs: Sequence[Sync],
    dry_run: bool,
    recovery_store: RecoveryStore,
    runtime_store: RuntimeStore | None = None,
    resend_all: bool = False,
    stage_batch_max_rows: int = DEFAULT_STAGE_BATCH_MAX_ROWS,
    reconcile_batch_max_rows: int = DEFAULT_RECONCILE_BATCH_MAX_ROWS,
    reconcile_batch_max_bytes: int | None = DEFAULT_RECONCILE_BATCH_MAX_BYTES,
    console: ConsoleInput = None,
) -> RunResult:
    return _executor.run_syncs(
        runner_name=runner_name,
        syncs=syncs,
        dry_run=dry_run,
        resend_all=resend_all,
        runtime_store=runtime_store,
        stage_batch_max_rows=stage_batch_max_rows,
        reconcile_batch_max_rows=reconcile_batch_max_rows,
        reconcile_batch_max_bytes=reconcile_batch_max_bytes,
        recovery_store=recovery_store,
        console=console,
    )


__all__ = [
    "DEFAULT_RECONCILE_BATCH_MAX_BYTES",
    "DEFAULT_RECONCILE_BATCH_MAX_ROWS",
    "DEFAULT_STAGE_BATCH_MAX_ROWS",
    "AttemptIdentity",
    "AttemptRecord",
    "CommitDecisionRecord",
    "CommitSummary",
    "DeclarationStageResult",
    "Runner",
    "DestinationSummary",
    "FailureSummary",
    "InMemoryAttemptRecoveryStore",
    "EventImportPage",
    "EventReconcilePageEvidence",
    "PhaseEvidence",
    "PhaseName",
    "PhaseStatus",
    "PhaseStatusValue",
    "PhaseSummary",
    "ProgressAdvance",
    "ProgressSummary",
    "RecoveryStore",
    "ReconcileSummary",
    "ReportReference",
    "RetentionSummary",
    "RunIndex",
    "RunIndexEntry",
    "RunResult",
    "RunStatus",
    "SourceGroupResult",
    "StageEvidence",
    "StageMode",
    "StagePageBoundary",
    "StageWorkPage",
    "SyncReport",
    "SyncResult",
    "SkippedRemoveEvidence",
    "StateOperationPage",
    "StateReconcileEvidence",
    "SyncProgressAdvance",
    "advance_progress_after_sync",
    "decide_destination_progress",
    "destination_progress_scope",
    "reconcile_event_imports",
    "reconcile_state_operations",
    "runner",
    "read_destination_progress",
    "register_destination_progress",
    "run_syncs",
    "stage_sync_pending_work",
    "stage_sync_resend_all_state",
]
