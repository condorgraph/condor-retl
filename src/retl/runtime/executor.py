from __future__ import annotations

import dataclasses
import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, cast

from retl.config import configured_runtime_store
from retl.console import ConsoleInput, ConsoleRenderer, resolve_console
from retl.declarations import DestinationBinding, Event, State, Sync
from retl.declarations.provenance import declaration_metadata
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.compatibility import (
    DestinationCompatibilityError,
    validate_surface_compatibility,
)
from retl.destinations.resolver import resolve_surface
from retl.destinations.surfaces import DestinationSurface
from retl.destinations.targets import TargetMapping, TargetResolutionEvidence
from retl.destinations.terminal_failures import decide_page_continuation
from retl.errors import DeclarationValidationError
from retl.events.producer import EventCollectEvidence
from retl.events.reconcile import EventReconcileEvidence, reconcile_event_imports
from retl.events.staging import stage_event_declaration
from retl.runtime.defaults import (
    DEFAULT_RECONCILE_BATCH_MAX_BYTES,
    DEFAULT_RECONCILE_BATCH_MAX_ROWS,
    DEFAULT_STAGE_BATCH_MAX_ROWS,
)
from retl.runtime.progress import (
    SyncProgressAdvance,
    advance_progress_after_sync,
    destination_progress_scope,
    register_destination_progress,
)
from retl.runtime.provenance import run_provenance
from retl.runtime.recovery import (
    AttemptIdentity,
    CommitDecisionRecord,
)
from retl.runtime.redaction import redact_text
from retl.runtime.reports import (
    CommitSummary,
    DestinationSummary,
    FailureSummary,
    ProgressSummary,
    ReconcileSummary,
    ReportReference,
    RetentionSummary,
    SyncReport,
    build_run_index,
    phase_summary,
    progress_scope_summary,
    scan_position_summary,
    sync_report_id,
    sync_report_ref,
)
from retl.runtime.results import (
    DeclarationStageResult,
    PhaseEvidence,
    PhaseStatus,
    RunResult,
    RunStatus,
    SourceGroupResult,
    SyncResult,
)
from retl.runtime.staging import StageEvidence
from retl.sources.contracts import source_identity
from retl.state_runtime.producer import StateCollectEvidence, produce_state_collect
from retl.state_runtime.reconcile import StateReconcileEvidence, reconcile_sync
from retl.state_runtime.staging import stage_declaration, stage_resend_all
from retl.stores.contracts import (
    DestinationProgress,
    DestinationProgressScope,
    EventKeysetScanPosition,
    EventSourceCursor,
    PendingWorkCursor,
    RecoveryStore,
    RuntimeStore,
    StateCurrentCursor,
)
from retl.sync_runtime.submission import (
    SyncPhaseEvidence,
    retry_destination_batches,
    sync_destination,
)

CollectEvidence = StateCollectEvidence | EventCollectEvidence
LOGGER = logging.getLogger("retl.runtime.executor")


@dataclass(frozen=True)
class _CollectGroup:
    key: str
    declaration: State | Event
    syncs: tuple[Sync, ...]


@dataclass(frozen=True)
class _SyncAttemptState:
    attempt: AttemptIdentity
    progress: DestinationProgress


@dataclass
class _SyncExecutionAccumulator:
    sync: Sync
    surface: DestinationSurface
    dry_run: bool
    page_count: int = 0
    stage_mode: str | None = None
    staged_row_count: int = 0
    safe_to_advance_collect_id: bool = False
    operation_count: int = 0
    upsert_count: int = 0
    remove_count: int = 0
    event_import_count: int = 0
    sync_status: Literal["deferred", "succeeded", "failed"] = "deferred"
    irreversible_writes: bool = False
    request_batch_count: int = 0
    destination_batch_count: int = 0
    destination_batch_ids: list[str] = dataclasses.field(default_factory=list)
    attempted_count: int = 0
    confirmed_count: int = 0
    accepted_count: int = 0
    retryable_failure_count: int = 0
    terminal_failure_count: int = 0
    pre_acceptance_failure_count: int = 0
    pre_acceptance_failure_category: str | None = None
    http_status: int | None = None
    partner_error_detail: str | None = None
    submission_summary: str = ""
    submission_status: str = ""
    delivery_decision_reason: str = ""
    target_statuses: set[str] = dataclasses.field(default_factory=set)
    target_logical_names: set[str] = dataclasses.field(default_factory=set)
    target_mapped_count: int = 0
    target_registry_count: int = 0
    target_managed_created_count: int = 0
    target_planned_create_count: int = 0
    last_progress: SyncProgressAdvance | None = None

    def add_page(
        self,
        *,
        staged: StageEvidence,
        reconciled: StateReconcileEvidence | EventReconcileEvidence,
        synced: SyncPhaseEvidence,
        advanced: SyncProgressAdvance,
    ) -> None:
        self.page_count += 1
        self.stage_mode = staged.mode
        self.staged_row_count += staged.row_count
        self.safe_to_advance_collect_id = staged.safe_to_advance_collect_id
        self.operation_count += int(getattr(reconciled, "operation_count", 0) or 0)
        self.upsert_count += int(getattr(reconciled, "upsert_count", 0) or 0)
        self.remove_count += int(getattr(reconciled, "remove_count", 0) or 0)
        self.event_import_count += int(getattr(reconciled, "import_count", 0) or 0)
        if synced.status == "failed":
            self.sync_status = "failed"
        elif self.sync_status != "failed" and synced.status != "deferred":
            self.sync_status = "succeeded"
        self.irreversible_writes = self.irreversible_writes or synced.irreversible_writes
        self._add_submission(synced.submission)
        self._add_target_resolution(synced.target_resolution)
        self.destination_batch_count += synced.destination_batch_count
        self.destination_batch_ids.extend(batch.batch_id for batch in synced.destination_batches)
        self.delivery_decision_reason = str(getattr(synced.delivery_decision, "reason", ""))
        self.last_progress = advanced

    def _add_submission(
        self,
        submission: DestinationSubmissionEvidence,
    ) -> None:
        self.submission_status = submission.status
        self.request_batch_count += submission.request_batch_count
        self.attempted_count += submission.attempted_count
        self.confirmed_count += submission.confirmed_count
        self.accepted_count += submission.accepted_count
        self.retryable_failure_count += submission.retryable_failure_count
        self.terminal_failure_count += submission.terminal_record_failure_count
        self.pre_acceptance_failure_count += submission.pre_acceptance_failure_count
        if self.pre_acceptance_failure_category is None:
            self.pre_acceptance_failure_category = submission.pre_acceptance_failure_category
        if self.http_status is None:
            self.http_status = submission.http_status
        if self.partner_error_detail is None:
            self.partner_error_detail = submission.partner_error_detail
        if not self.submission_summary and _submission_has_failure(submission):
            self.submission_summary = submission.summary

    def _add_target_resolution(self, target_resolution: TargetResolutionEvidence | None) -> None:
        if target_resolution is None:
            return
        self.target_statuses.add(target_resolution.status)
        self.target_logical_names.update(
            resolved.logical_target for resolved in target_resolution.resolved
        )
        self.target_logical_names.update(target_resolution.missing)
        self.target_mapped_count += target_resolution.mapped_count
        self.target_registry_count += target_resolution.registry_count
        self.target_managed_created_count += target_resolution.managed_created_count
        self.target_planned_create_count += target_resolution.planned_create_count

    @property
    def target_resolution_status(self) -> str | None:
        if not self.target_statuses:
            return None
        if "failed" in self.target_statuses:
            return "failed"
        if "planned" in self.target_statuses:
            return "planned"
        return "resolved"

    @property
    def progress(self) -> SyncProgressAdvance:
        if self.last_progress is None:
            raise DeclarationValidationError("Sync execution produced no progress decision.")
        return self.last_progress


def _submission_has_failure(submission: DestinationSubmissionEvidence) -> bool:
    return (
        submission.status
        in {"retryable_failure", "terminal_record_failure", "pre_acceptance_failure"}
        or submission.retryable_failure_count > 0
        or submission.terminal_record_failure_count > 0
        or submission.pre_acceptance_failure_count > 0
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
    console_renderer = resolve_console(console)
    run_id: str | None = None
    store: RuntimeStore | None = None
    try:
        _validate_resend_all_runtime_syncs(syncs=syncs, resend_all=resend_all)
        store = runtime_store or configured_runtime_store()
        if store is None:
            raise DeclarationValidationError(
                "runner execution requires a runtime_store or configured runtime store."
            )
        groups = _collect_groups(syncs)
        surfaces = {sync.name: _validate_sync_for_runtime(sync) for sync in syncs}

        source_groups: list[SourceGroupResult] = []
        declaration_stages: list[DeclarationStageResult] = []
        sync_results: list[SyncResult] = []
        sync_reports: list[SyncReport] = []
        generated_run_id = str(uuid.uuid4())
        store.register_run(
            run_provenance(run_id=generated_run_id, runner_name=runner_name, dry_run=dry_run)
        )
        run_id = generated_run_id
        _log_info(
            "run_started",
            run_id=run_id,
            runner_name=runner_name,
            dry_run=dry_run,
            sync_count=len(syncs),
            collect_group_count=len(groups),
            resend_all=resend_all,
        )
        _emit_console(
            console_renderer,
            "run_started",
            run_id=run_id,
            runner_name=runner_name,
            dry_run=dry_run,
            sync_count=len(syncs),
            collect_group_count=len(groups),
        )

        for group in groups:
            attempt_states = _begin_sync_attempts(
                runner_name=runner_name,
                run_id=run_id,
                syncs=group.syncs,
                store=store,
                recovery_store=recovery_store,
                dry_run=dry_run,
            )
            _log_info(
                "phase_started",
                **_collect_context(
                    run_id=run_id,
                    group=group,
                    dry_run=dry_run,
                    status="started",
                ),
            )
            progress_by_scope = {
                state.progress.scope: state.progress for state in attempt_states.values()
            }
            try:
                collect = _produce_collect(
                    group=group,
                    store=store,
                    progress_by_scope=progress_by_scope,
                )
            except Exception as exc:
                _log_error(
                    "phase_failed",
                    **_collect_context(
                        run_id=run_id,
                        group=group,
                        dry_run=dry_run,
                        status="failed",
                    ),
                    **_exception_context(exc),
                )
                raise
            collect_phase = _collect_phase(collect, dry_run=dry_run)
            _log_info(
                "phase_completed",
                **_collect_context(
                    run_id=run_id,
                    group=group,
                    dry_run=dry_run,
                    status=collect_phase.status,
                ),
                work_row_count=int(getattr(collect, "work_row_count", 0) or 0),
                collect_id=getattr(collect, "collect_id", None),
            )
            _emit_console(
                console_renderer,
                "collect_group_completed",
                run_id=run_id,
                declaration_name=group.declaration.name,
                declaration_kind=_declaration_kind(group.declaration),
                source_group_key=group.key,
                collect_id=getattr(collect, "collect_id", None),
                work_row_count=int(getattr(collect, "work_row_count", 0) or 0),
                status=collect_phase.status,
                source_name=group.declaration.source.name,
                source_mode=group.declaration.source.mode,
            )
            source_groups.append(
                SourceGroupResult(
                    group_key=group.key,
                    source_name=group.declaration.source.name,
                    source_mode=group.declaration.source.mode,
                    sync_names=tuple(sync.name for sync in group.syncs),
                    phase=collect_phase,
                    source_window=getattr(collect, "scan_upper_bound", None),
                )
            )
            declaration_stages.append(
                DeclarationStageResult(
                    group_key=f"stage:{group.key}",
                    declaration_name=group.declaration.name,
                    declaration_kind=_declaration_kind(group.declaration),
                    source_group_key=group.key,
                    sync_names=tuple(sync.name for sync in group.syncs),
                    phase=PhaseStatus(
                        name="stage",
                        status="planned",
                        evidence=PhaseEvidence(
                            kind="planned",
                            message=(
                                f"Per-Sync pending staging planned for {len(group.syncs)} Sync(s)."
                            ),
                            dry_run=dry_run,
                        ),
                    ),
                )
            )

            for sync in group.syncs:
                result, report = _run_one_sync(
                    runner_name=runner_name,
                    run_id=run_id,
                    sync=sync,
                    surface=surfaces[sync.name],
                    attempt_state=attempt_states[sync.name],
                    progress_by_scope=progress_by_scope,
                    collect=collect,
                    collect_phase=collect_phase,
                    store=store,
                    recovery_store=recovery_store,
                    dry_run=dry_run,
                    stage_batch_max_rows=stage_batch_max_rows,
                    resend_all=resend_all,
                    console=console_renderer,
                )
                sync_results.append(result)
                sync_reports.append(report)
                _record_report(store=store, report=report)
                _emit_console(
                    console_renderer,
                    "sync_report_recorded",
                    run_id=report.run_id,
                    attempt_id=report.attempt_id,
                    sync_name=report.sync_name,
                    destination_binding_name=report.destination_binding_name,
                    surface=report.surface,
                    status=report.status,
                    report_reference=report.ref.ref,
                )

        status = _run_status(sync_results)
        run_index = build_run_index(
            runner_name=runner_name,
            run_id=run_id,
            status=status,
            dry_run=dry_run,
            source_groups=tuple(source_groups),
            declaration_stages=tuple(declaration_stages),
            reports=tuple(sync_reports),
        )
        store.complete_run(run_id=run_id, status=status)
        run_result = RunResult(
            runner_name=runner_name,
            status=status,
            dry_run=dry_run,
            source_groups=tuple(source_groups),
            declaration_stages=tuple(declaration_stages),
            syncs=tuple(sync_results),
            progress_advanced=any(result.progress_advanced for result in sync_results),
            irreversible_writes=any(result.irreversible_writes for result in sync_results),
            run_index_reference=run_index.ref.ref,
            report_references=tuple(report.ref.ref for report in sync_reports),
            run_index=run_index,
            sync_reports=tuple(sync_reports),
            run_id=run_id,
        )
        _log_info(
            "run_completed",
            run_id=run_id,
            runner_name=runner_name,
            status=status,
            dry_run=dry_run,
            sync_count=len(sync_results),
            report_references=run_result.report_references,
            run_index_reference=run_result.run_index_reference,
            progress_advanced=run_result.progress_advanced,
            irreversible_writes=run_result.irreversible_writes,
        )
        _emit_console(
            console_renderer,
            "run_completed",
            run_id=run_id,
            runner_name=runner_name,
            status=status,
            dry_run=dry_run,
            sync_count=len(sync_results),
            sync_succeeded_count=_sync_status_count(sync_results, "succeeded"),
            sync_failed_count=_sync_status_count(sync_results, "failed"),
            sync_partial_count=_sync_status_count(sync_results, "partial"),
            sync_planned_count=_sync_status_count(sync_results, "deferred"),
            confirmed_count=sum(result.destination_confirmed_count for result in sync_results),
            accepted_count=sum(result.destination_accepted_count for result in sync_results),
            retryable_failure_count=sum(
                result.destination_retryable_failure_count for result in sync_results
            ),
            terminal_failure_count=sum(
                result.destination_terminal_failure_count for result in sync_results
            ),
            pre_acceptance_failure_count=sum(
                result.destination_pre_acceptance_failure_count for result in sync_results
            ),
            progress_advanced=run_result.progress_advanced,
            run_index_reference=run_result.run_index_reference,
            report_references=run_result.report_references,
        )
        return run_result
    except Exception as exc:
        if run_id is not None and store is not None:
            try:
                store.complete_run(run_id=run_id, status="failed")
            except Exception as completion_exc:
                _log_error(
                    "run_completion_failed",
                    run_id=run_id,
                    runner_name=runner_name,
                    status="failed",
                    dry_run=dry_run,
                    **_exception_context(completion_exc),
                )
        _log_error(
            "run_failed",
            run_id=run_id,
            runner_name=runner_name,
            status="failed",
            dry_run=dry_run,
            sync_count=len(syncs),
            **_exception_context(exc),
        )
        _emit_console(
            console_renderer,
            "run_completed",
            run_id=run_id,
            runner_name=runner_name,
            status="failed",
            dry_run=dry_run,
            sync_count=len(syncs),
        )
        raise


def _validate_sync_for_runtime(sync: Sync) -> DestinationSurface:
    if not isinstance(sync.declaration, State | Event):
        raise DeclarationValidationError("Runner requires State or Event Sync declarations.")
    if not isinstance(sync.destination, DestinationBinding):
        raise DeclarationValidationError(
            f"Sync `{sync.name}` requires a DestinationBinding for runner execution."
        )
    surface = resolve_surface(sync.destination, sync.surface)
    try:
        compatibility = validate_surface_compatibility(sync=sync, surface=surface)
    except DestinationCompatibilityError as exc:
        _log_error(
            "destination_compatibility_checked",
            sync_name=sync.name,
            declaration_name=sync.declaration.name,
            declaration_kind=_declaration_kind(sync.declaration),
            destination_binding_name=_destination_binding_name(sync),
            surface=surface.name,
            status="invalid",
            surface_family=surface.declaration_family,
            execution_mode=surface.execution_mode,
            delivery_outcome=surface.delivery_outcome,
            exception_type=type(exc).__name__,
            exception_message=redact_text(str(exc)),
        )
        raise
    _log_info(
        "destination_compatibility_checked",
        sync_name=sync.name,
        declaration_name=sync.declaration.name,
        declaration_kind=_declaration_kind(sync.declaration),
        destination_binding_name=_destination_binding_name(sync),
        surface=surface.name,
        status="valid",
        surface_family=compatibility.family,
        execution_mode=surface.execution_mode,
        delivery_outcome=compatibility.delivery_outcome,
    )
    return surface


def _validate_resend_all_runtime_syncs(*, syncs: Sequence[Sync], resend_all: bool) -> None:
    if not isinstance(resend_all, bool):
        raise DeclarationValidationError("`resend_all` must be a boolean.")
    if not resend_all:
        return
    event_sync_names = [
        sync.name for sync in syncs if isinstance(getattr(sync, "declaration", None), Event)
    ]
    if event_sync_names:
        joined = ", ".join(event_sync_names)
        raise DeclarationValidationError(
            "`resend_all=True` is only valid for State runner execution; "
            f"Event Syncs cannot resend current State: {joined}."
        )


def _collect_groups(syncs: Sequence[Sync]) -> tuple[_CollectGroup, ...]:
    grouped: dict[str, list[Sync]] = {}
    declarations: dict[str, State | Event] = {}
    for sync in syncs:
        declaration = sync.declaration
        if not isinstance(declaration, State | Event):
            raise DeclarationValidationError("Runner requires State or Event Sync declarations.")
        key = _collect_group_key(declaration)
        if isinstance(declaration, Event):
            key = ":".join(
                (
                    key,
                    sync.name,
                    _destination_binding_name(sync) or "",
                    sync.surface,
                )
            )
        grouped.setdefault(key, []).append(sync)
        declarations[key] = declaration
    return tuple(
        _CollectGroup(key=key, declaration=declarations[key], syncs=tuple(grouped[key]))
        for key in grouped
    )


def _collect_group_key(declaration: State | Event) -> str:
    return ":".join(
        (
            _declaration_kind(declaration),
            declaration.name,
            declaration.source.name,
            source_identity(declaration.source),
        )
    )


def _produce_collect(
    *,
    group: _CollectGroup,
    store: RuntimeStore,
    progress_by_scope: dict[DestinationProgressScope, DestinationProgress],
) -> CollectEvidence:
    if isinstance(group.declaration, State):
        return produce_state_collect(declaration=group.declaration, store=store)
    if isinstance(group.declaration, Event):
        if len(group.syncs) != 1:
            raise DeclarationValidationError(
                "Event collection is destination-scoped by scan cursor."
            )
        scope = destination_progress_scope(group.syncs[0])
        progress = progress_by_scope.get(scope)
        position = (
            progress.position
            if progress is not None
            else store.get_destination_progress(scope).position
        )
        if position is not None and not isinstance(position, EventKeysetScanPosition):
            raise DeclarationValidationError(
                "Event destination progress must be a keyset position."
            )
        return EventCollectEvidence(
            phase="collect",
            status="completed",
            collect_id=None,
            declaration_name=group.declaration.name,
            source_name=group.declaration.source.name,
            scan_after=position,
            scan_upper_bound=None,
            window_row_count=0,
            work_row_count=0,
            import_count=0,
            duplicate_risk_count=0,
        )
    raise DeclarationValidationError("Collect requires a State or Event declaration.")


def _begin_sync_attempts(
    *,
    runner_name: str,
    run_id: str,
    syncs: Sequence[Sync],
    store: RuntimeStore,
    recovery_store: RecoveryStore,
    dry_run: bool,
) -> dict[str, _SyncAttemptState]:
    attempts: dict[str, _SyncAttemptState] = {}
    for sync in syncs:
        progress = register_destination_progress(store=store, sync=sync)
        attempt = recovery_store.begin_attempt(
            runner_name=runner_name,
            sync_name=sync.name,
            dry_run=dry_run,
        )
        retry_destination_batches(
            sync=sync,
            runtime_store=store,
            run_id=run_id,
            attempt_id=attempt.attempt_id,
            dry_run=dry_run,
        )
        attempts[sync.name] = _SyncAttemptState(attempt=attempt, progress=progress)
    return attempts


def _run_one_sync(
    *,
    runner_name: str,
    run_id: str,
    sync: Sync,
    surface: DestinationSurface,
    attempt_state: _SyncAttemptState,
    progress_by_scope: dict[DestinationProgressScope, DestinationProgress],
    collect: CollectEvidence,
    collect_phase: PhaseStatus,
    store: RuntimeStore,
    recovery_store: RecoveryStore,
    dry_run: bool,
    stage_batch_max_rows: int,
    resend_all: bool,
    console: ConsoleRenderer,
) -> tuple[SyncResult, SyncReport]:
    attempt = attempt_state.attempt
    progress = attempt_state.progress
    _log_info(
        "sync_started",
        **_sync_context(
            run_id=run_id,
            attempt_id=attempt.attempt_id,
            sync=sync,
            surface=surface,
            dry_run=dry_run,
            status="started",
        ),
        resend_all=resend_all,
        delivery_outcome=surface.delivery_outcome,
    )
    _emit_console(
        console,
        "sync_started",
        run_id=run_id,
        attempt_id=attempt.attempt_id,
        sync_name=sync.name,
        destination_binding_name=_destination_binding_name(sync),
        surface=surface.name,
        declaration_kind=_declaration_kind(sync.declaration),
        declaration_name=sync.declaration.name,
        dry_run=dry_run,
    )
    try:
        execution = _execute_sync_pages(
            runner_name=runner_name,
            run_id=run_id,
            sync=sync,
            surface=surface,
            store=store,
            collect=collect,
            attempt_id=attempt.attempt_id,
            progress=progress,
            recovery_store=recovery_store,
            dry_run=dry_run,
            stage_batch_max_rows=stage_batch_max_rows,
            resend_all=resend_all,
            console=console,
        )
        advanced = execution.progress
        progress_by_scope[advanced.progress.scope] = DestinationProgress(
            scope=advanced.progress.scope,
            position=advanced.progress.after,
        )
        sync_allowed = execution.sync_status != "failed"
        status: Literal["completed", "failed"] = "completed" if sync_allowed else "failed"
        if dry_run:
            status = "completed"
        recovery_store.complete_attempt(attempt_id=attempt.attempt_id, status=status)

        report_reference = sync_report_ref(runner_name=runner_name, sync_name=sync.name)
        stage_phase = _stage_phase(execution=execution)
        reconcile_phase = _reconcile_phase(execution=execution)
        sync_phase = _sync_phase(execution=execution)
        result = _sync_result(
            sync=sync,
            dry_run=dry_run,
            attempt_id=attempt.attempt_id,
            report_reference=report_reference.ref,
            collect=collect_phase,
            stage=stage_phase,
            reconcile=reconcile_phase,
            sync_phase=sync_phase,
            execution=execution,
            advanced=advanced,
            surface=surface,
        )
        report = _sync_report(
            runner_name=runner_name,
            report_reference=report_reference,
            run_id=run_id,
            attempt_id=attempt.attempt_id,
            sync=sync,
            surface=surface,
            dry_run=dry_run,
            collect=collect_phase,
            stage=stage_phase,
            reconcile=reconcile_phase,
            sync_phase=sync_phase,
            execution=execution,
            retention_watermark=store.retention_watermark(
                family=destination_progress_scope(sync).family,
                declaration_name=sync.declaration.name,
                progress_positions=tuple(
                    progress.position
                    for progress in progress_by_scope.values()
                    if progress.scope.family == destination_progress_scope(sync).family
                    and progress.scope.declaration_name == sync.declaration.name
                ),
            ),
        )
        _log_info(
            "sync_completed",
            **_sync_context(
                run_id=run_id,
                attempt_id=attempt.attempt_id,
                sync=sync,
                surface=surface,
                dry_run=dry_run,
                status=status,
            ),
            operation_count=result.operation_count,
            event_import_count=result.event_import_count,
            destination_batch_count=result.destination_batch_count,
            confirmed_count=result.destination_confirmed_count,
            accepted_count=result.destination_accepted_count,
            retryable_failure_count=result.destination_retryable_failure_count,
            terminal_failure_count=result.destination_terminal_failure_count,
            pre_acceptance_failure_count=result.destination_pre_acceptance_failure_count,
            progress_advanced=result.progress_advanced,
            report_reference=result.report_reference,
        )
        _emit_console(
            console,
            "sync_completed",
            run_id=run_id,
            attempt_id=attempt.attempt_id,
            sync_name=sync.name,
            destination_binding_name=_destination_binding_name(sync),
            surface=surface.name,
            status=result.sync.status,
            operation_count=result.operation_count,
            upsert_count=result.upsert_count,
            remove_count=result.remove_count,
            event_import_count=result.event_import_count,
            destination_batch_count=result.destination_batch_count,
            confirmed_count=result.destination_confirmed_count,
            accepted_count=result.destination_accepted_count,
            retryable_failure_count=result.destination_retryable_failure_count,
            terminal_failure_count=result.destination_terminal_failure_count,
            pre_acceptance_failure_count=result.destination_pre_acceptance_failure_count,
            progress_advanced=result.progress_advanced,
            report_reference=result.report_reference,
        )
        return result, report
    except Exception as exc:
        _log_error(
            "sync_failed",
            **_sync_context(
                run_id=run_id,
                attempt_id=attempt.attempt_id,
                sync=sync,
                surface=surface,
                dry_run=dry_run,
                status="failed",
            ),
            **_exception_context(exc),
        )
        raise


def _execute_sync_pages(
    *,
    runner_name: str,
    run_id: str,
    sync: Sync,
    surface: DestinationSurface,
    store: RuntimeStore,
    collect: CollectEvidence,
    attempt_id: str,
    progress: DestinationProgress,
    recovery_store: RecoveryStore,
    dry_run: bool,
    stage_batch_max_rows: int,
    resend_all: bool,
    console: ConsoleRenderer,
) -> _SyncExecutionAccumulator:
    cursor: PendingWorkCursor | StateCurrentCursor | EventSourceCursor | None = None
    current_progress = progress
    resolved_target_mappings: tuple[TargetMapping, ...] = ()
    execution = _SyncExecutionAccumulator(sync=sync, surface=surface, dry_run=dry_run)
    while True:
        page_index = execution.page_count + 1
        _log_info(
            "phase_started",
            **_phase_context(
                run_id=run_id,
                attempt_id=attempt_id,
                sync=sync,
                surface=surface,
                dry_run=dry_run,
                phase="stage",
                status="started",
                page_index=page_index,
            ),
            mode="resend_all" if resend_all else "pending",
            max_rows=stage_batch_max_rows,
        )
        try:
            staged = _stage_sync_page(
                sync=sync,
                store=store,
                max_rows=stage_batch_max_rows,
                cursor=cursor,
                collect=collect,
                progress=current_progress,
                dry_run=dry_run,
                resend_all=resend_all,
            )
        except Exception as exc:
            _log_error(
                "phase_failed",
                **_phase_context(
                    run_id=run_id,
                    attempt_id=attempt_id,
                    sync=sync,
                    surface=surface,
                    dry_run=dry_run,
                    phase="stage",
                    status="failed",
                    page_index=page_index,
                ),
                **_exception_context(exc),
            )
            raise
        _log_info(
            "phase_completed",
            **_phase_context(
                run_id=run_id,
                attempt_id=attempt_id,
                sync=sync,
                surface=surface,
                dry_run=dry_run,
                phase="stage",
                status=staged.phase_status.status,
                page_index=page_index,
            ),
            mode=staged.mode,
            row_count=staged.row_count,
            progress_before=staged.progress_before,
            safe_to_advance_collect_id=staged.safe_to_advance_collect_id,
        )
        _emit_console(
            console,
            "stage_completed",
            run_id=run_id,
            attempt_id=attempt_id,
            sync_name=sync.name,
            destination_binding_name=_destination_binding_name(sync),
            surface=surface.name,
            phase="stage",
            status=staged.phase_status.status,
            mode=staged.mode,
            row_count=staged.row_count,
            page_index=page_index,
            progress_before=staged.progress_before,
            safe_to_advance_collect_id=staged.safe_to_advance_collect_id,
        )

        _log_info(
            "phase_started",
            **_phase_context(
                run_id=run_id,
                attempt_id=attempt_id,
                sync=sync,
                surface=surface,
                dry_run=dry_run,
                phase="reconcile",
                status="started",
                page_index=page_index,
            ),
            staged_row_count=staged.row_count,
            mode=staged.mode,
        )
        try:
            reconciled = _reconcile_sync_page(sync=sync, staged=staged, dry_run=dry_run)
        except Exception as exc:
            _log_error(
                "phase_failed",
                **_phase_context(
                    run_id=run_id,
                    attempt_id=attempt_id,
                    sync=sync,
                    surface=surface,
                    dry_run=dry_run,
                    phase="reconcile",
                    status="failed",
                    page_index=page_index,
                ),
                **_exception_context(exc),
            )
            raise
        _log_info(
            "phase_completed",
            **_phase_context(
                run_id=run_id,
                attempt_id=attempt_id,
                sync=sync,
                surface=surface,
                dry_run=dry_run,
                phase="reconcile",
                status=reconciled.phase_status.status,
                page_index=page_index,
            ),
            operation_count=int(getattr(reconciled, "operation_count", 0) or 0),
            event_import_count=int(getattr(reconciled, "import_count", 0) or 0),
            upsert_count=int(getattr(reconciled, "upsert_count", 0) or 0),
            remove_count=int(getattr(reconciled, "remove_count", 0) or 0),
            page_count=len(
                getattr(reconciled, "operation_pages", ())
                or getattr(reconciled, "import_pages", ())
            ),
        )
        _emit_console(
            console,
            "reconcile_completed",
            run_id=run_id,
            attempt_id=attempt_id,
            sync_name=sync.name,
            destination_binding_name=_destination_binding_name(sync),
            surface=surface.name,
            phase="reconcile",
            status=reconciled.phase_status.status,
            operation_count=int(getattr(reconciled, "operation_count", 0) or 0),
            event_import_count=int(getattr(reconciled, "import_count", 0) or 0),
            upsert_count=int(getattr(reconciled, "upsert_count", 0) or 0),
            remove_count=int(getattr(reconciled, "remove_count", 0) or 0),
            page_count=len(
                getattr(reconciled, "operation_pages", ())
                or getattr(reconciled, "import_pages", ())
            ),
            page_index=page_index,
        )

        _log_info(
            "phase_started",
            **_phase_context(
                run_id=run_id,
                attempt_id=attempt_id,
                sync=sync,
                surface=surface,
                dry_run=dry_run,
                phase="sync",
                status="started",
                page_index=page_index,
            ),
        )
        try:
            synced = sync_destination(
                sync=sync,
                reconciled=reconciled,
                dry_run=dry_run,
                runtime_store=store,
                run_id=run_id,
                attempt_id=attempt_id,
                page_index=page_index,
                resolved_target_mappings=resolved_target_mappings,
                console=console,
            )
            resolved_target_mappings = _merge_target_mappings(
                resolved_target_mappings,
                synced.resolved_target_mappings,
            )
            _emit_console(
                console,
                "destination_submission_completed",
                run_id=run_id,
                attempt_id=attempt_id,
                sync_name=sync.name,
                destination_binding_name=_destination_binding_name(sync),
                surface=surface.name,
                status=synced.submission.status,
                page_index=page_index,
                request_batch_count=synced.submission.request_batch_count,
                destination_batch_count=synced.destination_batch_count,
                attempted_count=synced.submission.attempted_count,
                confirmed_count=synced.submission.confirmed_count,
                accepted_count=synced.submission.accepted_count,
                retryable_failure_count=synced.submission.retryable_failure_count,
                terminal_failure_count=synced.submission.terminal_record_failure_count,
                pre_acceptance_failure_count=synced.submission.pre_acceptance_failure_count,
                progress_decision_allowed=synced.progress_decision.allowed,
            )
            advanced = advance_progress_after_sync(
                store=store,
                sync=sync,
                surface=surface,
                reconciled=reconciled,
                evidence=synced.submission,
                dry_run=dry_run,
                destination_batches=synced.destination_batches,
                current_position=current_progress.position,
                current_position_loaded=True,
                run_id=run_id,
                attempt_id=attempt_id,
                page_index=page_index,
            )
            current_progress = DestinationProgress(
                scope=advanced.progress.scope,
                position=advanced.progress.after,
            )
            _emit_console(
                console,
                "progress_commit_decided",
                run_id=run_id,
                attempt_id=attempt_id,
                sync_name=sync.name,
                destination_binding_name=_destination_binding_name(sync),
                surface=surface.name,
                status="allowed" if advanced.progress_decision.allowed else "blocked",
                progress_advanced=advanced.progress.advanced,
                progress_decision_allowed=advanced.progress_decision.allowed,
                reason=advanced.progress_decision.reason,
                page_index=page_index,
                planned_batch_count=len(synced.destination_batches),
                expected_batch_count=synced.submission.request_batch_count,
            )
        except Exception as exc:
            _log_error(
                "phase_failed",
                **_phase_context(
                    run_id=run_id,
                    attempt_id=attempt_id,
                    sync=sync,
                    surface=surface,
                    dry_run=dry_run,
                    phase="sync",
                    status="failed",
                    page_index=page_index,
                ),
                **_exception_context(exc),
            )
            raise
        _log_info(
            "phase_completed",
            **_phase_context(
                run_id=run_id,
                attempt_id=attempt_id,
                sync=sync,
                surface=surface,
                dry_run=dry_run,
                phase="sync",
                status=synced.phase_status.status,
                page_index=page_index,
            ),
            destination_batch_count=synced.destination_batch_count,
            confirmed_count=synced.submission.confirmed_count,
            accepted_count=synced.submission.accepted_count,
            retryable_failure_count=synced.submission.retryable_failure_count,
            terminal_failure_count=synced.submission.terminal_record_failure_count,
            pre_acceptance_failure_count=synced.submission.pre_acceptance_failure_count,
            progress_advanced=advanced.progress.advanced,
            progress_decision_allowed=advanced.progress_decision.allowed,
        )
        execution.add_page(
            staged=staged,
            reconciled=reconciled,
            synced=synced,
            advanced=advanced,
        )
        _record_submission_evidence(
            recovery_store=recovery_store,
            sync=sync,
            attempt_id=attempt_id,
            advanced=advanced,
        )
        if staged.next_cursor is None:
            return execution
        if not dry_run:
            page_continuation = decide_page_continuation(
                on_failure=sync.on_failure,
                submission=synced.submission,
                progress_allowed=advanced.progress_decision.allowed,
            )
            if not page_continuation.allowed:
                return execution
        if resend_all:
            cursor = cast(StateCurrentCursor, staged.next_cursor)
        else:
            cursor = cast(PendingWorkCursor | EventSourceCursor, staged.next_cursor)


def _stage_sync_page(
    *,
    sync: Sync,
    store: RuntimeStore,
    max_rows: int,
    cursor: PendingWorkCursor | StateCurrentCursor | EventSourceCursor | None,
    collect: CollectEvidence,
    progress: DestinationProgress,
    dry_run: bool,
    resend_all: bool,
) -> StageEvidence:
    if resend_all:
        if not isinstance(sync.declaration, State):
            raise DeclarationValidationError(
                "`resend_all=True` is only valid for State runner execution."
            )
        return stage_resend_all(
            sync=sync,
            store=store,
            max_rows=max_rows,
            cursor=cast(StateCurrentCursor | None, cursor),
            progress=progress,
            dry_run=dry_run,
        )
    if isinstance(sync.declaration, State):
        return stage_declaration(
            sync=sync,
            store=store,
            max_rows=max_rows,
            cursor=cast(PendingWorkCursor | None, cursor),
            progress=progress,
            dry_run=dry_run,
        )
    return stage_event_declaration(
        sync=sync,
        store=store,
        max_rows=max_rows,
        cursor=cast(EventSourceCursor | None, cursor),
        source_collect_id=collect.collect_id,
        progress=progress,
        dry_run=dry_run,
    )


def _reconcile_sync_page(
    *,
    sync: Sync,
    staged: StageEvidence,
    dry_run: bool,
) -> StateReconcileEvidence | EventReconcileEvidence:
    if isinstance(sync.declaration, State):
        return reconcile_sync(sync=sync, staged=staged, dry_run=dry_run)
    return reconcile_event_imports(sync=sync, staged=staged, dry_run=dry_run)


def _merge_target_mappings(
    current: Sequence[TargetMapping],
    learned: Sequence[TargetMapping],
) -> tuple[TargetMapping, ...]:
    if not learned:
        return tuple(current)
    merged = list(current)
    existing = {(mapping.surface, mapping.logical_target) for mapping in current}
    for mapping in learned:
        key = (mapping.surface, mapping.logical_target)
        if key in existing:
            continue
        merged.append(mapping)
        existing.add(key)
    return tuple(merged)


def _stage_phase(*, execution: _SyncExecutionAccumulator) -> PhaseStatus:
    return PhaseStatus(
        name="stage",
        status="succeeded",
        evidence=PhaseEvidence(
            kind="planned",
            message=(
                f"Staged {execution.staged_row_count} row(s) across "
                f"{execution.page_count} {execution.stage_mode} page(s)."
            ),
            dry_run=execution.dry_run,
        ),
    )


def _reconcile_phase(*, execution: _SyncExecutionAccumulator) -> PhaseStatus:
    unit = "State operation" if isinstance(execution.sync.declaration, State) else "Event import"
    count = execution.operation_count or execution.event_import_count
    return PhaseStatus(
        name="reconcile",
        status="succeeded",
        evidence=PhaseEvidence(
            kind="planned",
            message=f"Reconciled {count} {unit} row(s) across {execution.page_count} page(s).",
            dry_run=execution.dry_run,
        ),
    )


def _sync_phase(*, execution: _SyncExecutionAccumulator) -> PhaseStatus:
    advanced = execution.progress
    return PhaseStatus(
        name="sync",
        status=execution.sync_status,
        evidence=PhaseEvidence(
            kind="planned",
            message=(
                "Destination batch ledger evaluated "
                f"{execution.request_batch_count} request batch(es) across "
                f"{execution.page_count} page(s)."
            ),
            dry_run=execution.dry_run,
            irreversible_writes=execution.irreversible_writes,
            progress_advanced=advanced.progress.advanced,
        ),
    )


def _sync_report(
    *,
    runner_name: str,
    report_reference: ReportReference,
    run_id: str,
    attempt_id: str,
    sync: Sync,
    surface: DestinationSurface,
    dry_run: bool,
    collect: PhaseStatus,
    stage: PhaseStatus,
    reconcile: PhaseStatus,
    sync_phase: PhaseStatus,
    execution: _SyncExecutionAccumulator,
    retention_watermark: str | None,
) -> SyncReport:
    advanced = execution.progress
    metadata = declaration_metadata(sync.declaration)
    progress_reason = str(getattr(advanced.progress_decision, "reason", "") or "")
    return SyncReport(
        ref=report_reference,
        report_id=sync_report_id(run_id=run_id, attempt_id=attempt_id, sync_name=sync.name),
        runner_name=runner_name,
        run_id=run_id,
        attempt_id=attempt_id,
        sync_name=sync.name,
        declaration_name=sync.declaration.name,
        declaration_version_id=metadata.declaration_version_id,
        declaration_kind=_declaration_kind(sync.declaration),
        destination_binding_name=_destination_binding_name(sync),
        surface=sync.surface,
        dry_run=dry_run,
        status=_report_status_from_sync_status(execution.sync_status),
        on_failure=sync.on_failure,
        phases=tuple(phase_summary(phase) for phase in (collect, stage, reconcile, sync_phase)),
        reconcile=ReconcileSummary(
            operation_count=execution.operation_count,
            upsert_count=execution.upsert_count,
            remove_count=execution.remove_count,
            event_import_count=execution.event_import_count,
        ),
        destination=DestinationSummary(
            binding_name=_destination_binding_name(sync),
            surface=sync.surface,
            surface_family=surface.declaration_family,
            execution_mode=surface.execution_mode,
            compatibility_status="valid",
            target_resolution_status=execution.target_resolution_status,
            target_count=len(execution.target_logical_names),
            target_mapped_count=execution.target_mapped_count,
            target_registry_count=execution.target_registry_count,
            target_managed_created_count=execution.target_managed_created_count,
            target_planned_create_count=execution.target_planned_create_count,
            delivery_outcome=surface.delivery_outcome,
            submission_status=execution.submission_status,
            delivery_decision_reason=execution.delivery_decision_reason,
            request_batch_count=execution.request_batch_count,
            destination_batch_count=execution.destination_batch_count,
            destination_batch_ids=tuple(execution.destination_batch_ids),
            attempted_count=execution.attempted_count,
            confirmed_count=execution.confirmed_count,
            accepted_count=execution.accepted_count,
            retryable_failure_count=execution.retryable_failure_count,
            terminal_failure_count=execution.terminal_failure_count,
            pre_acceptance_failure_count=execution.pre_acceptance_failure_count,
            failure_category=execution.pre_acceptance_failure_category,
            http_status=execution.http_status,
            last_error_summary=execution.submission_summary,
            last_error_detail=execution.partner_error_detail,
        ),
        progress=ProgressSummary(
            scope=progress_scope_summary(advanced.progress.scope),
            stage_mode=execution.stage_mode,
            page_count=execution.page_count,
            staged_row_count=execution.staged_row_count,
            safe_to_advance_collect_id=execution.safe_to_advance_collect_id,
            before=scan_position_summary(advanced.progress.before),
            after=scan_position_summary(advanced.progress.after),
            advanced=advanced.progress.advanced,
            decision_allowed=advanced.progress_decision.allowed,
            decision_reason=progress_reason,
        ),
        failures=FailureSummary(
            retryable_count=execution.retryable_failure_count,
            terminal_count=execution.terminal_failure_count,
            pre_acceptance_count=execution.pre_acceptance_failure_count,
            on_failure=sync.on_failure,
            decision="allowed" if advanced.progress_decision.allowed else "blocked",
            category=execution.pre_acceptance_failure_category,
        ),
        commit=CommitSummary(
            progress_advanced=advanced.progress.advanced,
            reason=_commit_reason(dry_run=dry_run, advanced=advanced, fallback=progress_reason),
        ),
        retention=RetentionSummary(watermark_collect_id=retention_watermark),
    )


def _commit_reason(
    *,
    dry_run: bool,
    advanced: SyncProgressAdvance,
    fallback: str,
) -> str:
    if dry_run:
        return "dry run reports planned work but does not commit runtime progress."
    if fallback:
        return fallback
    if advanced.progress.advanced:
        return "destination evidence allowed runtime progress to advance."
    return "destination submission and progress commits are deferred."


def _report_status_from_sync_status(status: str) -> str:
    if status == "failed":
        return "failed"
    if status == "deferred":
        return "planned"
    return "succeeded"


def _sync_result(
    *,
    sync: Sync,
    dry_run: bool,
    attempt_id: str,
    report_reference: str,
    collect: PhaseStatus,
    stage: PhaseStatus,
    reconcile: PhaseStatus,
    sync_phase: PhaseStatus,
    execution: _SyncExecutionAccumulator,
    advanced: SyncProgressAdvance,
    surface: DestinationSurface,
) -> SyncResult:
    metadata = declaration_metadata(sync.declaration)
    return SyncResult(
        sync_name=sync.name,
        declaration_name=sync.declaration.name,
        declaration_kind=_declaration_kind(sync.declaration),
        destination_binding_name=_destination_binding_name(sync),
        surface=sync.surface,
        dry_run=dry_run,
        attempt_id=attempt_id,
        report_reference=report_reference,
        collect=collect,
        stage=stage,
        reconcile=reconcile,
        sync=sync_phase,
        progress_advanced=advanced.progress.advanced,
        irreversible_writes=execution.irreversible_writes,
        operation_count=execution.operation_count,
        upsert_count=execution.upsert_count,
        remove_count=execution.remove_count,
        event_import_count=execution.event_import_count,
        duplicate_risk_notes=(),
        destination_surface_family=surface.declaration_family,
        destination_surface_execution_mode=surface.execution_mode,
        destination_batch_count=execution.destination_batch_count,
        destination_compatibility_status="valid",
        target_resolution_status=execution.target_resolution_status,
        target_count=len(execution.target_logical_names),
        target_mapped_count=execution.target_mapped_count,
        target_registry_count=execution.target_registry_count,
        target_managed_created_count=execution.target_managed_created_count,
        target_planned_create_count=execution.target_planned_create_count,
        progress_decision=advanced.progress_decision,
        destination_submission_status=execution.submission_status,
        destination_confirmed_count=execution.confirmed_count,
        destination_accepted_count=execution.accepted_count,
        destination_retryable_failure_count=execution.retryable_failure_count,
        destination_terminal_failure_count=execution.terminal_failure_count,
        destination_pre_acceptance_failure_count=execution.pre_acceptance_failure_count,
        destination_pre_acceptance_failure_category=execution.pre_acceptance_failure_category,
        destination_remote_handles=(),
        delivery_decision_reason=execution.delivery_decision_reason,
        declaration_version_id=metadata.declaration_version_id,
    )


def _record_submission_evidence(
    *,
    recovery_store: RecoveryStore,
    sync: Sync,
    attempt_id: str,
    advanced: SyncProgressAdvance,
) -> None:
    recovery_store.record_commit_decision(
        CommitDecisionRecord(
            attempt_id=attempt_id,
            sync_name=sync.name,
            progress_advanced=advanced.progress.advanced,
            reason=advanced.progress_decision.reason,
        )
    )


def _collect_phase(collect: CollectEvidence, *, dry_run: bool) -> PhaseStatus:
    return PhaseStatus(
        name="collect",
        status="succeeded",
        evidence=PhaseEvidence(
            kind="planned",
            message=(
                f"Collected {getattr(collect, 'work_row_count', 0)} "
                f"ordered work row(s) for `{collect.declaration_name}`."
            ),
            dry_run=dry_run,
        ),
    )


def _declaration_kind(declaration: State | Event) -> Literal["state", "event"]:
    return "state" if isinstance(declaration, State) else "event"


def _destination_binding_name(sync: Sync) -> str | None:
    if isinstance(sync.destination, DestinationBinding):
        return sync.destination.binding_name
    return None


def _run_status(sync_results: Sequence[SyncResult]) -> RunStatus:
    if all(result.sync.status == "deferred" for result in sync_results):
        return "planned"
    if all(result.sync.status == "succeeded" for result in sync_results):
        return "succeeded"
    if any(result.sync.status in {"succeeded", "deferred"} for result in sync_results):
        return "partial"
    return "failed"


def _sync_status_count(sync_results: Sequence[SyncResult], status: str) -> int:
    return sum(1 for result in sync_results if result.sync.status == status)


def _record_report(*, store: RuntimeStore, report: SyncReport) -> None:
    store.record_sync_report(report)
    _log_info(
        "sync_report_recorded",
        run_id=report.run_id,
        attempt_id=report.attempt_id,
        sync_name=report.sync_name,
        declaration_name=report.declaration_name,
        declaration_kind=report.declaration_kind,
        destination_binding_name=report.destination_binding_name,
        surface=report.surface,
        status=report.status,
        report_reference=report.ref.ref,
    )


def _log_info(event: str, **context: object) -> None:
    LOGGER.info(event, extra=_log_extra(event, **context))


def _log_error(event: str, **context: object) -> None:
    LOGGER.error(event, extra=_log_extra(event, **context))


def _emit_console(console: ConsoleRenderer, method_name: str, **context: object) -> None:
    try:
        emit = getattr(console, method_name)
        emit(**{key: value for key, value in context.items() if value is not None})
    except Exception:
        return


def _log_extra(event: str, **context: object) -> dict[str, object]:
    extra: dict[str, object] = {"event": event}
    for key, value in context.items():
        if value is not None:
            extra[key] = value
    return extra


def _collect_context(
    *,
    run_id: str,
    group: _CollectGroup,
    dry_run: bool,
    status: str,
) -> dict[str, object]:
    declaration = group.declaration
    return {
        "run_id": run_id,
        "declaration_name": declaration.name,
        "declaration_kind": _declaration_kind(declaration),
        "phase": "collect",
        "status": status,
        "dry_run": dry_run,
        "sync_count": len(group.syncs),
    }


def _sync_context(
    *,
    run_id: str,
    attempt_id: str,
    sync: Sync,
    surface: DestinationSurface,
    dry_run: bool,
    status: str,
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "attempt_id": attempt_id,
        "sync_name": sync.name,
        "declaration_name": sync.declaration.name,
        "declaration_kind": _declaration_kind(sync.declaration),
        "destination_binding_name": _destination_binding_name(sync),
        "surface": surface.name,
        "status": status,
        "dry_run": dry_run,
    }


def _phase_context(
    *,
    run_id: str,
    attempt_id: str,
    sync: Sync,
    surface: DestinationSurface,
    dry_run: bool,
    phase: str,
    status: str,
    page_index: int,
) -> dict[str, object]:
    context = _sync_context(
        run_id=run_id,
        attempt_id=attempt_id,
        sync=sync,
        surface=surface,
        dry_run=dry_run,
        status=status,
    )
    context["phase"] = phase
    context["page_index"] = page_index
    return context


def _exception_context(exc: Exception) -> dict[str, object]:
    return {
        "exception_type": type(exc).__name__,
        "exception_message": _safe_exception_message(exc),
    }


def _safe_exception_message(exc: Exception) -> str:
    if isinstance(exc, DestinationCompatibilityError):
        return "Destination compatibility validation failed."
    return redact_text(str(exc))


__all__ = ["run_syncs"]
