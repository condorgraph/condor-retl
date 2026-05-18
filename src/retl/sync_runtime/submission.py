from __future__ import annotations

import json
import logging
import math
import random
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeGuard, cast

import pyarrow as pa  # type: ignore[import-untyped]

from retl.artifacts.arrow_ipc import iter_columnar_batches
from retl.artifacts.columnar import ColumnarArtifactRef
from retl.auth import (
    AuthMode,
    JwtSigner,
    SecretResolver,
    TokenTransport,
    redacted_auth_evidence,
    resolve_auth,
    select_auth_mode,
)
from retl.collect_identity import is_uuidv7
from retl.config import configured_secret_resolver
from retl.console import ConsoleRenderer
from retl.declarations import (
    CredentialValue,
    DestinationBinding,
    Event,
    FailureHandlingMode,
    State,
    Sync,
)
from retl.declarations.provenance import declaration_metadata
from retl.destinations.acknowledgements import (
    DeliveryEvidenceDecision,
    DestinationSubmissionEvidence,
    evaluate_delivery_outcome,
)
from retl.destinations.builtins.mock import submit_mock_destination
from retl.destinations.builtins.reference import submit_reference_destination
from retl.destinations.compatibility import (
    DestinationCompatibility,
    DestinationCompatibilityError,
    validate_surface_compatibility,
)
from retl.destinations.receipts import sanitize_partner_error_detail
from retl.destinations.request_batch import (
    DryRunSubmissionPlan,
    RequestBatchPlan,
)
from retl.destinations.resolver import resolve_connector, resolve_surface
from retl.destinations.surfaces import DestinationSurface
from retl.destinations.targets import (
    TargetMapping,
    TargetResolutionEvidence,
    TargetResolutionFailure,
    resolve_targets,
)
from retl.destinations.terminal_failures import (
    CommitDecision,
    DestinationSyncEvidence,
    decide_progress_commit,
    decide_request_batch_continuation,
    pre_acceptance_failure_retryable,
)
from retl.errors import DeclarationValidationError
from retl.events.reconcile import EventReconcileEvidence
from retl.runtime.redaction import redact_text
from retl.runtime.results import PhaseEvidence, PhaseStatus
from retl.stores.contracts import (
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    EventKeysetScanPosition,
    EventSourceWindowRequest,
    EventSourceWindowSource,
    RuntimeStore,
    destination_batch_id,
)
from retl.sync_runtime.destination_batch_outcomes import classify_destination_batch_outcomes

if TYPE_CHECKING:
    from retl.state_runtime.reconcile import StateReconcileEvidence
else:
    StateReconcileEvidence = object

LOGGER = logging.getLogger("retl.sync_runtime.submission")


class _StateReconcileEvidenceLike(Protocol):
    phase: Literal["reconcile"]
    operation_pages: object


@dataclass(frozen=True)
class _DestinationBatchLedgerPlan:
    record: DestinationBatchRecord
    request_batch: RequestBatchPlan
    row_count: int


@dataclass(frozen=True)
class _DestinationBatchLedgerPlanning:
    plans: tuple[_DestinationBatchLedgerPlan, ...]
    request_batches: tuple[RequestBatchPlan, ...] | None = None


@dataclass(frozen=True)
class _DestinationBatchRerunPlan:
    current: tuple[_DestinationBatchLedgerPlan, ...]
    selected: tuple[_DestinationBatchLedgerPlan, ...]
    skipped: tuple[DestinationBatchRecord, ...]
    blocked: tuple[DestinationBatchRecord, ...]
    summary: str = ""


_RetryReconcileGroupKey = tuple[str, int | None, int | None]


_DEFAULT_DESTINATION_BATCH_RETRY_LIMIT = 3
_DEFAULT_IN_RUN_RETRY_ATTEMPT_LIMIT = 2
_DEFAULT_IN_RUN_RETRY_MAX_RETRY_AFTER_SECONDS = 5.0
_DEFAULT_IN_RUN_RETRY_SLEEP_BUDGET_SECONDS = 10.0
_DEFAULT_IN_RUN_RETRY_BASE_BACKOFF_SECONDS = 0.25
_DEFAULT_IN_RUN_RETRY_MAX_BACKOFF_SECONDS = 2.0
_DESTINATION_BATCH_STATUS_FLUSH_THRESHOLD = 10
_DEFAULT_IN_RUN_RETRY_JITTER_RATIO = 0.2
_sleep = time.sleep
_random = random.random


@dataclass(frozen=True)
class _InRunRetryPolicy:
    attempt_limit: int
    max_retry_after_seconds: float
    sleep_budget_seconds: float
    base_backoff_seconds: float
    max_backoff_seconds: float
    jitter_ratio: float


@dataclass(frozen=True)
class SyncPhaseEvidence:
    phase: Literal["sync"]
    status: Literal["deferred", "succeeded", "failed"]
    phase_status: PhaseStatus
    sync_name: str
    destination_surface: str
    dry_run: bool
    irreversible_writes: bool
    progress_advanced: bool
    request_batches_planned: bool
    surface_family: str
    surface_execution_mode: str
    delivery_outcome: str
    compatibility: DestinationCompatibility
    progress_decision: CommitDecision
    submission: DestinationSubmissionEvidence
    delivery_decision: DeliveryEvidenceDecision
    destination_evidence: DestinationSyncEvidence | None = None
    target_resolution: TargetResolutionEvidence | None = None
    resolved_target_mappings: tuple[TargetMapping, ...] = ()
    destination_batch_count: int = 0
    destination_batches: tuple[DestinationBatchRecord, ...] = ()
    auth: object | None = None
    notes: tuple[str, ...] = ()


def sync_destination(
    *,
    sync: Sync,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    dry_run: bool,
    destination_evidence: DestinationSyncEvidence | None = None,
    secret_resolver: SecretResolver | None = None,
    runtime_store: RuntimeStore | None = None,
    run_id: str | None = None,
    attempt_id: str | None = None,
    page_index: int | None = None,
    resolved_target_mappings: Sequence[TargetMapping] = (),
    console: ConsoleRenderer | None = None,
) -> SyncPhaseEvidence:
    """Validate and submit deterministic destination evidence for the sync boundary."""

    binding, surface = _binding_and_surface(sync)
    binding = _binding_with_runtime_target_registry(binding=binding, runtime_store=runtime_store)
    binding = _binding_with_target_mappings(
        binding=binding,
        target_mappings=resolved_target_mappings,
    )
    auth_mode = _binding_auth_mode(binding)
    credentials = _binding_credentials(binding)
    resolved_auth = resolve_auth(
        mode=auth_mode,
        credentials=credentials,
        resolver=secret_resolver or configured_secret_resolver(),
        token_transport=_binding_token_transport(binding),
        jwt_signer=_binding_jwt_signer(binding),
    )
    binding = _binding_with_connector_managed_target_client(
        binding=binding,
        surface=surface,
        resolved_auth=resolved_auth,
    )
    redacted_auth = redacted_auth_evidence(mode=auth_mode, credentials=credentials, resolved=True)
    operation_kinds = _state_operation_kinds(reconciled)
    try:
        compatibility = validate_surface_compatibility(
            sync=sync,
            surface=surface,
            operation_kinds=operation_kinds,
        )
    except DestinationCompatibilityError as exc:
        _log_error(
            "destination_compatibility_checked",
            **_destination_context(
                sync=sync,
                binding=binding,
                surface=surface,
                dry_run=dry_run,
                run_id=run_id,
                attempt_id=attempt_id,
                status="invalid",
            ),
            surface_family=surface.declaration_family,
            execution_mode=surface.execution_mode,
            delivery_outcome=surface.delivery_outcome,
            operation_kinds=tuple(operation_kinds or ()),
            exception_type=type(exc).__name__,
            exception_message=redact_text(str(exc)),
        )
        raise
    _log_info(
        "destination_compatibility_checked",
        **_destination_context(
            sync=sync,
            binding=binding,
            surface=surface,
            dry_run=dry_run,
            run_id=run_id,
            attempt_id=attempt_id,
            status="valid",
        ),
        surface_family=compatibility.family,
        execution_mode=surface.execution_mode,
        delivery_outcome=compatibility.delivery_outcome,
        operation_kinds=tuple(str(kind) for kind in compatibility.operation_kinds),
    )
    target_resolution = _resolve_state_targets(
        sync=sync,
        reconciled=reconciled,
        binding=binding,
        surface=surface,
        runtime_store=runtime_store,
        dry_run=dry_run,
    )
    if target_resolution is not None and target_resolution.status == "failed":
        _log_error(
            "target_resolution_failed",
            **_destination_context(
                sync=sync,
                binding=binding,
                surface=surface,
                dry_run=dry_run,
                run_id=run_id,
                attempt_id=attempt_id,
                status=target_resolution.status,
            ),
            **_target_resolution_context(target_resolution),
            target_missing_count=len(target_resolution.missing),
            target_failure_count=len(target_resolution.failures),
            diagnostic="Target resolution failed before destination submission.",
        )
        missing = ", ".join(target_resolution.missing)
        failures = _target_resolution_failure_summary(target_resolution)
        detail = " ".join(
            part for part in (f"Missing: {missing}." if missing else "", failures) if part
        )
        raise DestinationCompatibilityError(
            f"Target resolution failed for surface `{surface.name}`. {detail}".strip()
        )
    if target_resolution is not None:
        _log_info(
            "target_resolution_completed",
            **_destination_context(
                sync=sync,
                binding=binding,
                surface=surface,
                dry_run=dry_run,
                run_id=run_id,
                attempt_id=attempt_id,
                status=target_resolution.status,
            ),
            **_target_resolution_context(target_resolution),
        )
        binding = _binding_with_resolved_target_mappings(
            binding=binding,
            target_resolution=target_resolution,
        )
    learned_target_mappings = (
        _target_mappings_from_resolution(target_resolution) if target_resolution is not None else ()
    )

    attempted_count = _attempted_record_count(reconciled)
    destination_batch_count = _destination_batch_count(reconciled)
    ledger_planning = _destination_batch_ledger_plans(
        sync=sync,
        binding=binding,
        surface=surface,
        reconciled=reconciled,
        page_index=page_index or 1,
        dry_run=dry_run,
    )
    ledger_plans = ledger_planning.plans
    _log_info(
        "destination_batches_planned",
        **_destination_context(
            sync=sync,
            binding=binding,
            surface=surface,
            dry_run=dry_run,
            run_id=run_id,
            attempt_id=attempt_id,
            status="planned",
        ),
        page_index=page_index,
        destination_batch_count=len(ledger_plans),
        request_batch_count=(
            len(ledger_planning.request_batches)
            if ledger_planning.request_batches is not None
            else len(ledger_plans)
        ),
        attempted_count=attempted_count,
    )
    stored_batches = _destination_batch_working_set(
        store=runtime_store,
        plans=ledger_plans,
        dry_run=dry_run,
        sync=sync,
        binding=binding,
        surface=surface,
        run_id=run_id,
        attempt_id=attempt_id,
    )
    rerun_plan = _destination_batch_rerun_plan(
        plans=ledger_plans,
        stored_batches=stored_batches,
        retry_limit=_destination_batch_retry_limit(binding.config),
        dry_run=dry_run,
    )
    if ledger_plans:
        destination_batch_count = len(ledger_plans)
    selected_request_plans = (
        ledger_planning.request_batches
        if dry_run and ledger_planning.request_batches is not None
        else tuple(plan.request_batch for plan in rerun_plan.selected)
        if not dry_run
        else None
    )
    selected_attempted_count = (
        attempted_count if dry_run else sum(plan.row_count for plan in rerun_plan.selected)
    )
    _log_info(
        "destination_submission_started",
        **_destination_context(
            sync=sync,
            binding=binding,
            surface=surface,
            dry_run=dry_run,
            run_id=run_id,
            attempt_id=attempt_id,
            status="started",
        ),
        page_index=page_index,
        selected_batch_count=(
            len(ledger_planning.request_batches)
            if dry_run and ledger_planning.request_batches is not None
            else len(rerun_plan.selected)
        ),
        selected_row_count=selected_attempted_count,
        skipped_batch_count=len(rerun_plan.skipped),
        blocked_batch_count=len(rerun_plan.blocked),
    )
    selected_submission = _select_destination_submission(
        destination_evidence=destination_evidence,
        attempted_count=attempted_count,
        dry_run=dry_run,
        reconciled=reconciled,
        ledger_plans=ledger_plans,
        rerun_plan=rerun_plan,
        binding=binding,
        surface=surface,
        selected_attempted_count=selected_attempted_count,
        resolved_auth=resolved_auth,
        selected_request_plans=selected_request_plans,
    )
    if runtime_store is not None and run_id is not None and attempt_id is not None:
        if destination_evidence is None:
            selected_submission, updated_batches = _submit_with_in_run_retries(
                sync=sync,
                binding=binding,
                surface=surface,
                reconciled=reconciled,
                resolved_auth=resolved_auth,
                dry_run=dry_run,
                store=runtime_store,
                plans=rerun_plan.selected,
                current_batches=stored_batches,
                run_id=run_id,
                attempt_id=attempt_id,
                selected_submission=selected_submission,
                selected_attempted_count=selected_attempted_count,
                selected_request_plans=selected_request_plans,
                console=console,
            )
        else:
            updated_batches = _record_destination_batch_states(
                store=runtime_store,
                plans=rerun_plan.selected,
                current_batches=stored_batches,
                submission=selected_submission,
                run_id=run_id,
                attempt_id=attempt_id,
                dry_run=dry_run,
                console=console,
            )
        stored_batches = _merge_destination_batch_working_set(
            current_batches=stored_batches,
            updated_batches=updated_batches,
            plans=ledger_plans,
        )
    elif (
        destination_evidence is None
        and not dry_run
        and bool(selected_request_plans)
        and selected_submission.status == "planned"
    ):
        selected_submission = _submit_request_batches_without_recording(
            binding=binding,
            surface=surface,
            reconciled=reconciled,
            resolved_auth=resolved_auth,
            plans=rerun_plan.selected,
            on_failure=sync.on_failure,
        )
    submission = _submission_with_rerun_decisions(
        submission=selected_submission,
        rerun_plan=rerun_plan,
        attempted_count=attempted_count,
        dry_run=dry_run,
    )
    if submission.request_batch_count:
        destination_batch_count = submission.request_batch_count
    terminal_evidence = _terminal_evidence_from_submission(submission)
    terminal_decision = decide_progress_commit(
        delivery_outcome=surface.delivery_outcome,
        on_failure=sync.on_failure,
        destination_evidence=terminal_evidence,
        dry_run=dry_run,
    )
    delivery_decision = evaluate_delivery_outcome(
        surface=surface,
        evidence=submission,
    )
    progress_decision = _combine_commit_decisions(
        terminal_decision=terminal_decision,
        delivery_decision=delivery_decision,
    )
    _log_info(
        "destination_submission_completed",
        **_destination_context(
            sync=sync,
            binding=binding,
            surface=surface,
            dry_run=dry_run,
            run_id=run_id,
            attempt_id=attempt_id,
            status=submission.status,
        ),
        page_index=page_index,
        **_submission_summary_context(
            submission,
            request_batch_count=submission.request_batch_count or len(ledger_plans),
        ),
        progress_decision_allowed=progress_decision.allowed,
    )
    if submission.status in {
        "retryable_failure",
        "terminal_record_failure",
        "pre_acceptance_failure",
    }:
        _log_failure = _log_warning if submission.status == "retryable_failure" else _log_error
        _log_failure(
            "destination_submission_failed",
            **_destination_context(
                sync=sync,
                binding=binding,
                surface=surface,
                dry_run=dry_run,
                run_id=run_id,
                attempt_id=attempt_id,
                status=submission.status,
            ),
            page_index=page_index,
            failure_category=_submission_failure_category(submission),
            **_submission_failure_context(
                submission,
                request_batch_count=submission.request_batch_count or len(ledger_plans),
            ),
        )
    progress_advanced = False
    irreversible_writes = _irreversible_writes_planned(target_resolution=target_resolution)
    status: Literal["deferred", "succeeded", "failed"] = "deferred"
    if not dry_run and not _reconciled_work_deferred(reconciled):
        status = "succeeded" if progress_decision.allowed else "failed"

    return SyncPhaseEvidence(
        phase="sync",
        status=status,
        phase_status=_submission_phase(
            sync_name=sync.name,
            surface=surface,
            dry_run=dry_run,
            destination_batch_count=destination_batch_count,
            target_resolution=target_resolution,
            irreversible_writes=irreversible_writes,
            submission=submission,
            progress_decision=progress_decision,
            progress_advanced=progress_advanced,
            status=status,
        ),
        sync_name=sync.name,
        destination_surface=sync.surface,
        dry_run=dry_run,
        irreversible_writes=irreversible_writes,
        progress_advanced=progress_advanced,
        request_batches_planned=True,
        surface_family=surface.declaration_family,
        surface_execution_mode=surface.execution_mode,
        delivery_outcome=surface.delivery_outcome,
        compatibility=compatibility,
        progress_decision=progress_decision,
        submission=submission,
        delivery_decision=delivery_decision,
        destination_evidence=terminal_evidence,
        target_resolution=target_resolution,
        resolved_target_mappings=learned_target_mappings,
        destination_batch_count=destination_batch_count,
        destination_batches=stored_batches,
        auth=redacted_auth,
        notes=(
            f"Destination Surface `{surface.name}` compatibility validated.",
            f"Auth Mode `{resolved_auth.mode}` resolved with redacted evidence.",
            submission.summary,
            progress_decision.reason,
            "Partner-specific payloads should be built only at this sync boundary.",
            f"Reconcile evidence for `{reconciled.sync_name}` remains per Sync.",
            *_managed_target_write_notes(target_resolution=target_resolution),
        ),
    )


def retry_destination_batches(
    *,
    sync: Sync,
    runtime_store: RuntimeStore,
    run_id: str,
    attempt_id: str,
    dry_run: bool,
) -> tuple[DestinationBatchRecord, ...]:
    if dry_run:
        return ()
    binding, surface = _binding_and_surface(sync)
    binding = _binding_with_runtime_target_registry(binding=binding, runtime_store=runtime_store)
    retry_limit = _destination_batch_retry_limit(binding.config)
    candidates = runtime_store.list_destination_batch_retry_candidates(
        scope=_destination_scope(sync),
        retry_limit=retry_limit,
    )
    if not candidates:
        return ()
    scoped_batches = runtime_store.list_destination_batches(scope=_destination_scope(sync))
    reconstructed = tuple(
        reconstructed
        for group_candidates in _destination_batch_reconcile_candidate_groups(candidates)
        for reconstructed in _retry_ledger_plans_from_reconcile_group(
            sync=sync,
            binding=binding,
            surface=surface,
            store=runtime_store,
            candidates=group_candidates,
            scoped_batches=scoped_batches,
        )
    )
    if not reconstructed:
        return ()
    plans = tuple(plan for plan, _ in reconstructed)
    work_pages = tuple(work_page for _, work_page in reconstructed)
    auth_mode = _binding_auth_mode(binding)
    credentials = _binding_credentials(binding)
    resolved_auth = resolve_auth(
        mode=auth_mode,
        credentials=credentials,
        resolver=configured_secret_resolver(),
        token_transport=_binding_token_transport(binding),
        jwt_signer=_binding_jwt_signer(binding),
    )
    binding = _binding_with_connector_managed_target_client(
        binding=binding,
        surface=surface,
        resolved_auth=resolved_auth,
    )
    selected_request_plans = tuple(plan.request_batch for plan in plans)
    submission = _submission_evidence(
        binding=binding,
        surface=surface,
        delivery_outcome=surface.delivery_outcome,
        attempted_count=sum(plan.row_count for plan in plans),
        dry_run=False,
        resolved_auth=resolved_auth,
        reconciled=cast(
            Any,
            SimpleNamespace(
                sync_name=sync.name,
                operation_pages=work_pages if _destination_scope(sync).family == "state" else (),
                import_pages=work_pages if _destination_scope(sync).family == "event" else (),
            ),
        ),
        selected_request_plans=selected_request_plans,
    )
    return _record_destination_batch_states(
        store=runtime_store,
        plans=plans,
        current_batches=candidates,
        submission=submission,
        run_id=run_id,
        attempt_id=f"{attempt_id}:retry-sweep",
        dry_run=False,
    )


def _select_destination_submission(
    *,
    destination_evidence: DestinationSyncEvidence | None,
    attempted_count: int,
    dry_run: bool,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    ledger_plans: tuple[_DestinationBatchLedgerPlan, ...],
    rerun_plan: _DestinationBatchRerunPlan,
    binding: DestinationBinding,
    surface: DestinationSurface,
    selected_attempted_count: int,
    resolved_auth: object,
    selected_request_plans: tuple[RequestBatchPlan, ...] | None,
) -> DestinationSubmissionEvidence:
    if destination_evidence is not None:
        return _submission_from_destination_sync_evidence(destination_evidence)
    if _reconciled_work_deferred(reconciled):
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=dry_run,
            summary="Destination submission is deferred until reconciled work is available.",
        )
    if ledger_plans and not dry_run and not rerun_plan.selected:
        return DestinationSubmissionEvidence.planned(
            attempted_count=0,
            dry_run=False,
            request_batch_count=0,
            summary=rerun_plan.summary or "No destination batches selected for submission.",
        )
    if not dry_run and selected_request_plans:
        return DestinationSubmissionEvidence.planned(
            attempted_count=0,
            dry_run=False,
            request_batch_count=len(selected_request_plans),
            summary="Destination request batches selected for runtime-owned submission.",
        )
    return _submission_evidence(
        binding=binding,
        surface=surface,
        delivery_outcome=surface.delivery_outcome,
        attempted_count=selected_attempted_count,
        dry_run=dry_run,
        resolved_auth=resolved_auth,
        reconciled=reconciled,
        selected_request_plans=selected_request_plans,
    )


def _submit_with_in_run_retries(
    *,
    sync: Sync,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    resolved_auth: object,
    dry_run: bool,
    store: RuntimeStore,
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    current_batches: tuple[DestinationBatchRecord, ...],
    run_id: str,
    attempt_id: str,
    selected_submission: DestinationSubmissionEvidence,
    selected_attempted_count: int,
    selected_request_plans: tuple[RequestBatchPlan, ...] | None,
    console: ConsoleRenderer | None = None,
) -> tuple[DestinationSubmissionEvidence, tuple[DestinationBatchRecord, ...]]:
    if dry_run or not plans:
        return selected_submission, tuple(plan.record for plan in plans)
    policy = _in_run_retry_policy(binding.config)
    if selected_request_plans is not None:
        submission, updated_batches = _submit_and_record_request_batches(
            binding=binding,
            surface=surface,
            reconciled=reconciled,
            resolved_auth=resolved_auth,
            store=store,
            plans=plans,
            current_batches=current_batches,
            run_id=run_id,
            attempt_id=attempt_id,
            on_failure=sync.on_failure,
            console=console,
        )
    else:
        submission = selected_submission
        updated_batches = _record_destination_batch_states(
            store=store,
            plans=plans,
            current_batches=current_batches,
            submission=submission,
            run_id=run_id,
            attempt_id=attempt_id,
            dry_run=dry_run,
            console=console,
        )
    if not _in_run_retry_supported(
        policy=policy,
        selected_request_plans=selected_request_plans,
    ):
        return submission, updated_batches

    cumulative_sleep = 0.0
    attempt_number = 1
    retried = False
    retry_plans = _in_run_retry_plans(plans=plans, batches=updated_batches)
    while (
        attempt_number < policy.attempt_limit
        and retry_plans
        and _submission_retryable_in_run(submission)
    ):
        wait_seconds = _next_in_run_retry_wait(
            submission=submission,
            attempt_number=attempt_number,
            policy=policy,
        )
        if wait_seconds is None or cumulative_sleep + wait_seconds > policy.sleep_budget_seconds:
            _log_info(
                "destination_in_run_retry_skipped",
                **_destination_context(
                    sync=sync,
                    binding=binding,
                    surface=surface,
                    dry_run=dry_run,
                    run_id=run_id,
                    attempt_id=attempt_id,
                    status=submission.status,
                ),
                retry_attempt_number=attempt_number + 1,
                retry_after_seconds=submission.retry_after_seconds,
                planned_sleep_seconds=wait_seconds,
                cumulative_sleep_seconds=cumulative_sleep,
                sleep_budget_seconds=policy.sleep_budget_seconds,
            )
            break
        _log_info(
            "destination_in_run_retry_scheduled",
            **_destination_context(
                sync=sync,
                binding=binding,
                surface=surface,
                dry_run=dry_run,
                run_id=run_id,
                attempt_id=attempt_id,
                status=submission.status,
            ),
            retry_attempt_number=attempt_number + 1,
            retry_after_seconds=submission.retry_after_seconds,
            planned_sleep_seconds=wait_seconds,
            cumulative_sleep_seconds=cumulative_sleep,
        )
        if wait_seconds > 0:
            _sleep(wait_seconds)
        cumulative_sleep += wait_seconds
        attempt_number += 1
        retried = True
        retry_submission, retry_updated_batches = _submit_and_record_request_batches(
            binding=binding,
            surface=surface,
            reconciled=reconciled,
            resolved_auth=resolved_auth,
            store=store,
            plans=retry_plans,
            current_batches=updated_batches,
            run_id=run_id,
            attempt_id=f"{attempt_id}:in-run-retry-{attempt_number}",
            on_failure=sync.on_failure,
            console=console,
        )
        submission = retry_submission
        updated_batches = _merge_destination_batch_working_set(
            current_batches=updated_batches,
            updated_batches=retry_updated_batches,
            plans=plans,
        )
        retry_plans = _in_run_retry_plans(plans=plans, batches=updated_batches)
    if not retried:
        return submission, updated_batches
    return (
        _submission_from_in_run_batches(
            plans=plans,
            batches=updated_batches,
            fallback=submission,
            attempted_count=selected_attempted_count,
        ),
        updated_batches,
    )


def _submit_and_record_request_batches(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    resolved_auth: object,
    store: RuntimeStore,
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    current_batches: tuple[DestinationBatchRecord, ...],
    run_id: str,
    attempt_id: str,
    on_failure: FailureHandlingMode,
    console: ConsoleRenderer | None = None,
) -> tuple[DestinationSubmissionEvidence, tuple[DestinationBatchRecord, ...]]:
    current_working_set = current_batches
    flush_final_records: list[DestinationBatchRecord] = []
    submissions: list[DestinationSubmissionEvidence] = []

    def flush_destination_batch_statuses() -> None:
        nonlocal current_working_set
        if not flush_final_records:
            return
        updated_batch = _persist_destination_batch_state(
            store=store,
            run_id=run_id,
            base_batches=current_working_set,
            final_records=tuple(flush_final_records),
            console=None,
        )
        flush_final_records.clear()
        current_working_set = _merge_destination_batch_working_set(
            current_batches=current_working_set,
            updated_batches=updated_batch,
            plans=plans,
        )

    for index, plan in enumerate(plans, start=1):
        submission = _submission_evidence(
            binding=binding,
            surface=surface,
            delivery_outcome=surface.delivery_outcome,
            attempted_count=plan.row_count,
            dry_run=False,
            resolved_auth=resolved_auth,
            reconciled=reconciled,
            selected_request_plans=(plan.request_batch,),
        )
        submissions.append(submission)
        final_records = _destination_batch_records_after_submission(
            plans=(plan,),
            current_batches=current_working_set,
            submission=submission,
            run_id=run_id,
            attempt_id=f"{attempt_id}:request-batch-{index}",
        )
        for record in final_records:
            _log_destination_batch_attempt_completed(record=record, run_id=run_id)
            _emit_console_destination_batch_attempt(
                console=console,
                batch=record,
                row_count=record.record_count,
                run_id=run_id,
            )
        flush_final_records.extend(final_records)
        current_working_set = _merge_destination_batch_working_set(
            current_batches=current_working_set,
            updated_batches=final_records,
            plans=plans,
        )
        if len(flush_final_records) >= _DESTINATION_BATCH_STATUS_FLUSH_THRESHOLD:
            flush_destination_batch_statuses()
        decision = decide_request_batch_continuation(
            on_failure=on_failure,
            submission=submission,
        )
        if not decision.allowed:
            break
    flush_destination_batch_statuses()

    aggregate = _aggregate_request_batch_submissions(
        submissions=tuple(submissions),
        attempted_count=sum(submission.attempted_count for submission in submissions),
        request_batch_count=len(plans),
    )
    return aggregate, current_working_set


def _submit_request_batches_without_recording(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    resolved_auth: object,
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    on_failure: FailureHandlingMode,
) -> DestinationSubmissionEvidence:
    submissions: list[DestinationSubmissionEvidence] = []
    for plan in plans:
        submission = _submission_evidence(
            binding=binding,
            surface=surface,
            delivery_outcome=surface.delivery_outcome,
            attempted_count=plan.row_count,
            dry_run=False,
            resolved_auth=resolved_auth,
            reconciled=reconciled,
            selected_request_plans=(plan.request_batch,),
        )
        submissions.append(submission)
        decision = decide_request_batch_continuation(
            on_failure=on_failure,
            submission=submission,
        )
        if not decision.allowed:
            break
    return _aggregate_request_batch_submissions(
        submissions=tuple(submissions),
        attempted_count=sum(submission.attempted_count for submission in submissions),
        request_batch_count=len(plans),
    )


def _aggregate_request_batch_submissions(
    *,
    submissions: tuple[DestinationSubmissionEvidence, ...],
    attempted_count: int,
    request_batch_count: int,
) -> DestinationSubmissionEvidence:
    if not submissions:
        return DestinationSubmissionEvidence.planned(
            attempted_count=0,
            dry_run=False,
            request_batch_count=request_batch_count,
            summary="Destination submission had no request batches to execute.",
        )
    pre_acceptance_failure_count = sum(
        submission.pre_acceptance_failure_count for submission in submissions
    )
    retryable_failure_count = sum(submission.retryable_failure_count for submission in submissions)
    terminal_record_failure_count = sum(
        submission.terminal_record_failure_count for submission in submissions
    )
    confirmed_count = sum(submission.confirmed_count for submission in submissions)
    accepted_count = sum(submission.accepted_count for submission in submissions)
    failure = next(
        (
            submission
            for submission in submissions
            if submission.status
            in {"pre_acceptance_failure", "retryable_failure", "terminal_record_failure"}
        ),
        None,
    )
    if pre_acceptance_failure_count:
        status: Literal[
            "confirmed",
            "accepted",
            "retryable_failure",
            "terminal_record_failure",
            "pre_acceptance_failure",
        ] = "pre_acceptance_failure"
    elif retryable_failure_count:
        status = "retryable_failure"
    elif terminal_record_failure_count:
        status = "terminal_record_failure"
    elif accepted_count and not confirmed_count:
        status = "accepted"
    else:
        status = "confirmed"
    summaries = tuple(submission.summary for submission in submissions if submission.summary)
    summary = summaries[0] if len(summaries) == 1 else " ".join(summaries)
    if failure is not None and failure.summary:
        summary = failure.summary
    return DestinationSubmissionEvidence(
        status=status,
        attempted_count=attempted_count,
        dry_run=False,
        confirmed_count=confirmed_count,
        accepted_count=accepted_count,
        retryable_failure_count=retryable_failure_count,
        terminal_record_failure_count=terminal_record_failure_count,
        pre_acceptance_failure_count=pre_acceptance_failure_count,
        pre_acceptance_failure_category=(
            failure.pre_acceptance_failure_category if failure is not None else None
        ),
        request_batch_count=request_batch_count,
        receipts=tuple(receipt for submission in submissions for receipt in submission.receipts),
        remote_handles=tuple(
            handle for submission in submissions for handle in submission.remote_handles
        ),
        summary=summary,
        http_status=failure.http_status if failure is not None else submissions[-1].http_status,
        partner_error_code=(
            failure.partner_error_code
            if failure is not None
            else submissions[-1].partner_error_code
        ),
        partner_error_subcode=(
            failure.partner_error_subcode
            if failure is not None
            else submissions[-1].partner_error_subcode
        ),
        partner_error_detail=(
            failure.partner_error_detail
            if failure is not None
            else submissions[-1].partner_error_detail
        ),
        retry_after_seconds=(
            failure.retry_after_seconds
            if failure is not None
            else submissions[-1].retry_after_seconds
        ),
    )


def _in_run_retry_plans(
    *,
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    batches: tuple[DestinationBatchRecord, ...],
) -> tuple[_DestinationBatchLedgerPlan, ...]:
    batches_by_id = {batch.batch_id: batch for batch in batches}
    return tuple(
        plan
        for plan in plans
        if (
            (batch := batches_by_id.get(plan.record.batch_id)) is not None
            and batch.status == "failed"
            and batch.retry_eligible is True
            and batch.completion_state == "unresolved"
        )
    )


def _submission_from_in_run_batches(
    *,
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    batches: tuple[DestinationBatchRecord, ...],
    fallback: DestinationSubmissionEvidence,
    attempted_count: int,
) -> DestinationSubmissionEvidence:
    batches_by_id = {batch.batch_id: batch for batch in batches}
    confirmed_count = 0
    accepted_count = 0
    retryable_failure_count = 0
    terminal_record_failure_count = 0
    for plan in plans:
        batch = batches_by_id.get(plan.record.batch_id, plan.record)
        if batch.status == "succeeded":
            confirmed_count += plan.row_count
        elif batch.status == "accepted":
            accepted_count += plan.row_count
        elif batch.status == "failed" and batch.retry_eligible is True:
            retryable_failure_count += plan.row_count
        elif batch.status == "failed":
            terminal_record_failure_count += plan.row_count
    if retryable_failure_count:
        status: Literal[
            "confirmed",
            "accepted",
            "retryable_failure",
            "terminal_record_failure",
        ] = "retryable_failure"
    elif terminal_record_failure_count:
        status = "terminal_record_failure"
    elif confirmed_count and not accepted_count:
        status = "confirmed"
    elif accepted_count and not confirmed_count:
        status = "accepted"
    elif confirmed_count or accepted_count:
        status = "confirmed"
    else:
        return fallback
    return DestinationSubmissionEvidence(
        status=status,
        attempted_count=attempted_count,
        dry_run=False,
        confirmed_count=confirmed_count,
        accepted_count=accepted_count,
        retryable_failure_count=retryable_failure_count,
        terminal_record_failure_count=terminal_record_failure_count,
        request_batch_count=len(plans),
        summary=fallback.summary,
        http_status=fallback.http_status,
        partner_error_code=fallback.partner_error_code,
        partner_error_subcode=fallback.partner_error_subcode,
        partner_error_detail=fallback.partner_error_detail,
        retry_after_seconds=fallback.retry_after_seconds,
    )


def _in_run_retry_supported(
    *,
    policy: _InRunRetryPolicy,
    selected_request_plans: tuple[RequestBatchPlan, ...] | None,
) -> bool:
    return policy.attempt_limit > 1 and selected_request_plans is not None


def _submission_retryable_in_run(submission: DestinationSubmissionEvidence) -> bool:
    if submission.status == "retryable_failure" or submission.retryable_failure_count > 0:
        return True
    if submission.status != "pre_acceptance_failure":
        return False
    if submission.pre_acceptance_failure_category not in {"transport", "rate_limit"}:
        return False
    return submission.http_status in {408, 425, 429, 599, *range(500, 600)}


def _next_in_run_retry_wait(
    *,
    submission: DestinationSubmissionEvidence,
    attempt_number: int,
    policy: _InRunRetryPolicy,
) -> float | None:
    retry_after = submission.retry_after_seconds
    if retry_after is not None:
        wait = float(retry_after)
        return wait if wait <= policy.max_retry_after_seconds else None
    backoff = min(
        policy.max_backoff_seconds,
        policy.base_backoff_seconds * (2 ** max(0, attempt_number - 1)),
    )
    if policy.jitter_ratio == 0:
        return backoff
    jitter_span = backoff * policy.jitter_ratio
    return max(0.0, backoff - jitter_span + (_random() * jitter_span * 2))


def _destination_batch_ledger_plans(
    *,
    sync: Sync,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    page_index: int,
    dry_run: bool,
) -> _DestinationBatchLedgerPlanning:
    if _reconciled_work_deferred(reconciled):
        return _DestinationBatchLedgerPlanning(plans=())
    metadata = declaration_metadata(sync.declaration)
    scope = getattr(reconciled, "scope", None)
    if scope is None:
        return _DestinationBatchLedgerPlanning(plans=())
    work = _reconciled_work(reconciled)
    if work is None:
        return _DestinationBatchLedgerPlanning(plans=())
    request_plan = _connector_request_plan(
        binding=binding,
        surface=surface,
        reconciled=reconciled,
    )
    if request_plan is not None and request_plan.plans:
        return _DestinationBatchLedgerPlanning(
            plans=tuple(
                _ledger_plan_from_request_batch(
                    request_batch=batch,
                    declaration_version_id=metadata.declaration_version_id,
                    scope=scope,
                    reconcile_page_index=page_index,
                )
                for batch in request_plan.plans
            ),
            request_batches=request_plan.plans,
        )
    if request_plan is not None:
        if _attempted_record_count(reconciled) == 0 or dry_run:
            return _DestinationBatchLedgerPlanning(
                plans=(),
                request_batches=request_plan.plans,
            )
    if _attempted_record_count(reconciled) == 0:
        return _DestinationBatchLedgerPlanning(plans=())
    if dry_run:
        return _DestinationBatchLedgerPlanning(plans=())
    raise DestinationCompatibilityError(
        f"Destination connector `{binding.destination_ref}` must produce request-batch plans "
        f"for submitting surface `{surface.name}`. Reconcile-batch ledger fallback is not "
        "supported."
    )


def _destination_batch_working_set(
    *,
    store: RuntimeStore | None,
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    dry_run: bool,
    sync: Sync,
    binding: DestinationBinding,
    surface: DestinationSurface,
    run_id: str | None,
    attempt_id: str | None,
) -> tuple[DestinationBatchRecord, ...]:
    planned = tuple(plan.record for plan in plans)
    if store is None or dry_run or not planned:
        _log_info(
            "destination_batches_recorded",
            **_destination_context(
                sync=sync,
                binding=binding,
                surface=surface,
                dry_run=dry_run,
                run_id=run_id,
                attempt_id=attempt_id,
                status="planned" if planned else "empty",
            ),
            pending_batch_count=len(planned),
            upserted_batch_count=0,
        )
        return planned
    existing = store.get_destination_batches(batch_ids=tuple(batch.batch_id for batch in planned))
    existing_by_id = {batch.batch_id: batch for batch in existing}
    current = tuple(
        replace(existing_batch, identity=planned_batch.identity)
        if (existing_batch := existing_by_id.get(planned_batch.batch_id)) is not None
        else planned_batch
        for planned_batch in planned
    )
    updated = store.upsert_destination_batches(
        current,
        read_back=False,
        existing_batches=existing,
    )
    _log_info(
        "destination_batches_recorded",
        **_destination_context(
            sync=sync,
            binding=binding,
            surface=surface,
            dry_run=dry_run,
            run_id=run_id,
            attempt_id=attempt_id,
            status="recorded",
        ),
        pending_batch_count=len(planned),
        upserted_batch_count=len(updated),
    )
    return updated


def _merge_destination_batch_working_set(
    *,
    current_batches: tuple[DestinationBatchRecord, ...],
    updated_batches: tuple[DestinationBatchRecord, ...],
    plans: tuple[_DestinationBatchLedgerPlan, ...],
) -> tuple[DestinationBatchRecord, ...]:
    updated_by_id = {batch.batch_id: batch for batch in updated_batches}
    current_by_id = {batch.batch_id: batch for batch in current_batches}
    return tuple(
        updated_by_id.get(plan.record.batch_id)
        or current_by_id.get(plan.record.batch_id)
        or plan.record
        for plan in plans
    )


def _destination_batch_rerun_plan(
    *,
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    stored_batches: tuple[DestinationBatchRecord, ...],
    retry_limit: int,
    dry_run: bool,
) -> _DestinationBatchRerunPlan:
    if dry_run:
        return _DestinationBatchRerunPlan(
            current=plans,
            selected=(),
            skipped=(),
            blocked=(),
            summary="Dry run planned destination batches without submission or ledger mutation.",
        )
    stored_by_id = {batch.batch_id: batch for batch in stored_batches}
    selected: list[_DestinationBatchLedgerPlan] = []
    skipped: list[DestinationBatchRecord] = []
    blocked: list[DestinationBatchRecord] = []
    for plan in plans:
        stored = stored_by_id.get(plan.record.batch_id, plan.record)
        if _destination_batch_resolved_for_rerun(stored):
            skipped.append(stored)
        elif _destination_batch_retryable_for_rerun(stored, retry_limit=retry_limit):
            selected.append(plan)
        elif stored.attempt_count == 0 and stored.status == "pending":
            selected.append(plan)
        else:
            blocked.append(stored)
    if blocked:
        return _DestinationBatchRerunPlan(
            current=plans,
            selected=(),
            skipped=tuple(skipped),
            blocked=tuple(blocked),
            summary=(
                "Destination submission blocked by unresolved destination batch ledger state."
            ),
        )
    return _DestinationBatchRerunPlan(
        current=plans,
        selected=tuple(selected),
        skipped=tuple(skipped),
        blocked=(),
        summary=_rerun_summary(skipped=tuple(skipped), selected=tuple(selected)),
    )


def _destination_batch_resolved_for_rerun(batch: DestinationBatchRecord) -> bool:
    return batch.status in {"accepted", "succeeded", "skipped"} and (
        batch.completion_state == "resolved"
    )


def _destination_batch_retryable_for_rerun(
    batch: DestinationBatchRecord,
    *,
    retry_limit: int,
) -> bool:
    return (
        batch.status == "failed"
        and batch.retry_eligible is True
        and batch.completion_state == "unresolved"
        and batch.attempt_count < retry_limit
    )


def _destination_batch_retry_limit(config: Mapping[str, object]) -> int:
    raw = config.get("destination_batch_retry_limit", _DEFAULT_DESTINATION_BATCH_RETRY_LIMIT)
    if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
        raise ValueError("`destination_batch_retry_limit` must be a non-negative integer.")
    return raw


def _in_run_retry_policy(config: Mapping[str, object]) -> _InRunRetryPolicy:
    return _InRunRetryPolicy(
        attempt_limit=_nonnegative_int_config(
            config,
            "destination_in_run_retry_attempt_limit",
            _DEFAULT_IN_RUN_RETRY_ATTEMPT_LIMIT,
        ),
        max_retry_after_seconds=_nonnegative_finite_float_config(
            config,
            "destination_in_run_retry_max_retry_after_seconds",
            _DEFAULT_IN_RUN_RETRY_MAX_RETRY_AFTER_SECONDS,
        ),
        sleep_budget_seconds=_nonnegative_finite_float_config(
            config,
            "destination_in_run_retry_sleep_budget_seconds",
            _DEFAULT_IN_RUN_RETRY_SLEEP_BUDGET_SECONDS,
        ),
        base_backoff_seconds=_nonnegative_finite_float_config(
            config,
            "destination_in_run_retry_base_backoff_seconds",
            _DEFAULT_IN_RUN_RETRY_BASE_BACKOFF_SECONDS,
        ),
        max_backoff_seconds=_nonnegative_finite_float_config(
            config,
            "destination_in_run_retry_max_backoff_seconds",
            _DEFAULT_IN_RUN_RETRY_MAX_BACKOFF_SECONDS,
        ),
        jitter_ratio=_nonnegative_finite_float_config(
            config,
            "destination_in_run_retry_jitter_ratio",
            _DEFAULT_IN_RUN_RETRY_JITTER_RATIO,
        ),
    )


def _nonnegative_int_config(
    config: Mapping[str, object],
    name: str,
    default: int,
) -> int:
    raw = config.get(name, default)
    if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
        raise ValueError(f"`{name}` must be a non-negative integer.")
    return raw


def _nonnegative_finite_float_config(
    config: Mapping[str, object],
    name: str,
    default: float,
) -> float:
    raw = config.get(name, default)
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise ValueError(f"`{name}` must be a non-negative finite number.")
    value = float(raw)
    if value < 0 or not math.isfinite(value):
        raise ValueError(f"`{name}` must be a non-negative finite number.")
    return value


def _rerun_summary(
    *,
    skipped: tuple[DestinationBatchRecord, ...],
    selected: tuple[_DestinationBatchLedgerPlan, ...],
) -> str:
    if skipped and selected:
        return (
            f"Skipped {len(skipped)} resolved destination batch(es) and selected "
            f"{len(selected)} destination batch(es) for submission."
        )
    if skipped:
        return f"Skipped {len(skipped)} resolved destination batch(es)."
    return ""


def _submission_with_rerun_decisions(
    *,
    submission: DestinationSubmissionEvidence,
    rerun_plan: _DestinationBatchRerunPlan,
    attempted_count: int,
    dry_run: bool,
) -> DestinationSubmissionEvidence:
    if dry_run or not rerun_plan.current:
        return submission
    if rerun_plan.blocked:
        blocked = rerun_plan.blocked[0]
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=False,
            request_batch_count=len(rerun_plan.current),
            summary=(
                f"{rerun_plan.summary} First blocked batch is "
                f"{blocked.status}/{blocked.completion_state} after "
                f"{blocked.attempt_count} attempt(s)."
            ),
        )
    skipped_confirmed = sum(
        _row_count_for_batch(rerun_plan.current, batch)
        for batch in rerun_plan.skipped
        if batch.status == "succeeded"
    )
    skipped_accepted = sum(
        _row_count_for_batch(rerun_plan.current, batch)
        for batch in rerun_plan.skipped
        if batch.status == "accepted"
    )
    skipped_terminal = 0
    if not rerun_plan.selected and rerun_plan.skipped:
        status: Literal[
            "planned",
            "confirmed",
            "accepted",
            "retryable_failure",
            "terminal_record_failure",
            "pre_acceptance_failure",
        ] = "accepted" if skipped_accepted and not skipped_confirmed else "confirmed"
        return DestinationSubmissionEvidence(
            status=status,
            attempted_count=skipped_confirmed + skipped_accepted,
            dry_run=False,
            confirmed_count=skipped_confirmed,
            accepted_count=skipped_accepted,
            terminal_record_failure_count=skipped_terminal,
            request_batch_count=len(rerun_plan.current),
            summary=rerun_plan.summary,
        )
    if not rerun_plan.skipped:
        return submission
    return DestinationSubmissionEvidence(
        status=submission.status,
        attempted_count=(
            submission.attempted_count + skipped_confirmed + skipped_accepted + skipped_terminal
        ),
        dry_run=False,
        confirmed_count=submission.confirmed_count + skipped_confirmed,
        accepted_count=submission.accepted_count + skipped_accepted,
        retryable_failure_count=submission.retryable_failure_count,
        terminal_record_failure_count=(submission.terminal_record_failure_count + skipped_terminal),
        pre_acceptance_failure_count=submission.pre_acceptance_failure_count,
        pre_acceptance_failure_category=submission.pre_acceptance_failure_category,
        request_batch_count=len(rerun_plan.current),
        receipts=submission.receipts,
        remote_handles=submission.remote_handles,
        summary=" ".join(part for part in (rerun_plan.summary, submission.summary) if part),
        http_status=submission.http_status,
        partner_error_code=submission.partner_error_code,
        partner_error_subcode=submission.partner_error_subcode,
        partner_error_detail=submission.partner_error_detail,
        retry_after_seconds=submission.retry_after_seconds,
    )


def _row_count_for_batch(
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    batch: DestinationBatchRecord,
) -> int:
    for plan in plans:
        if plan.record.batch_id == batch.batch_id:
            return plan.row_count
    return 0


def _record_destination_batch_states(
    *,
    store: RuntimeStore,
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    current_batches: tuple[DestinationBatchRecord, ...] = (),
    submission: DestinationSubmissionEvidence,
    run_id: str,
    attempt_id: str,
    dry_run: bool,
    console: ConsoleRenderer | None = None,
) -> tuple[DestinationBatchRecord, ...]:
    if dry_run or not plans:
        return tuple(plan.record for plan in plans)
    final_records = _destination_batch_records_after_submission(
        plans=plans,
        current_batches=current_batches,
        submission=submission,
        run_id=run_id,
        attempt_id=attempt_id,
    )
    if final_records == current_batches:
        return final_records
    return _persist_destination_batch_state(
        store=store,
        run_id=run_id,
        base_batches=current_batches,
        final_records=final_records,
        console=console,
    )


def _destination_batch_records_after_submission(
    *,
    plans: tuple[_DestinationBatchLedgerPlan, ...],
    current_batches: tuple[DestinationBatchRecord, ...],
    submission: DestinationSubmissionEvidence,
    run_id: str,
    attempt_id: str,
) -> tuple[DestinationBatchRecord, ...]:
    now = datetime.now(UTC)
    outcomes = classify_destination_batch_outcomes(
        row_counts=tuple(plan.row_count for plan in plans),
        submission=submission,
    )
    current_by_id = {batch.batch_id: batch for batch in current_batches}
    final_records: list[DestinationBatchRecord] = []
    for index, (plan, outcome) in enumerate(zip(plans, outcomes, strict=True), start=1):
        current = current_by_id.get(plan.record.batch_id, plan.record)
        status = outcome.status
        if status == "pending":
            final_records.append(current)
            continue
        completed_at = now if outcome.completed else None
        final_records.append(
            _destination_batch_after_submission(
                current,
                run_id=run_id,
                attempt_id=f"{attempt_id}:destination-batch-{index}",
                attempt_number=current.attempt_count + 1,
                record_count=plan.row_count,
                status=status,
                retry_eligible=outcome.retry_eligible,
                http_status=submission.http_status,
                error_summary=_destination_batch_attempt_summary(submission),
                error_detail=submission.partner_error_detail,
                failure_category=_destination_batch_failure_category(submission),
                attempted_at=now,
                completed_at=completed_at,
            )
        )
    return tuple(final_records)


def _persist_destination_batch_state(
    *,
    store: RuntimeStore,
    run_id: str,
    base_batches: tuple[DestinationBatchRecord, ...],
    final_records: tuple[DestinationBatchRecord, ...],
    console: ConsoleRenderer | None = None,
) -> tuple[DestinationBatchRecord, ...]:
    if not final_records:
        return final_records
    updated = store.upsert_destination_batches(
        final_records,
        existing_batches=base_batches,
        read_back=False,
    )
    for batch in updated:
        _emit_console_destination_batch_attempt(
            console=console,
            batch=batch,
            row_count=batch.record_count,
            run_id=run_id,
        )
    return updated


def _emit_console_destination_batch_attempt(
    *,
    console: ConsoleRenderer | None,
    batch: DestinationBatchRecord,
    row_count: int,
    run_id: str,
) -> None:
    if console is None:
        return
    try:
        console.destination_batch_attempt_recorded(
            run_id=run_id,
            sync_name=batch.identity.scope.sync_name,
            destination_binding_name=batch.identity.scope.destination_name,
            surface=batch.identity.scope.surface,
            status=batch.status,
            destination_batch_index=batch.identity.destination_batch_index,
            row_count=row_count,
            completion_state=batch.completion_state,
            attempt_count=batch.attempt_count,
            run_action=_destination_batch_run_action(batch=batch, run_id=run_id),
            progress_implication=_destination_batch_progress_implication(batch),
            retry_eligible=batch.retry_eligible,
            http_status=batch.http_status,
            diagnostic_summary=batch.last_error_summary,
        )
    except Exception:
        return


def _destination_batch_run_action(*, batch: DestinationBatchRecord, run_id: str) -> str:
    if batch.run_id == run_id:
        return "attempted"
    if batch.completion_state == "resolved":
        return "already_resolved"
    if batch.status == "failed" and batch.attempt_count > 0:
        return "blocked_unresolved"
    return "not_attempted"


def _destination_batch_progress_implication(batch: DestinationBatchRecord) -> str:
    if batch.completion_state == "resolved":
        return "resolved_for_progress"
    if batch.status == "failed" and batch.retry_eligible is True:
        return "retryable_unresolved"
    if batch.status == "failed":
        return "unresolved_failure"
    return "awaits_attempt"


def _log_destination_batch_attempt_completed(
    *,
    record: DestinationBatchRecord,
    run_id: str,
) -> None:
    _log_info(
        "destination_batch_attempt_completed",
        run_id=run_id,
        attempt_id=record.attempt_id,
        sync_name=record.identity.scope.sync_name,
        destination_binding_name=record.identity.scope.destination_name,
        surface=record.identity.scope.surface,
        status=record.status,
        destination_batch_index=record.identity.destination_batch_index,
        row_count=record.record_count,
        completion_state=record.completion_state,
        attempt_count=record.attempt_count,
        run_action=_destination_batch_run_action(batch=record, run_id=run_id),
        progress_implication=_destination_batch_progress_implication(record),
        retry_eligible=record.retry_eligible,
        http_status=record.http_status,
        diagnostic_summary=redact_text(record.last_error_summary),
    )


def _destination_batch_after_submission(
    batch: DestinationBatchRecord,
    *,
    run_id: str,
    attempt_id: str,
    attempt_number: int,
    record_count: int,
    status: str,
    retry_eligible: bool | None,
    http_status: int | None,
    error_summary: str | None,
    error_detail: str | None,
    failure_category: str | None,
    attempted_at: datetime,
    completed_at: datetime | None,
) -> DestinationBatchRecord:
    completion_state = _destination_batch_completion_state(status)
    first_submitted_at = batch.first_submitted_at
    if first_submitted_at is None and status != "pending":
        first_submitted_at = attempted_at
    return DestinationBatchRecord(
        batch_id=batch.batch_id,
        identity=batch.identity,
        run_id=run_id,
        attempt_id=attempt_id,
        record_count=record_count,
        status=cast(Any, status),
        completion_state=completion_state,
        attempt_count=max(batch.attempt_count, attempt_number),
        last_error_summary=sanitize_partner_error_detail(error_summary),
        last_error_detail=sanitize_partner_error_detail(error_detail),
        last_failure_category=failure_category,
        http_status=http_status,
        retry_eligible=retry_eligible,
        first_submitted_at=first_submitted_at,
        last_attempted_at=attempted_at,
        completed_at=completed_at if completion_state != "unresolved" else None,
    )


def _destination_batch_failure_category(
    submission: DestinationSubmissionEvidence,
) -> str | None:
    if submission.status in {
        "retryable_failure",
        "terminal_record_failure",
        "pre_acceptance_failure",
    }:
        return _submission_failure_category(submission)
    return None


def _destination_batch_completion_state(status: str) -> Literal["unresolved", "resolved"]:
    if status in {"accepted", "succeeded", "skipped"}:
        return "resolved"
    return "unresolved"


def _connector_request_plan(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
) -> DryRunSubmissionPlan | None:
    hook = getattr(binding.connector, "batch_planning_hook", None)
    if not callable(hook):
        return None
    plan = hook(binding=binding, surface=surface, reconciled=reconciled)
    if not isinstance(plan, DryRunSubmissionPlan):
        raise TypeError("Destination batch_planning_hook must return DryRunSubmissionPlan.")
    return plan


def _reconciled_work(
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
) -> object | None:
    return getattr(reconciled, "operation_pages", None) or getattr(reconciled, "import_pages", None)


def _ledger_plan_from_request_batch(
    *,
    request_batch: RequestBatchPlan,
    declaration_version_id: str,
    scope: object,
    reconcile_page_index: int,
) -> _DestinationBatchLedgerPlan:
    first_collect_id = _request_plan_collect_id(
        request_batch.first_collect_id,
        "first_collect_id",
    )
    last_collect_id = _request_plan_collect_id(
        request_batch.last_collect_id,
        "last_collect_id",
    )
    first_sequence_order = _request_plan_sequence_order(
        request_batch.first_sequence_order,
        "first_sequence_order",
    )
    last_sequence_order = _request_plan_sequence_order(
        request_batch.last_sequence_order,
        "last_sequence_order",
    )
    identity = DestinationBatchIdentity(
        scope=cast(Any, scope),
        declaration_version_id=declaration_version_id,
        source_range=request_batch.source_range,
        reconcile_page_index=reconcile_page_index,
        first_collect_id=first_collect_id,
        last_collect_id=last_collect_id,
        first_sequence_order=first_sequence_order,
        last_sequence_order=last_sequence_order,
        destination_batch_index=request_batch.index,
        payload_fingerprint=request_batch.payload_fingerprint,
        target_request_fingerprint=request_batch.target_request_fingerprint,
    )
    return _DestinationBatchLedgerPlan(
        record=DestinationBatchRecord(
            batch_id=destination_batch_id(identity),
            identity=identity,
            record_count=request_batch.row_count,
        ),
        request_batch=request_batch,
        row_count=request_batch.row_count,
    )


def _event_range_kind(identity: DestinationBatchIdentity, field_name: str) -> str | None:
    source_range = identity.source_range
    if source_range is None:
        return None
    position = source_range.upper_bound_inclusive
    cursor = getattr(position, "cursor_value", None)
    primary_key = getattr(position, "primary_key_value", None)
    scalar = cursor if field_name == "cursor" else primary_key
    kind = getattr(scalar, "kind", None)
    return kind if isinstance(kind, str) else None


def _retry_ledger_plans_from_reconcile_group(
    *,
    sync: Sync,
    binding: DestinationBinding,
    surface: DestinationSurface,
    store: RuntimeStore,
    candidates: tuple[DestinationBatchRecord, ...],
    scoped_batches: tuple[DestinationBatchRecord, ...],
) -> tuple[tuple[_DestinationBatchLedgerPlan, pa.RecordBatch], ...]:
    if not candidates:
        return ()
    group_key = _destination_batch_reconcile_group_key(candidates[0])
    siblings = _destination_batch_reconcile_siblings(group_key=group_key, batches=scoped_batches)
    work_by_batch_id = {
        sibling.batch_id: _read_destination_batch_replay_work(
            sync=sync,
            store=store,
            batch=sibling,
        )
        for sibling in siblings
    }
    work_pages = tuple(work_by_batch_id[sibling.batch_id] for sibling in siblings)
    if not work_pages or any(page.num_rows == 0 for page in work_pages):
        return ()
    identity = candidates[0].identity
    replan_work_pages: tuple[object, ...]
    if identity.scope.family == "event":
        cursor_kind = _event_range_kind(identity, "cursor")
        primary_key_kind = _event_range_kind(identity, "primary_key")
        replan_work_pages = tuple(
            SimpleNamespace(
                payload=page,
                event_cursor_kind=cursor_kind,
                event_primary_key_kind=primary_key_kind,
            )
            for page in work_pages
        )
    else:
        replan_work_pages = work_pages
    reconstructed = SimpleNamespace(
        sync_name=sync.name,
        operation_pages=replan_work_pages if identity.scope.family == "state" else (),
        import_pages=replan_work_pages if identity.scope.family == "event" else (),
    )
    request_plan = _connector_request_plan(
        binding=binding,
        surface=surface,
        reconciled=cast(Any, reconstructed),
    )
    if request_plan is None:
        return ()
    reconstructed_candidates: list[tuple[_DestinationBatchLedgerPlan, pa.RecordBatch]] = []
    for candidate in candidates:
        identity = candidate.identity
        matching = tuple(
            plan
            for plan in request_plan.plans
            if _request_plan_matches_identity(plan=plan, identity=identity)
        )
        work_page = work_by_batch_id.get(candidate.batch_id)
        if len(matching) != 1 or work_page is None:
            continue
        plan = matching[0]
        reconstructed_candidates.append(
            (
                _DestinationBatchLedgerPlan(
                    record=DestinationBatchRecord(
                        batch_id=candidate.batch_id,
                        identity=identity,
                        record_count=plan.row_count,
                    ),
                    request_batch=plan,
                    row_count=plan.row_count,
                ),
                work_page,
            )
        )
    return tuple(reconstructed_candidates)


def _request_plan_matches_identity(
    *,
    plan: RequestBatchPlan,
    identity: DestinationBatchIdentity,
) -> bool:
    if identity.scope.family == "event" and identity.source_range is not None:
        return (
            plan.index == identity.destination_batch_index
            and plan.source_range == identity.source_range
        )
    return (
        plan.payload_fingerprint == identity.payload_fingerprint
        and plan.target_request_fingerprint == identity.target_request_fingerprint
        and _request_plan_coordinates_match_identity(plan=plan, identity=identity)
    )


def _read_destination_batch_replay_work(
    *,
    sync: Sync,
    store: RuntimeStore,
    batch: DestinationBatchRecord,
) -> pa.RecordBatch:
    if batch.identity.scope.family != "event":
        return store.read_destination_batch_work(batch=batch).payload
    declaration = sync.declaration
    if not isinstance(declaration, Event):
        raise DeclarationValidationError("Event destination batch replay requires an Event Sync.")
    source_range = batch.identity.source_range
    if source_range is None:
        raise DeclarationValidationError(
            "Event destination batch replay requires a stored source keyset range."
        )
    lower = source_range.lower_bound_exclusive
    upper = source_range.upper_bound_inclusive
    if lower is not None and not isinstance(lower, EventKeysetScanPosition):
        raise DeclarationValidationError("Event destination batch lower bound is not keyset.")
    if not isinstance(upper, EventKeysetScanPosition):
        raise DeclarationValidationError("Event destination batch upper bound is not keyset.")
    checkpoint = declaration.source.checkpoint
    if checkpoint is None:
        raise DeclarationValidationError(
            "Event destination batch replay requires checkpoint metadata."
        )
    backend = declaration.source.backend
    if backend is None:
        raise DeclarationValidationError(
            "Event destination batch replay requires a Source backend."
        )
    adapter = cast(Any, backend).adapter()
    if not isinstance(adapter, EventSourceWindowSource):
        raise DeclarationValidationError(
            "Event destination batch replay requires source keyset replay support."
        )
    expected_row_count = batch.record_count
    replay_max_rows = expected_row_count if expected_row_count > 0 else 10_000
    page = store.read_event_source_window(
        declaration=declaration,
        window=adapter.prepare_event_source_window(
            EventSourceWindowRequest(
                source_name=declaration.source.name,
                query=declaration.source.query,
                cursor_column=checkpoint["cursor"],
                primary_key_column=checkpoint["primary_key"],
                scan_after=lower,
                scan_through=upper,
                limit=replay_max_rows + 1,
            )
        ),
        max_rows=replay_max_rows,
    )
    if expected_row_count > 0 and (
        page.row_count != expected_row_count or page.next_cursor is not None
    ):
        raise DeclarationValidationError(
            "Event source replay returned a different row count for a stored destination "
            "batch range. The source may no longer retain the rows required for retry."
        )
    return page.payload


def _destination_batch_reconcile_candidate_groups(
    candidates: tuple[DestinationBatchRecord, ...],
) -> tuple[tuple[DestinationBatchRecord, ...], ...]:
    groups: dict[_RetryReconcileGroupKey, list[DestinationBatchRecord]] = {}
    for candidate in candidates:
        groups.setdefault(_destination_batch_reconcile_group_key(candidate), []).append(candidate)
    return tuple(tuple(group) for group in groups.values())


def _destination_batch_reconcile_group_key(
    batch: DestinationBatchRecord,
) -> _RetryReconcileGroupKey:
    identity = batch.identity
    return (
        identity.declaration_version_id,
        identity.source_page_index,
        identity.reconcile_page_index,
    )


def _destination_batch_reconcile_siblings(
    *,
    group_key: _RetryReconcileGroupKey,
    batches: tuple[DestinationBatchRecord, ...],
) -> tuple[DestinationBatchRecord, ...]:
    return tuple(
        sorted(
            (
                candidate
                for candidate in batches
                if _destination_batch_reconcile_group_key(candidate) == group_key
            ),
            key=lambda candidate: (
                candidate.identity.first_collect_id,
                candidate.identity.first_sequence_order,
                candidate.identity.destination_batch_index,
                candidate.batch_id,
            ),
        )
    )


def _request_plan_coordinates_match_identity(
    *,
    plan: RequestBatchPlan,
    identity: DestinationBatchIdentity,
) -> bool:
    if identity.scope.family == "event" and identity.source_range is not None:
        return (
            plan.index == identity.destination_batch_index
            and plan.source_range == identity.source_range
        )
    return (
        _request_plan_collect_id(plan.first_collect_id, "first_collect_id")
        == identity.first_collect_id
        and _request_plan_collect_id(plan.last_collect_id, "last_collect_id")
        == identity.last_collect_id
        and _request_plan_sequence_order(plan.first_sequence_order, "first_sequence_order")
        == identity.first_sequence_order
        and _request_plan_sequence_order(plan.last_sequence_order, "last_sequence_order")
        == identity.last_sequence_order
        and plan.index == identity.destination_batch_index
    )


def _destination_scope(sync: Sync) -> DestinationProgressScope:
    from retl.runtime.progress import destination_progress_scope

    return destination_progress_scope(sync)


def _request_plan_collect_id(value: object, field_name: str) -> str:
    if isinstance(value, str) and is_uuidv7(value):
        return value
    raise TypeError(f"Destination request batch plans require `{field_name}` UUIDv7 values.")


def _request_plan_sequence_order(value: object, field_name: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"Destination request batch plans require `{field_name}` coordinates.")


def _destination_batch_attempt_summary(evidence: DestinationSubmissionEvidence) -> str:
    if evidence.status in {
        "retryable_failure",
        "terminal_record_failure",
        "pre_acceptance_failure",
    }:
        return evidence.summary
    return ""


def _binding_and_surface(sync: Sync) -> tuple[DestinationBinding, DestinationSurface]:
    if isinstance(sync.destination, DestinationBinding):
        return sync.destination, resolve_surface(sync.destination, sync.surface)
    connector = resolve_connector("retl/reference")
    binding = DestinationBinding(
        binding_name=f"{sync.name}_reference_destination",
        destination_ref=connector.ref,
        connector=connector,
    )
    return binding, connector.surface(sync.surface)


def _binding_auth_mode(binding: DestinationBinding) -> AuthMode:
    connector = binding.connector
    auth_modes = getattr(connector, "auth_modes", ())
    return select_auth_mode(tuple(auth_modes), binding.auth_mode)


def _binding_credentials(binding: DestinationBinding) -> Mapping[str, CredentialValue]:
    credentials = binding.credentials
    if isinstance(credentials, Mapping):
        return cast(Mapping[str, CredentialValue], credentials)
    return {}


def _binding_token_transport(binding: DestinationBinding) -> TokenTransport | None:
    connector = binding.connector
    token_transport = getattr(connector, "auth_token_transport", None)
    return token_transport if callable(token_transport) else None


def _binding_jwt_signer(binding: DestinationBinding) -> JwtSigner | None:
    connector = binding.connector
    jwt_signer = getattr(connector, "auth_jwt_signer", None)
    return jwt_signer if callable(jwt_signer) else None


def _state_operation_kinds(
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
) -> tuple[str, ...] | None:
    if not _is_state_reconcile_evidence(reconciled):
        return None
    return _unique_work_column_values(reconciled.operation_pages, "operation")


def _resolve_state_targets(
    *,
    sync: Sync,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    binding: DestinationBinding,
    surface: DestinationSurface,
    runtime_store: RuntimeStore | None,
    dry_run: bool,
) -> TargetResolutionEvidence | None:
    if not isinstance(sync.declaration, State):
        return None
    if surface.target_mode == "unsupported" or sync.declaration.target is None:
        return None
    logical_targets: Iterable[str | None] = ()
    if _is_state_reconcile_evidence(reconciled):
        logical_targets = _unique_work_column_values(reconciled.operation_pages, "target_json")
    return resolve_targets(
        logical_targets=logical_targets,
        binding=binding,
        surface=surface.name,
        target_mappings=binding.target_mappings,
        registry=binding.target_registry or runtime_store,
        managed_client=binding.managed_target_client,
        managed_targets=surface.supports_managed_targets,
        dry_run=dry_run,
    )


def _binding_with_runtime_target_registry(
    *,
    binding: DestinationBinding,
    runtime_store: RuntimeStore | None,
) -> DestinationBinding:
    if binding.target_registry is not None or runtime_store is None:
        return binding
    return replace(binding, target_registry=runtime_store)


def _binding_with_resolved_target_mappings(
    *,
    binding: DestinationBinding,
    target_resolution: TargetResolutionEvidence,
) -> DestinationBinding:
    return _binding_with_target_mappings(
        binding=binding,
        target_mappings=_target_mappings_from_resolution(target_resolution),
    )


def _target_mappings_from_resolution(
    target_resolution: TargetResolutionEvidence,
) -> tuple[TargetMapping, ...]:
    if not target_resolution.resolved:
        return ()
    mappings: list[TargetMapping] = []
    seen: set[tuple[str, str]] = set()
    for resolved in target_resolution.resolved:
        if resolved.remote is None:
            continue
        key = (target_resolution.surface, resolved.logical_target)
        if key in seen:
            continue
        mappings.append(
            TargetMapping(
                logical_target=resolved.logical_target,
                remote=resolved.remote,
                surface=target_resolution.surface,
            )
        )
        seen.add(key)
    return tuple(mappings)


def _binding_with_target_mappings(
    *,
    binding: DestinationBinding,
    target_mappings: Sequence[TargetMapping],
) -> DestinationBinding:
    if not target_mappings:
        return binding

    existing = {
        (mapping.surface, mapping.logical_target)
        for mapping in binding.target_mappings
        if mapping.surface is not None
    }
    existing_default = {
        mapping.logical_target for mapping in binding.target_mappings if mapping.surface is None
    }
    mappings = list(binding.target_mappings)
    for mapping in target_mappings:
        if (
            mapping.logical_target in existing_default
            or (mapping.surface, mapping.logical_target) in existing
        ):
            continue
        mappings.append(mapping)
        if mapping.surface is None:
            existing_default.add(mapping.logical_target)
        else:
            existing.add((mapping.surface, mapping.logical_target))
    if len(mappings) == len(binding.target_mappings):
        return binding
    return replace(binding, target_mappings=tuple(mappings))


def _binding_with_connector_managed_target_client(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    resolved_auth: object,
) -> DestinationBinding:
    if binding.managed_target_client is not None:
        return binding
    hook = getattr(binding.connector, "managed_target_client_hook", None)
    if not callable(hook):
        return binding
    managed_client = hook(binding=binding, surface=surface, resolved_auth=resolved_auth)
    if managed_client is None:
        return binding
    return replace(binding, managed_target_client=managed_client)


def _destination_batch_count(
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
) -> int:
    operation_pages = getattr(reconciled, "operation_pages", None)
    if operation_pages is not None:
        return _work_batch_count(operation_pages)
    import_pages = getattr(reconciled, "import_pages", None)
    if import_pages is not None:
        return _work_batch_count(import_pages)
    return 0


def _attempted_record_count(
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
) -> int:
    return int(
        getattr(reconciled, "operation_count", 0) or getattr(reconciled, "import_count", 0) or 0
    )


def _reconciled_work_deferred(
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
) -> bool:
    return (
        getattr(reconciled, "status", None) == "deferred"
        and getattr(reconciled, "operation_pages", None) is None
        and getattr(reconciled, "import_pages", None) is None
    )


def _is_state_reconcile_evidence(
    reconciled: object,
) -> TypeGuard[_StateReconcileEvidenceLike]:
    return getattr(reconciled, "phase", None) == "reconcile" and hasattr(
        reconciled, "operation_pages"
    )


def _submission_evidence(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    delivery_outcome: str,
    attempted_count: int,
    dry_run: bool,
    resolved_auth: object,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
) -> DestinationSubmissionEvidence:
    hook = getattr(binding.connector, "submission_hook", None)
    if callable(hook):
        evidence = hook(
            binding=binding,
            surface=surface,
            delivery_outcome=delivery_outcome,
            attempted_count=attempted_count,
            dry_run=dry_run,
            resolved_auth=resolved_auth,
            reconciled=reconciled,
            selected_request_plans=selected_request_plans,
        )
        if not isinstance(evidence, DestinationSubmissionEvidence):
            raise TypeError(
                "Destination submission_hook must return DestinationSubmissionEvidence."
            )
        return evidence
    if dry_run:
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=True,
            summary="Dry run planned destination work without submission.",
        )
    if binding.destination_ref == "retl/mock":
        return submit_mock_destination(
            surface=surface,
            delivery_outcome=delivery_outcome,
            attempted_count=attempted_count,
            config=binding.config,
            selected_request_plans=selected_request_plans,
        )
    if binding.destination_ref == "retl/reference":
        return submit_reference_destination(
            surface=surface,
            delivery_outcome=delivery_outcome,
            attempted_count=attempted_count,
            config=binding.config,
            selected_request_plans=selected_request_plans,
        )
    return DestinationSubmissionEvidence.planned(
        attempted_count=attempted_count,
        dry_run=False,
        summary="Destination package does not expose deterministic submission evidence.",
    )


def _submission_from_destination_sync_evidence(
    evidence: DestinationSyncEvidence,
) -> DestinationSubmissionEvidence:
    status: Literal[
        "confirmed", "accepted", "retryable_failure", "terminal_record_failure", "planned"
    ]
    if evidence.confirmed_count:
        status = "confirmed"
    elif evidence.accepted_count:
        status = "accepted"
    elif evidence.retryable_failure_count:
        status = "retryable_failure"
    else:
        status = "terminal_record_failure" if evidence.terminal_failure_count else "planned"
    return DestinationSubmissionEvidence(
        status=status,
        attempted_count=evidence.attempted_count,
        confirmed_count=evidence.confirmed_count,
        accepted_count=evidence.accepted_count,
        retryable_failure_count=evidence.retryable_failure_count,
        terminal_record_failure_count=evidence.terminal_failure_count,
        summary="Caller supplied destination submission evidence.",
    )


def _terminal_evidence_from_submission(
    evidence: DestinationSubmissionEvidence,
) -> DestinationSyncEvidence:
    pre_acceptance_retryable = 0
    pre_acceptance_terminal = evidence.pre_acceptance_failure_count
    if evidence.pre_acceptance_failure_count and pre_acceptance_failure_retryable(
        evidence.http_status
    ):
        pre_acceptance_retryable = evidence.pre_acceptance_failure_count
        pre_acceptance_terminal = 0
    return DestinationSyncEvidence(
        attempted_count=evidence.attempted_count,
        confirmed_count=evidence.confirmed_count,
        accepted_count=evidence.accepted_count,
        retryable_failure_count=evidence.retryable_failure_count,
        terminal_failure_count=evidence.terminal_record_failure_count,
        pre_acceptance_failure_count=evidence.pre_acceptance_failure_count,
        pre_acceptance_retryable_failure_count=pre_acceptance_retryable,
        pre_acceptance_terminal_failure_count=pre_acceptance_terminal,
    )


def _combine_commit_decisions(
    *,
    terminal_decision: CommitDecision,
    delivery_decision: DeliveryEvidenceDecision,
) -> CommitDecision:
    if not terminal_decision.allowed:
        return terminal_decision
    return terminal_decision


def _irreversible_writes_planned(
    *,
    target_resolution: TargetResolutionEvidence | None,
) -> bool:
    if target_resolution is None:
        return False
    return not target_resolution.dry_run and target_resolution.managed_created_count > 0


def _managed_target_write_notes(
    *,
    target_resolution: TargetResolutionEvidence | None,
) -> tuple[str, ...]:
    if target_resolution is None or target_resolution.managed_created_count == 0:
        return ()
    return (
        "Managed Target creation performed destination-side writes before row mutation submission.",
    )


def _unique_column_values(table: pa.Table, column_name: str) -> tuple[str, ...]:
    if column_name not in table.column_names:
        return ()
    seen: set[str] = set()
    values: list[str] = []
    column = table.column(column_name).combine_chunks()
    for index in range(table.num_rows):
        value = column[index].as_py()
        if value is None:
            continue
        text = str(value)
        if text not in seen:
            seen.add(text)
            values.append(text)
    return tuple(values)


def _unique_work_column_values(work: object, column_name: str) -> tuple[str, ...]:
    seen: set[str] = set()
    values: list[str] = []
    for batch in _iter_work_batches(work):
        if column_name not in batch.schema.names:
            continue
        column = batch.column(batch.schema.get_field_index(column_name))
        for index in range(batch.num_rows):
            value = column[index].as_py()
            if value is None:
                continue
            text = (
                _logical_target_text(value)
                if column_name in {"target", "target_json"}
                else str(value)
            )
            if text not in seen:
                seen.add(text)
                values.append(text)
    return tuple(values)


def _logical_target_text(value: object) -> str:
    if isinstance(value, Mapping) and set(value) == {"value"}:
        nested = value.get("value")
        return "" if nested is None else str(nested)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return value
        if isinstance(decoded, Mapping) and set(decoded) == {"value"}:
            nested = decoded.get("value")
            return "" if nested is None else str(nested)
    return str(value)


def _work_batch_count(work: object) -> int:
    batch_count = getattr(work, "batch_count", None)
    if isinstance(batch_count, int):
        return batch_count
    if isinstance(work, ColumnarArtifactRef) and work.batch_count is not None:
        return work.batch_count
    return sum(1 for _batch in _iter_work_batches(work))


def _iter_work_batches(work: object) -> Iterable[pa.RecordBatch]:
    if isinstance(work, pa.RecordBatch):
        yield work
        return
    if isinstance(work, tuple | list):
        for item in work:
            yield from _iter_work_batches(item)
        return
    payload = getattr(work, "payload", None)
    if isinstance(payload, pa.RecordBatch):
        yield payload
        return
    if isinstance(work, ColumnarArtifactRef):
        yield from iter_columnar_batches(work)
        return
    iter_record_batches = getattr(work, "iter_record_batches", None)
    if callable(iter_record_batches):
        for batch in iter_record_batches():
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError("Destination work page iterators must yield pyarrow.RecordBatch.")
            yield batch
        return
    batches = getattr(work, "batches", None)
    if batches is not None:
        for batch in batches:
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError("Destination work `batches` must contain pyarrow.RecordBatch.")
            yield batch
        return
    if isinstance(work, Iterable) and not isinstance(work, Mapping | str | bytes):
        for page in work:
            yield from _iter_work_batches(page)
        return
    raise TypeError("Destination sync validation requires bounded Arrow work pages.")


def _submission_phase(
    *,
    sync_name: str,
    surface: DestinationSurface,
    dry_run: bool,
    destination_batch_count: int,
    target_resolution: TargetResolutionEvidence | None,
    irreversible_writes: bool,
    submission: DestinationSubmissionEvidence,
    progress_decision: CommitDecision,
    progress_advanced: bool,
    status: Literal["deferred", "succeeded", "failed"],
) -> PhaseStatus:
    target_message = ""
    if target_resolution is not None:
        target_message = (
            f" Target resolution status: {target_resolution.status}; "
            f"{target_resolution.target_count} logical target(s)."
        )
    return PhaseStatus(
        name="sync",
        status=status,
        evidence=PhaseEvidence(
            kind="deferred" if dry_run else "planned",
            message=(
                f"Destination Surface `{surface.name}` validated for Sync `{sync_name}`; "
                f"{destination_batch_count} destination batch(es) planned."
                f"{target_message} "
                f"Destination batch status: {_ledger_status_value(submission.status)}. "
                f"Progress decision: {progress_decision.reason}"
            ),
            dry_run=dry_run,
            irreversible_writes=irreversible_writes,
            progress_advanced=progress_advanced,
        ),
    )


def _ledger_status_value(value: object) -> str:
    return {
        "confirmed": "succeeded",
        "terminal_record_failure": "failed",
        "pre_acceptance_failure": "failed",
        "retryable_failure": "failed",
        "planned": "pending",
    }.get(str(value), str(value))


def _log_info(event: str, **context: object) -> None:
    LOGGER.info(event, extra=_log_extra(event, **context))


def _log_warning(event: str, **context: object) -> None:
    LOGGER.warning(event, extra=_log_extra(event, **context))


def _log_error(event: str, **context: object) -> None:
    LOGGER.error(event, extra=_log_extra(event, **context))


def _log_extra(event: str, **context: object) -> dict[str, object]:
    extra: dict[str, object] = {"event": event}
    for key, value in context.items():
        if value is not None:
            extra[key] = value
    return extra


def _destination_context(
    *,
    sync: Sync,
    binding: DestinationBinding,
    surface: DestinationSurface,
    dry_run: bool,
    run_id: str | None,
    attempt_id: str | None,
    status: str,
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "attempt_id": attempt_id,
        "sync_name": sync.name,
        "declaration_name": sync.declaration.name,
        "declaration_kind": _declaration_kind(sync),
        "destination_binding_name": binding.binding_name,
        "surface": surface.name,
        "status": status,
        "dry_run": dry_run,
    }


def _declaration_kind(sync: Sync) -> Literal["state", "event"]:
    return "state" if isinstance(sync.declaration, State) else "event"


def _target_resolution_context(evidence: TargetResolutionEvidence) -> dict[str, object]:
    context: dict[str, object] = {
        "target_count": evidence.target_count,
        "mapped_count": evidence.mapped_count,
        "registry_count": evidence.registry_count,
        "managed_created_count": evidence.managed_created_count,
        "planned_create_count": evidence.planned_create_count,
    }
    if evidence.failure_details:
        failure = evidence.failure_details[0]
        context.update(
            {
                "target_failure_action": failure.action,
                "target_failure_category": failure.category,
                "http_status": failure.http_status,
                "partner_error_code": failure.partner_error_code,
                "partner_error_subcode": failure.partner_error_subcode,
                "partner_error_detail": redact_text(
                    sanitize_partner_error_detail(failure.partner_error_detail) or ""
                ),
            }
        )
    return context


def _target_resolution_failure_summary(evidence: TargetResolutionEvidence) -> str:
    if evidence.failure_details:
        return "; ".join(
            _format_target_resolution_failure(failure) for failure in evidence.failure_details
        )
    return "; ".join(evidence.failures)


def _format_target_resolution_failure(failure: TargetResolutionFailure) -> str:
    parts = [f"target=`{failure.logical_target}`"]
    if failure.action:
        parts.append(f"action={failure.action}")
    if failure.http_status is not None:
        parts.append(f"http_status={failure.http_status}")
    if failure.category:
        parts.append(f"category={failure.category}")
    if failure.partner_error_code:
        parts.append(f"partner_code={failure.partner_error_code}")
    if failure.partner_error_subcode:
        parts.append(f"partner_subcode={failure.partner_error_subcode}")
    parts.append(f"summary={failure.summary}")
    if failure.partner_error_detail:
        parts.append(f"partner_detail={failure.partner_error_detail}")
    return " ".join(parts)


def _submission_summary_context(
    evidence: DestinationSubmissionEvidence,
    *,
    request_batch_count: int | None = None,
) -> dict[str, object]:
    return {
        "attempted_count": evidence.attempted_count,
        "confirmed_count": evidence.confirmed_count,
        "accepted_count": evidence.accepted_count,
        "request_batch_count": (
            evidence.request_batch_count if request_batch_count is None else request_batch_count
        ),
        "retryable_failure_count": evidence.retryable_failure_count,
        "terminal_failure_count": evidence.terminal_record_failure_count,
        "pre_acceptance_failure_count": evidence.pre_acceptance_failure_count,
    }


def _submission_failure_context(
    evidence: DestinationSubmissionEvidence,
    *,
    request_batch_count: int | None = None,
) -> dict[str, object]:
    context = _submission_summary_context(evidence, request_batch_count=request_batch_count)
    context.update(
        {
            "http_status": evidence.http_status,
            "partner_error_code": evidence.partner_error_code,
            "partner_error_subcode": evidence.partner_error_subcode,
            "retry_after_seconds": evidence.retry_after_seconds,
            "partner_error_detail": redact_text(
                sanitize_partner_error_detail(evidence.partner_error_detail) or ""
            ),
        }
    )
    return context


def _submission_failure_category(evidence: DestinationSubmissionEvidence) -> str:
    if evidence.status == "pre_acceptance_failure":
        return evidence.pre_acceptance_failure_category or "pre_acceptance"
    if evidence.status == "retryable_failure":
        return "retryable"
    return "terminal_record"


__all__ = ["SyncPhaseEvidence", "retry_destination_batches", "sync_destination"]
