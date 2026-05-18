from __future__ import annotations

import logging
from dataclasses import dataclass

import pyarrow as pa  # type: ignore[import-untyped]

from retl.declarations import DestinationBinding, Event, State, Sync
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.surfaces import DestinationSurface
from retl.destinations.terminal_failures import (
    CommitDecision,
    DestinationSyncEvidence,
    decide_progress_commit,
    evaluate_failure_mode,
    pre_acceptance_failure_retryable,
)
from retl.errors import DeclarationValidationError
from retl.runtime.redaction import redact_text
from retl.stores.contracts import (
    CanonicalKey,
    CanonicalKeyScalar,
    DestinationBatchLedgerStore,
    DestinationBatchRecord,
    DestinationProgress,
    DestinationProgressScope,
    DestinationProgressUpdate,
    EventKeysetScanPosition,
    OrderedWorkStore,
    ScanPosition,
    StateCurrentCursor,
    StateCurrentSnapshotScanPosition,
    StateOrderedWorkScanPosition,
    WorkFamily,
    compare_scan_positions,
)

LOGGER = logging.getLogger("retl.runtime.progress")


@dataclass(frozen=True)
class ProgressAdvance:
    progress: DestinationProgressUpdate
    decision: CommitDecision


@dataclass(frozen=True)
class SyncProgressAdvance:
    progress: DestinationProgressUpdate
    progress_decision: CommitDecision


def destination_progress_scope(sync: Sync) -> DestinationProgressScope:
    if not isinstance(sync, Sync):
        raise DeclarationValidationError("Destination progress scope requires a Sync.")
    destination_name = _destination_name(sync.destination)
    declaration = sync.declaration
    if isinstance(declaration, State):
        family: WorkFamily = "state"
    elif isinstance(declaration, Event):
        family = "event"
    else:
        raise DeclarationValidationError(
            "Destination progress scope requires a State or Event Sync declaration."
        )
    return DestinationProgressScope(
        sync_name=sync.name,
        destination_name=destination_name,
        surface=sync.surface,
        family=family,
        declaration_name=declaration.name,
    )


def register_destination_progress(
    *,
    store: OrderedWorkStore,
    sync: Sync,
) -> DestinationProgress:
    return store.register_destination_progress(destination_progress_scope(sync))


def read_destination_progress(
    *,
    store: OrderedWorkStore,
    sync: Sync,
) -> DestinationProgress:
    return store.get_destination_progress(destination_progress_scope(sync))


def decide_destination_progress(
    *,
    sync: Sync,
    surface: DestinationSurface,
    evidence: DestinationSubmissionEvidence | None,
    dry_run: bool = False,
) -> CommitDecision:
    _validate_sync_surface(sync=sync, surface=surface)
    terminal_decision = decide_progress_commit(
        delivery_outcome=surface.delivery_outcome,
        on_failure=sync.on_failure,
        destination_evidence=(
            None if evidence is None else _destination_sync_evidence_from_submission(evidence)
        ),
        dry_run=dry_run or bool(getattr(evidence, "dry_run", False)),
    )
    if not terminal_decision.allowed or evidence is None:
        return terminal_decision

    return terminal_decision


def advance_progress_after_sync(
    *,
    store: DestinationBatchLedgerStore,
    sync: Sync,
    surface: DestinationSurface,
    reconciled: object,
    evidence: DestinationSubmissionEvidence | None,
    dry_run: bool = False,
    destination_batches: tuple[DestinationBatchRecord, ...] = (),
    current_position: ScanPosition | None = None,
    current_position_loaded: bool = False,
    run_id: str | None = None,
    attempt_id: str | None = None,
    page_index: int | None = None,
) -> SyncProgressAdvance:
    """Advance runtime cursors after Sync submission for a safe reconciled page."""

    _validate_sync_surface(sync=sync, surface=surface)
    if isinstance(sync.declaration, State):
        return _advance_state_progress_after_sync(
            store=store,
            sync=sync,
            surface=surface,
            reconciled=reconciled,
            evidence=evidence,
            dry_run=dry_run,
            destination_batches=destination_batches,
            current_position=current_position,
            current_position_loaded=current_position_loaded,
            run_id=run_id,
            attempt_id=attempt_id,
            page_index=page_index,
        )

    if isinstance(sync.declaration, Event):
        return _advance_event_progress_after_sync(
            store=store,
            sync=sync,
            surface=surface,
            reconciled=reconciled,
            evidence=evidence,
            dry_run=dry_run,
            destination_batches=destination_batches,
            current_position=current_position,
            current_position_loaded=current_position_loaded,
            run_id=run_id,
            attempt_id=attempt_id,
            page_index=page_index,
        )

    raise DeclarationValidationError(
        "Destination progress advancement requires a State or Event Sync declaration."
    )


def _advance_event_progress_after_sync(
    *,
    store: DestinationBatchLedgerStore,
    sync: Sync,
    surface: DestinationSurface,
    reconciled: object,
    evidence: DestinationSubmissionEvidence | None,
    dry_run: bool,
    destination_batches: tuple[DestinationBatchRecord, ...],
    current_position: ScanPosition | None,
    current_position_loaded: bool,
    run_id: str | None,
    attempt_id: str | None,
    page_index: int | None,
) -> SyncProgressAdvance:
    position = _event_scanned_upper_position(sync=sync, reconciled=reconciled)
    if position is None:
        decision = CommitDecision(
            subject="progress",
            allowed=False,
            reason=_event_progress_block_reason(reconciled=reconciled, dry_run=dry_run),
        )
        progress = _unchanged_progress(
            store=store,
            sync=sync,
            current_position=current_position,
            current_position_loaded=current_position_loaded,
        )
        _log_progress_decided(
            sync=sync,
            surface=surface,
            decision=decision,
            progress=progress,
            dry_run=dry_run,
            run_id=run_id,
            attempt_id=attempt_id,
            page_index=page_index,
            expected_batch_count=evidence.request_batch_count if evidence is not None else None,
            planned_batch_count=len(destination_batches),
        )
        return SyncProgressAdvance(
            progress=progress,
            progress_decision=decision,
        )
    advanced = _advance_scan_destination_progress(
        store=store,
        sync=sync,
        surface=surface,
        position=position,
        evidence=evidence,
        dry_run=dry_run,
        destination_batches=destination_batches,
        current_position=current_position,
        current_position_loaded=current_position_loaded,
        family_label="Event",
        run_id=run_id,
        attempt_id=attempt_id,
        page_index=page_index,
    )
    return SyncProgressAdvance(
        progress=advanced.progress,
        progress_decision=advanced.decision,
    )


def _advance_state_progress_after_sync(
    *,
    store: DestinationBatchLedgerStore,
    sync: Sync,
    surface: DestinationSurface,
    reconciled: object,
    evidence: DestinationSubmissionEvidence | None,
    dry_run: bool,
    destination_batches: tuple[DestinationBatchRecord, ...],
    current_position: ScanPosition | None,
    current_position_loaded: bool,
    run_id: str | None,
    attempt_id: str | None,
    page_index: int | None,
) -> SyncProgressAdvance:
    position = _state_scanned_upper_position(reconciled)
    if position is None:
        decision = CommitDecision(
            subject="progress",
            allowed=False,
            reason=_state_progress_block_reason(reconciled=reconciled, dry_run=dry_run),
        )
        progress = _unchanged_progress(
            store=store,
            sync=sync,
            current_position=current_position,
            current_position_loaded=current_position_loaded,
        )
        _log_progress_decided(
            sync=sync,
            surface=surface,
            decision=decision,
            progress=progress,
            dry_run=dry_run,
            run_id=run_id,
            attempt_id=attempt_id,
            page_index=page_index,
            expected_batch_count=evidence.request_batch_count if evidence is not None else None,
            planned_batch_count=len(destination_batches),
        )
        return SyncProgressAdvance(
            progress=progress,
            progress_decision=decision,
        )
    advanced = _advance_scan_destination_progress(
        store=store,
        sync=sync,
        surface=surface,
        position=position,
        evidence=evidence,
        dry_run=dry_run,
        destination_batches=destination_batches,
        current_position=current_position,
        current_position_loaded=current_position_loaded,
        family_label="State",
        run_id=run_id,
        attempt_id=attempt_id,
        page_index=page_index,
    )
    return SyncProgressAdvance(
        progress=advanced.progress,
        progress_decision=advanced.decision,
    )


def _advance_scan_destination_progress(
    *,
    store: DestinationBatchLedgerStore,
    sync: Sync,
    surface: DestinationSurface,
    position: ScanPosition,
    evidence: DestinationSubmissionEvidence | None,
    dry_run: bool,
    destination_batches: tuple[DestinationBatchRecord, ...],
    current_position: ScanPosition | None,
    current_position_loaded: bool,
    family_label: str,
    run_id: str | None,
    attempt_id: str | None,
    page_index: int | None,
) -> ProgressAdvance:
    decision = _decide_scan_destination_ledger_progress(
        store=store,
        sync=sync,
        surface=surface,
        position=position,
        evidence=evidence,
        dry_run=dry_run,
        destination_batches=destination_batches,
        current_position=current_position,
        current_position_loaded=current_position_loaded,
        family_label=family_label,
    )
    if not decision.allowed:
        progress = _unchanged_progress(
            store=store,
            sync=sync,
            current_position=current_position,
            current_position_loaded=current_position_loaded,
        )
        _log_progress_decided(
            sync=sync,
            surface=surface,
            decision=decision,
            progress=progress,
            dry_run=dry_run,
            run_id=run_id,
            attempt_id=attempt_id,
            page_index=page_index,
            expected_batch_count=evidence.request_batch_count if evidence is not None else None,
            planned_batch_count=len(destination_batches),
        )
        return ProgressAdvance(
            progress=progress,
            decision=decision,
        )
    scope = destination_progress_scope(sync)
    progress = store.update_destination_progress(
        scope=scope,
        position=position,
        current_position=current_position,
        current_position_loaded=current_position_loaded,
    )
    _log_progress_decided(
        sync=sync,
        surface=surface,
        decision=decision,
        progress=progress,
        dry_run=dry_run,
        run_id=run_id,
        attempt_id=attempt_id,
        page_index=page_index,
        expected_batch_count=evidence.request_batch_count if evidence is not None else None,
        planned_batch_count=len(destination_batches),
    )
    return ProgressAdvance(
        progress=progress,
        decision=decision,
    )


def _decide_scan_destination_ledger_progress(
    *,
    store: DestinationBatchLedgerStore,
    sync: Sync,
    surface: DestinationSurface,
    position: ScanPosition,
    evidence: DestinationSubmissionEvidence | None,
    dry_run: bool,
    destination_batches: tuple[DestinationBatchRecord, ...],
    current_position: ScanPosition | None,
    current_position_loaded: bool,
    family_label: str,
) -> CommitDecision:
    _validate_sync_surface(sync=sync, surface=surface)
    if dry_run or bool(getattr(evidence, "dry_run", False)):
        return CommitDecision(
            subject="progress",
            allowed=False,
            reason="Dry run does not advance destination progress.",
        )
    if evidence is None:
        return CommitDecision(
            subject="progress",
            allowed=False,
            reason="Destination progress waits for destination submission evidence.",
        )
    scope = destination_progress_scope(sync)
    if not current_position_loaded:
        current_position = store.get_destination_progress(scope).position
    if current_position is not None:
        try:
            comparison = compare_scan_positions(position, current_position)
        except ValueError as exc:
            return CommitDecision(
                subject="progress",
                allowed=False,
                reason=str(exc),
            )
        if comparison < 0:
            raise DeclarationValidationError(
                "Destination progress cannot move behind the current scan position."
            )
    batches = _current_durable_batches_for_position(
        scope=scope,
        position=position,
        destination_batches=destination_batches,
    )
    if not batches:
        return CommitDecision(
            subject="progress",
            allowed=False,
            reason="Destination progress waits for destination batch ledger records.",
        )
    expected_batch_count = evidence.request_batch_count
    if expected_batch_count and len(batches) < expected_batch_count:
        return CommitDecision(
            subject="progress",
            allowed=False,
            reason="Destination progress waits for all planned destination batch records.",
        )
    failure_evaluation = evaluate_failure_mode(
        mode=sync.on_failure,
        attempted_count=len(batches),
        terminal_failure_count=sum(
            1 for batch in batches if batch.status == "failed" and batch.retry_eligible is not True
        ),
        retryable_failure_count=sum(
            1 for batch in batches if batch.status == "failed" and batch.retry_eligible is True
        ),
    )
    if not failure_evaluation.allowed:
        return CommitDecision(
            subject="progress",
            allowed=False,
            reason=failure_evaluation.reason,
            failure_mode_evaluation=failure_evaluation,
        )
    return CommitDecision(
        subject="progress",
        allowed=True,
        reason=f"Destination batch ledger durably records the current {family_label} scan page.",
        failure_mode_evaluation=failure_evaluation,
    )


def _current_durable_batches_for_position(
    *,
    scope: DestinationProgressScope,
    position: ScanPosition,
    destination_batches: tuple[DestinationBatchRecord, ...],
) -> tuple[DestinationBatchRecord, ...]:
    durable_batches: list[DestinationBatchRecord] = []
    for batch in destination_batches:
        if batch.identity.scope != scope:
            continue
        source_range = batch.identity.source_range
        if source_range is None:
            continue
        try:
            upper_comparison = compare_scan_positions(
                source_range.upper_bound_inclusive,
                position,
            )
        except ValueError as exc:
            raise DeclarationValidationError(str(exc)) from exc
        if upper_comparison <= 0:
            durable_batches.append(batch)
    if not any(
        batch.identity.source_range is not None
        and compare_scan_positions(batch.identity.source_range.upper_bound_inclusive, position) == 0
        for batch in durable_batches
    ):
        return ()
    return tuple(durable_batches)


def _state_progress_block_reason(*, reconciled: object, dry_run: bool) -> str:
    if dry_run or bool(getattr(reconciled, "dry_run", False)):
        return "Dry run does not advance destination progress."
    if _reconciled_row_count(reconciled) == 0:
        return "No reconciled work was submitted; progress remains unchanged."
    return "Reconciled State page has no typed scan-position boundary."


def _event_progress_block_reason(*, reconciled: object, dry_run: bool) -> str:
    if dry_run or bool(getattr(reconciled, "dry_run", False)):
        return "Dry run does not advance destination progress."
    if _reconciled_row_count(reconciled) == 0:
        return "No reconciled work was submitted; progress remains unchanged."
    return "Reconciled Event page has no source-native keyset boundary."


def _event_scanned_upper_position(
    *,
    sync: Sync,
    reconciled: object,
) -> EventKeysetScanPosition | None:
    if _reconciled_row_count(reconciled) == 0:
        return None
    pages = getattr(reconciled, "import_pages", None)
    if pages is None:
        return None
    for page in reversed(tuple(pages)):
        payload = getattr(page, "payload", None)
        if not isinstance(payload, pa.RecordBatch) or payload.num_rows == 0:
            continue
        checkpoint = sync.declaration.source.checkpoint
        if checkpoint is None:
            raise DeclarationValidationError("Event declaration requires checkpoint types.")
        position = _event_position_from_payload(
            payload,
            payload.num_rows - 1,
            cursor_kind=checkpoint["cursor_type"],
            primary_key_kind=checkpoint["primary_key_type"],
        )
        if position is not None:
            return position
    return None


def _event_position_from_payload(
    payload: pa.RecordBatch,
    index: int,
    *,
    cursor_kind: str,
    primary_key_kind: str,
) -> EventKeysetScanPosition | None:
    cursor_index = payload.schema.get_field_index("event_cursor_value")
    primary_key_index = payload.schema.get_field_index("event_primary_key_value")
    if cursor_index < 0 or primary_key_index < 0:
        return None
    cursor_value = payload.column(cursor_index)[index].as_py()
    primary_key_value = payload.column(primary_key_index)[index].as_py()
    if cursor_value is None or primary_key_value is None:
        return None
    return EventKeysetScanPosition(
        cursor_value=_event_scalar_from_text(str(cursor_value), cursor_kind),
        primary_key_value=_event_scalar_from_text(str(primary_key_value), primary_key_kind),
    )


def _state_scanned_upper_position(reconciled: object) -> ScanPosition | None:
    if _reconciled_row_count(reconciled) == 0:
        return None
    mode = getattr(reconciled, "mode", None)
    if mode == "pending":
        boundary = getattr(reconciled, "progress_boundary", None)
        collect_id = getattr(boundary, "last_collect_id", None)
        sequence_order = getattr(boundary, "last_sequence_order", None)
        if collect_id is None or sequence_order is None:
            return None
        return StateOrderedWorkScanPosition(
            collect_id=str(collect_id),
            sequence_order=int(sequence_order),
        )
    if mode == "resend_all":
        next_cursor = getattr(reconciled, "next_cursor", None)
        if isinstance(next_cursor, StateCurrentCursor):
            return next_cursor.position
        return _last_current_snapshot_position(reconciled)
    return None


def _last_current_snapshot_position(reconciled: object) -> StateCurrentSnapshotScanPosition | None:
    pages = getattr(reconciled, "operation_pages", None)
    if pages is None:
        return None
    for page in reversed(tuple(pages)):
        payload = getattr(page, "payload", None)
        if not isinstance(payload, pa.RecordBatch) or payload.num_rows == 0:
            continue
        position = _current_snapshot_position_from_payload(payload, payload.num_rows - 1)
        if position is not None:
            return position
    return None


def _current_snapshot_position_from_payload(
    payload: pa.RecordBatch,
    index: int,
) -> StateCurrentSnapshotScanPosition | None:
    identity_index = payload.schema.get_field_index("identity_json")
    if identity_index < 0:
        return None
    identity = payload.column(identity_index)[index].as_py()
    if not isinstance(identity, str) or not identity.strip():
        return None
    return StateCurrentSnapshotScanPosition(
        key=CanonicalKey.of(CanonicalKeyScalar.string(identity))
    )


def _event_scalar_from_text(value: str, kind: str) -> CanonicalKeyScalar:
    if kind == "string":
        return CanonicalKeyScalar.string(value)
    if kind == "integer":
        return CanonicalKeyScalar.integer(int(value))
    if kind == "number":
        return CanonicalKeyScalar.number(float(value))
    if kind == "boolean":
        lowered = value.casefold()
        if lowered in {"true", "1"}:
            return CanonicalKeyScalar.boolean(True)
        if lowered in {"false", "0"}:
            return CanonicalKeyScalar.boolean(False)
    raise DeclarationValidationError("Event checkpoint scalar type is not supported.")


def _validate_sync_surface(*, sync: Sync, surface: DestinationSurface) -> None:
    scope = destination_progress_scope(sync)
    if surface.name != sync.surface:
        raise DeclarationValidationError(
            f"Sync `{sync.name}` targets surface `{sync.surface}`, "
            f"but progress was evaluated against `{surface.name}`."
        )
    if surface.declaration_family != scope.family:
        raise DeclarationValidationError(
            f"Sync `{sync.name}` uses {scope.family} work, "
            f"but surface `{surface.name}` accepts {surface.declaration_family} work."
        )


def _reconciled_row_count(reconciled: object) -> int:
    return int(
        getattr(reconciled, "operation_count", 0) or getattr(reconciled, "import_count", 0) or 0
    )


def _unchanged_progress(
    *,
    store: OrderedWorkStore,
    sync: Sync,
    current_position: ScanPosition | None = None,
    current_position_loaded: bool = False,
) -> DestinationProgressUpdate:
    scope = destination_progress_scope(sync)
    before = (
        current_position
        if current_position_loaded
        else store.get_destination_progress(scope).position
    )
    return store.update_destination_progress(
        scope=scope,
        position=before,
        advance=False,
        current_position=before,
        current_position_loaded=True,
    )


def _destination_name(destination: object) -> str:
    if isinstance(destination, DestinationBinding):
        return destination.binding_name
    binding_name = getattr(destination, "binding_name", None)
    if isinstance(binding_name, str) and binding_name.strip():
        return binding_name
    raise DeclarationValidationError(
        "Destination progress scope requires a destination binding name."
    )


def _destination_sync_evidence_from_submission(
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


def _log_progress_decided(
    *,
    sync: Sync,
    surface: DestinationSurface,
    decision: CommitDecision,
    progress: DestinationProgressUpdate,
    dry_run: bool,
    run_id: str | None,
    attempt_id: str | None,
    page_index: int | None,
    expected_batch_count: int | None,
    planned_batch_count: int,
) -> None:
    LOGGER.info(
        "progress_commit_decided",
        extra=_log_extra(
            "progress_commit_decided",
            run_id=run_id,
            attempt_id=attempt_id,
            sync_name=sync.name,
            declaration_name=sync.declaration.name,
            destination_binding_name=destination_progress_scope(sync).destination_name,
            surface=surface.name,
            status="allowed" if decision.allowed else "blocked",
            dry_run=dry_run,
            page_index=page_index,
            progress_decision_allowed=decision.allowed,
            progress_advanced=progress.advanced,
            reason=redact_text(decision.reason),
            expected_batch_count=expected_batch_count,
            planned_batch_count=planned_batch_count,
        ),
    )


def _log_extra(event: str, **context: object) -> dict[str, object]:
    extra: dict[str, object] = {"event": event}
    for key, value in context.items():
        if value is not None:
            extra[key] = value
    return extra


__all__ = [
    "ProgressAdvance",
    "SyncProgressAdvance",
    "advance_progress_after_sync",
    "decide_destination_progress",
    "destination_progress_scope",
    "read_destination_progress",
    "register_destination_progress",
]
