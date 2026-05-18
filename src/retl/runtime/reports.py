from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from retl.declarations import FailureHandlingMode
from retl.destinations.receipts import sanitize_partner_error_detail
from retl.runtime.redaction import redact_text, redact_value
from retl.runtime.results import PhaseStatus
from retl.stores.contracts import scan_position_to_jsonable


@dataclass(frozen=True)
class ReportReference:
    ref: str
    kind: Literal["sync_report", "run_index"]


@dataclass(frozen=True)
class PhaseSummary:
    name: str
    status: str
    message: str
    dry_run: bool
    irreversible_writes: bool = False
    progress_advanced: bool = False


@dataclass(frozen=True)
class ReconcileSummary:
    operation_count: int = 0
    upsert_count: int = 0
    remove_count: int = 0
    event_import_count: int = 0


@dataclass(frozen=True)
class DestinationSummary:
    binding_name: str | None
    surface: str
    surface_family: str | None = None
    execution_mode: str | None = None
    compatibility_status: str | None = None
    target_resolution_status: str | None = None
    target_count: int = 0
    target_mapped_count: int = 0
    target_registry_count: int = 0
    target_managed_created_count: int = 0
    target_planned_create_count: int = 0
    delivery_outcome: str = ""
    submission_status: str = ""
    delivery_decision_reason: str = ""
    request_batch_count: int = 0
    destination_batch_count: int = 0
    destination_batch_ids: tuple[str, ...] = ()
    attempted_count: int = 0
    confirmed_count: int = 0
    accepted_count: int = 0
    retryable_failure_count: int = 0
    terminal_failure_count: int = 0
    pre_acceptance_failure_count: int = 0
    failure_category: str | None = None
    http_status: int | None = None
    last_error_summary: str = ""
    last_error_detail: str | None = None


@dataclass(frozen=True)
class ProgressSummary:
    scope: Mapping[str, object]
    stage_mode: str | None = None
    page_count: int = 0
    staged_row_count: int = 0
    safe_to_advance_collect_id: bool = False
    before: Mapping[str, object] | None = None
    after: Mapping[str, object] | None = None
    advanced: bool = False
    decision_allowed: bool | None = None
    decision_reason: str = ""


@dataclass(frozen=True)
class FailureSummary:
    retryable_count: int = 0
    terminal_count: int = 0
    pre_acceptance_count: int = 0
    on_failure: FailureHandlingMode = "continue_on_any"
    decision: str = "not_evaluated"
    category: str | None = None


@dataclass(frozen=True)
class CommitSummary:
    progress_advanced: bool
    reason: str


@dataclass(frozen=True)
class RetentionSummary:
    watermark_collect_id: str | None = None


@dataclass(frozen=True)
class SyncReport:
    ref: ReportReference
    report_id: str
    runner_name: str
    run_id: str
    attempt_id: str
    sync_name: str
    declaration_name: str
    declaration_version_id: str | None
    declaration_kind: Literal["state", "event"]
    destination_binding_name: str | None
    surface: str
    dry_run: bool
    status: str
    on_failure: FailureHandlingMode
    phases: tuple[PhaseSummary, ...]
    reconcile: ReconcileSummary
    destination: DestinationSummary
    progress: ProgressSummary
    failures: FailureSummary
    commit: CommitSummary
    retention: RetentionSummary = dataclasses.field(default_factory=RetentionSummary)

    def to_dict(self) -> dict[str, object]:
        return _to_redacted_data(self)

    def to_text(self) -> str:
        lines = [
            f"Sync Report: {redact_text(self.sync_name)}",
            f"Report ID: {redact_text(self.report_id)}",
            f"Report Reference: {self.ref.ref}",
            f"Run: {redact_text(self.run_id)} attempt={redact_text(self.attempt_id)}",
            f"Declaration: {redact_text(self.declaration_name)} ({self.declaration_kind})",
            f"Destination Surface: {redact_text(self.surface)}",
            f"Status: {self.status}",
            f"Dry Run: {self.dry_run}",
            "Phases:",
        ]
        lines.extend(
            f"- {phase.name}: {phase.status}; {redact_text(phase.message)}" for phase in self.phases
        )
        lines.extend(
            [
                (
                    "Progress: "
                    f"scope={_format_mapping(self.progress.scope)}, "
                    f"pages={self.progress.page_count}, "
                    f"stage_mode={self.progress.stage_mode}, "
                    f"advanced={self.progress.advanced}, "
                    f"allowed={self.progress.decision_allowed}"
                ),
                (
                    "Reconcile: "
                    f"operations={self.reconcile.operation_count}, "
                    f"imports={self.reconcile.event_import_count}"
                ),
                (
                    "Destination: "
                    f"request_batches={self.destination.request_batch_count}, "
                    f"destination_batches={self.destination.destination_batch_count}, "
                    f"confirmed={self.destination.confirmed_count}, "
                    f"accepted={self.destination.accepted_count}, "
                    f"retryable={self.destination.retryable_failure_count}, "
                    f"terminal={self.destination.terminal_failure_count}, "
                    f"blocking={self.destination.pre_acceptance_failure_count}"
                ),
                (
                    "Destination Batch IDs: "
                    f"{', '.join(self.destination.destination_batch_ids) or 'none'}"
                ),
                (
                    "Commit: "
                    f"progress_advanced={self.commit.progress_advanced}; "
                    f"{redact_text(self.commit.reason)}"
                ),
            ]
        )
        if self.destination.last_error_detail:
            detail = sanitize_partner_error_detail(self.destination.last_error_detail)
            if detail:
                lines.append(f"Partner Error Detail: {detail}")
        if self.destination.last_error_summary:
            lines.append(f"Last Error Summary: {redact_text(self.destination.last_error_summary)}")
        return "\n".join(lines)


@dataclass(frozen=True)
class RunIndexEntry:
    sync_name: str
    report_id: str
    report_ref: ReportReference
    status: str
    destination_binding_name: str | None
    surface: str
    operation_count: int
    event_import_count: int
    progress_advanced: bool


@dataclass(frozen=True)
class RunIndex:
    ref: ReportReference
    runner_name: str
    run_id: str
    status: str
    dry_run: bool
    source_groups: tuple[Mapping[str, object], ...]
    declaration_stages: tuple[Mapping[str, object], ...]
    syncs: tuple[RunIndexEntry, ...]

    def to_dict(self) -> dict[str, object]:
        return _to_redacted_data(self)

    def to_text(self) -> str:
        lines = [
            f"Run Index: {redact_text(self.run_id)}",
            f"Runner: {redact_text(self.runner_name)}",
            f"Status: {self.status}",
            f"Dry Run: {self.dry_run}",
            "Sync Reports:",
        ]
        lines.extend(
            (
                f"- {redact_text(entry.sync_name)} -> {entry.report_id} "
                f"({entry.report_ref.ref}); status={entry.status}"
            )
            for entry in self.syncs
        )
        return "\n".join(lines)


def build_run_index(
    *,
    runner_name: str,
    run_id: str,
    status: str,
    dry_run: bool,
    source_groups: Iterable[object],
    declaration_stages: Iterable[object],
    reports: Sequence[SyncReport],
) -> RunIndex:
    return RunIndex(
        ref=ReportReference(
            ref=f"run-index:{_stable_hash(runner_name, run_id)}",
            kind="run_index",
        ),
        runner_name=runner_name,
        run_id=run_id,
        status=status,
        dry_run=dry_run,
        source_groups=tuple(
            {
                "group_key": getattr(group, "group_key", None),
                "source_name": getattr(group, "source_name", None),
                "source_mode": getattr(group, "source_mode", None),
                "sync_names": tuple(getattr(group, "sync_names", ())),
            }
            for group in source_groups
        ),
        declaration_stages=tuple(
            {
                "group_key": getattr(stage, "group_key", None),
                "declaration_name": getattr(stage, "declaration_name", None),
                "declaration_kind": getattr(stage, "declaration_kind", None),
                "sync_names": tuple(getattr(stage, "sync_names", ())),
            }
            for stage in declaration_stages
        ),
        syncs=tuple(
            RunIndexEntry(
                sync_name=report.sync_name,
                report_id=report.report_id,
                report_ref=report.ref,
                status=report.status,
                destination_binding_name=report.destination_binding_name,
                surface=report.surface,
                operation_count=report.reconcile.operation_count,
                event_import_count=report.reconcile.event_import_count,
                progress_advanced=report.commit.progress_advanced,
            )
            for report in reports
        ),
    )


def sync_report_ref(*, runner_name: str, sync_name: str) -> ReportReference:
    return ReportReference(
        ref=f"sync-report:{_stable_hash(runner_name, sync_name)}",
        kind="sync_report",
    )


def sync_report_id(*, run_id: str, attempt_id: str, sync_name: str) -> str:
    return f"{run_id}:{attempt_id}:{sync_name}"


def phase_summary(phase: PhaseStatus) -> PhaseSummary:
    return PhaseSummary(
        name=phase.name,
        status=phase.status,
        message=redact_text(phase.evidence.message),
        dry_run=phase.evidence.dry_run,
        irreversible_writes=phase.evidence.irreversible_writes,
        progress_advanced=phase.evidence.progress_advanced,
    )


def scan_position_summary(value: object) -> Mapping[str, object] | None:
    if value is None:
        return None
    try:
        return scan_position_to_jsonable(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def progress_scope_summary(value: object) -> Mapping[str, object]:
    return {
        "sync_name": getattr(value, "sync_name", None),
        "destination_name": getattr(value, "destination_name", None),
        "surface": getattr(value, "surface", None),
        "family": getattr(value, "family", None),
        "declaration_name": getattr(value, "declaration_name", None),
    }


def _format_mapping(value: Mapping[str, object]) -> str:
    return ",".join(f"{key}={redact_text(item)}" for key, item in value.items())


def _to_redacted_data(value: object) -> dict[str, object]:
    converted = _convert(value, field_name="")
    if not isinstance(converted, dict):
        raise TypeError("Report serialization produced a non-dict root.")
    return converted


def _convert(value: object, *, field_name: str) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _convert(getattr(value, field.name), field_name=field.name)
            for field in dataclasses.fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): _convert(redact_value(str(key), item), field_name=str(key))
            for key, item in value.items()
        }
    if isinstance(value, tuple | list):
        return tuple(_convert(item, field_name=field_name) for item in value)
    return redact_value(field_name, value)


def _stable_hash(*parts: object) -> str:
    payload = json.dumps(tuple(str(part) for part in parts), sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


__all__ = [
    "CommitSummary",
    "DestinationSummary",
    "FailureSummary",
    "PhaseSummary",
    "ProgressSummary",
    "ReconcileSummary",
    "ReportReference",
    "RetentionSummary",
    "RunIndex",
    "RunIndexEntry",
    "SyncReport",
    "build_run_index",
    "phase_summary",
    "progress_scope_summary",
    "scan_position_summary",
    "sync_report_id",
    "sync_report_ref",
]
