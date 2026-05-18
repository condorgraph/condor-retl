from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias

from retl.destinations.terminal_failures import CommitDecision

if TYPE_CHECKING:
    from retl.runtime.reports import RunIndex, SyncReport

PhaseName: TypeAlias = Literal["collect", "stage", "reconcile", "sync"]
PhaseStatusValue: TypeAlias = Literal["planned", "deferred", "succeeded", "failed"]
RunStatus: TypeAlias = Literal["planned", "partial", "succeeded", "failed"]


@dataclass(frozen=True)
class PhaseEvidence:
    kind: Literal["planned", "deferred"]
    message: str
    dry_run: bool
    irreversible_writes: bool = False
    progress_advanced: bool = False


@dataclass(frozen=True)
class PhaseStatus:
    name: PhaseName
    status: PhaseStatusValue
    evidence: PhaseEvidence


@dataclass(frozen=True)
class SourceGroupResult:
    group_key: str
    source_name: str
    source_mode: str
    sync_names: tuple[str, ...]
    phase: PhaseStatus
    source_window: object | None = None


@dataclass(frozen=True)
class DeclarationStageResult:
    group_key: str
    declaration_name: str
    declaration_kind: Literal["state", "event"]
    source_group_key: str
    sync_names: tuple[str, ...]
    phase: PhaseStatus


@dataclass(frozen=True)
class SyncResult:
    sync_name: str
    declaration_name: str
    declaration_kind: Literal["state", "event"]
    destination_binding_name: str | None
    surface: str
    dry_run: bool
    attempt_id: str | None
    report_reference: str
    collect: PhaseStatus
    stage: PhaseStatus
    reconcile: PhaseStatus
    sync: PhaseStatus
    progress_advanced: bool = False
    irreversible_writes: bool = False
    operation_count: int = 0
    upsert_count: int = 0
    remove_count: int = 0
    event_import_count: int = 0
    duplicate_risk_notes: tuple[str, ...] = ()
    destination_surface_family: str | None = None
    destination_surface_execution_mode: str | None = None
    destination_batch_count: int = 0
    destination_compatibility_status: str | None = None
    target_resolution_status: str | None = None
    target_count: int = 0
    target_mapped_count: int = 0
    target_registry_count: int = 0
    target_managed_created_count: int = 0
    target_planned_create_count: int = 0
    progress_decision: CommitDecision | None = None
    destination_submission_status: str = ""
    destination_confirmed_count: int = 0
    destination_accepted_count: int = 0
    destination_retryable_failure_count: int = 0
    destination_terminal_failure_count: int = 0
    destination_pre_acceptance_failure_count: int = 0
    destination_pre_acceptance_failure_category: str | None = None
    destination_remote_handles: tuple[str, ...] = ()
    delivery_decision_reason: str = ""
    declaration_version_id: str | None = None

    @property
    def phase_evidence(self) -> tuple[PhaseStatus, PhaseStatus, PhaseStatus, PhaseStatus]:
        return (self.collect, self.stage, self.reconcile, self.sync)

    def to_text(self) -> str:
        status = _destination_status_text(self.destination_submission_status or self.sync.status)
        diagnostic_marker = (
            f" diagnostic_category={self.destination_pre_acceptance_failure_category}"
            if self.destination_pre_acceptance_failure_category is not None and status == "failed"
            else ""
        )
        return (
            f"sync={self.sync_name} "
            f"status={status} "
            f"operations={self.operation_count} "
            f"upserts={self.upsert_count} "
            f"removes={self.remove_count} "
            f"destination_batches={self.destination_batch_count} "
            f"confirmed_rows={self.destination_confirmed_count} "
            f"accepted_rows={self.destination_accepted_count} "
            f"retryable_rows={self.destination_retryable_failure_count} "
            f"terminal_rows={self.destination_terminal_failure_count} "
            f"blocking_rows={self.destination_pre_acceptance_failure_count} "
            f"progress_advanced={self.progress_advanced} "
            f"report={self.report_reference}"
            f"{diagnostic_marker}"
        )


@dataclass(frozen=True)
class RunResult:
    runner_name: str
    status: RunStatus
    dry_run: bool
    source_groups: tuple[SourceGroupResult, ...]
    declaration_stages: tuple[DeclarationStageResult, ...]
    syncs: tuple[SyncResult, ...]
    progress_advanced: bool = False
    irreversible_writes: bool = False
    run_index_reference: str | None = None
    report_references: tuple[str, ...] = ()
    run_index: RunIndex | None = None
    sync_reports: tuple[SyncReport, ...] = ()
    run_id: str | None = None

    @property
    def sync_results(self) -> tuple[SyncResult, ...]:
        return self.syncs

    @property
    def reports(self) -> tuple[SyncReport, ...]:
        return self.sync_reports

    def to_text(self) -> str:
        sections: list[str] = []
        if self.run_index is not None:
            sections.append(self.run_index.to_text())
        if self.syncs:
            sections.append("Sync Results:\n" + "\n".join(sync.to_text() for sync in self.syncs))
        sections.extend(report.to_text() for report in self.sync_reports)
        if sections:
            return "\n\n".join(sections)
        return f"Run Result: {self.runner_name}\nStatus: {self.status}\nDry Run: {self.dry_run}"


def _destination_status_text(status: object) -> str:
    return {
        "terminal_record_failure": "failed",
        "pre_acceptance_failure": "failed",
        "retryable_failure": "failed",
    }.get(str(status), str(status))


__all__ = [
    "DeclarationStageResult",
    "PhaseEvidence",
    "PhaseName",
    "PhaseStatus",
    "PhaseStatusValue",
    "RunResult",
    "RunStatus",
    "SourceGroupResult",
    "SyncResult",
]
