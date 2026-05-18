from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias

from retl.destinations.surfaces import DeliveryOutcome, DestinationSurface

SubmissionStatus: TypeAlias = Literal[
    "planned",
    "confirmed",
    "accepted",
    "retryable_failure",
    "terminal_record_failure",
    "pre_acceptance_failure",
]
PreAcceptanceFailureCategory: TypeAlias = Literal[
    "transport",
    "auth",
    "schema",
    "rate_limit",
    "submission",
]


@dataclass(frozen=True)
class RemoteHandle:
    kind: str
    value: str

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("Remote Handle `kind` must be a non-empty string.")
        if not self.value.strip():
            raise ValueError("Remote Handle `value` must be a non-empty string.")


@dataclass(frozen=True)
class DestinationReceipt:
    status: Literal["confirmed", "accepted"]
    count: int
    remote_handle: RemoteHandle | None = None

    def __post_init__(self) -> None:
        if self.count < 0:
            raise ValueError("Destination Receipt `count` must be greater than or equal to 0.")


@dataclass(frozen=True)
class DestinationSubmissionEvidence:
    status: SubmissionStatus
    attempted_count: int
    dry_run: bool = False
    confirmed_count: int = 0
    accepted_count: int = 0
    retryable_failure_count: int = 0
    terminal_record_failure_count: int = 0
    pre_acceptance_failure_count: int = 0
    pre_acceptance_failure_category: PreAcceptanceFailureCategory | None = None
    request_batch_count: int = 0
    receipts: tuple[DestinationReceipt, ...] = ()
    remote_handles: tuple[RemoteHandle, ...] = ()
    summary: str = ""
    http_status: int | None = None
    partner_error_code: str | None = None
    partner_error_subcode: str | None = None
    partner_error_detail: str | None = None
    retry_after_seconds: int | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "attempted_count",
            "confirmed_count",
            "accepted_count",
            "retryable_failure_count",
            "terminal_record_failure_count",
            "pre_acceptance_failure_count",
            "request_batch_count",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"`{field_name}` must be greater than or equal to 0.")
        if self.status == "pre_acceptance_failure" and self.pre_acceptance_failure_category is None:
            raise ValueError("Pre-acceptance failure evidence requires a failure category.")
        if self.http_status is not None and (self.http_status < 100 or self.http_status > 599):
            raise ValueError("`http_status` must be between 100 and 599 when provided.")
        if self.retry_after_seconds is not None and self.retry_after_seconds < 0:
            raise ValueError("`retry_after_seconds` must be non-negative when provided.")

    @property
    def successful_count(self) -> int:
        return self.confirmed_count + self.accepted_count

    @property
    def blocks_accepted_progress(self) -> bool:
        return self.pre_acceptance_failure_count > 0 or self.status == "pre_acceptance_failure"

    @classmethod
    def planned(
        cls,
        *,
        attempted_count: int,
        dry_run: bool,
        request_batch_count: int = 0,
        summary: str = "Destination work planned without submission.",
    ) -> DestinationSubmissionEvidence:
        return cls(
            status="planned",
            attempted_count=attempted_count,
            dry_run=dry_run,
            request_batch_count=request_batch_count,
            summary=summary,
        )


@dataclass(frozen=True)
class DeliveryEvidenceDecision:
    delivery_outcome: DeliveryOutcome
    progress_allowed: bool
    reason: str


def evaluate_delivery_outcome(
    *,
    surface: DestinationSurface,
    evidence: DestinationSubmissionEvidence,
) -> DeliveryEvidenceDecision:
    delivery_outcome = surface.delivery_outcome
    required_successes = evidence.attempted_count - evidence.terminal_record_failure_count
    if delivery_outcome == "succeeded":
        if evidence.confirmed_count >= required_successes and evidence.status == "confirmed":
            return DeliveryEvidenceDecision(
                delivery_outcome=delivery_outcome,
                progress_allowed=True,
                reason="Surface delivery outcome is satisfied by final succeeded evidence.",
            )
        return DeliveryEvidenceDecision(
            delivery_outcome=delivery_outcome,
            progress_allowed=False,
            reason="Surface delivery outcome requires final succeeded evidence.",
        )

    if delivery_outcome != "accepted":
        raise ValueError("Surface delivery outcome must be either 'accepted' or 'succeeded'.")

    if evidence.blocks_accepted_progress:
        return DeliveryEvidenceDecision(
            delivery_outcome=delivery_outcome,
            progress_allowed=False,
            reason="Accepted delivery outcome is blocked by pre-acceptance failure evidence.",
        )
    if (
        evidence.status in ("accepted", "confirmed")
        and evidence.successful_count >= required_successes
    ):
        return DeliveryEvidenceDecision(
            delivery_outcome=delivery_outcome,
            progress_allowed=True,
            reason="Accepted delivery outcome is satisfied by successful submission evidence.",
        )
    return DeliveryEvidenceDecision(
        delivery_outcome=delivery_outcome,
        progress_allowed=False,
        reason="Accepted delivery outcome requires accepted or succeeded evidence.",
    )


__all__ = [
    "DeliveryEvidenceDecision",
    "DeliveryOutcome",
    "DestinationReceipt",
    "DestinationSubmissionEvidence",
    "PreAcceptanceFailureCategory",
    "RemoteHandle",
    "SubmissionStatus",
    "evaluate_delivery_outcome",
]
