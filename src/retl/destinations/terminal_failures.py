"""Destination failure mode and commit-gate evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from retl.declarations import FailureHandlingMode
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.surfaces import DeliveryOutcome

_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429, 599, *range(500, 600)})


@dataclass(frozen=True)
class FailureModeEvaluation:
    mode: FailureHandlingMode
    attempted_count: int
    terminal_failure_count: int
    retryable_failure_count: int
    pre_acceptance_failure_count: int
    pre_acceptance_retryable_failure_count: int
    pre_acceptance_terminal_failure_count: int
    allowed: bool
    reason: str


@dataclass(frozen=True)
class DestinationSyncEvidence:
    """Bounded destination evidence needed for commit decisions."""

    attempted_count: int
    confirmed_count: int = 0
    accepted_count: int = 0
    retryable_failure_count: int = 0
    terminal_failure_count: int = 0
    pre_acceptance_failure_count: int = 0
    pre_acceptance_retryable_failure_count: int = 0
    pre_acceptance_terminal_failure_count: int = 0

    def __post_init__(self) -> None:
        counts = {
            "attempted_count": self.attempted_count,
            "confirmed_count": self.confirmed_count,
            "accepted_count": self.accepted_count,
            "retryable_failure_count": self.retryable_failure_count,
            "terminal_failure_count": self.terminal_failure_count,
            "pre_acceptance_failure_count": self.pre_acceptance_failure_count,
            "pre_acceptance_retryable_failure_count": (self.pre_acceptance_retryable_failure_count),
            "pre_acceptance_terminal_failure_count": self.pre_acceptance_terminal_failure_count,
        }
        negative = tuple(name for name, value in counts.items() if value < 0)
        if negative:
            names = ", ".join(negative)
            raise ValueError(f"Destination sync evidence counts cannot be negative: {names}.")
        if self.confirmed_count + self.accepted_count > self.attempted_count:
            raise ValueError("Successful destination count cannot exceed attempted records.")
        failure_count = (
            self.retryable_failure_count
            + self.terminal_failure_count
            + self.pre_acceptance_failure_count
        )
        if failure_count > self.attempted_count:
            raise ValueError("Destination failure count cannot exceed attempted records.")
        pre_acceptance_split = (
            self.pre_acceptance_retryable_failure_count + self.pre_acceptance_terminal_failure_count
        )
        if pre_acceptance_split != self.pre_acceptance_failure_count:
            raise ValueError(
                "Pre-acceptance retryable and terminal counts must equal "
                "pre_acceptance_failure_count."
            )


@dataclass(frozen=True)
class CommitDecision:
    subject: Literal["progress"]
    allowed: bool
    reason: str
    failure_mode_evaluation: FailureModeEvaluation | None = None


@dataclass(frozen=True)
class ContinuationDecision:
    subject: Literal["request_batch", "page"]
    allowed: bool
    reason: str
    failure_mode_evaluation: FailureModeEvaluation | None = None


def evaluate_failure_mode(
    *,
    mode: FailureHandlingMode,
    attempted_count: int,
    terminal_failure_count: int,
    retryable_failure_count: int = 0,
    pre_acceptance_failure_count: int = 0,
    pre_acceptance_retryable_failure_count: int = 0,
    pre_acceptance_terminal_failure_count: int = 0,
) -> FailureModeEvaluation:
    if (
        attempted_count < 0
        or terminal_failure_count < 0
        or retryable_failure_count < 0
        or pre_acceptance_failure_count < 0
        or pre_acceptance_retryable_failure_count < 0
        or pre_acceptance_terminal_failure_count < 0
    ):
        raise ValueError("Failure mode counts cannot be negative.")
    failure_count = terminal_failure_count + retryable_failure_count + pre_acceptance_failure_count
    if failure_count > attempted_count:
        raise ValueError("Failure counts cannot exceed attempted records.")
    if mode not in {"stop_on_terminal", "stop_on_any", "continue_on_any"}:
        raise ValueError("Unsupported failure handling mode.")

    if mode == "stop_on_any" and failure_count:
        return FailureModeEvaluation(
            mode=mode,
            attempted_count=attempted_count,
            terminal_failure_count=terminal_failure_count,
            retryable_failure_count=retryable_failure_count,
            pre_acceptance_failure_count=pre_acceptance_failure_count,
            pre_acceptance_retryable_failure_count=pre_acceptance_retryable_failure_count,
            pre_acceptance_terminal_failure_count=pre_acceptance_terminal_failure_count,
            allowed=False,
            reason="on_failure=stop_on_any blocks progress when destination failures are present.",
        )
    if mode == "stop_on_terminal" and (
        terminal_failure_count or pre_acceptance_terminal_failure_count
    ):
        return FailureModeEvaluation(
            mode=mode,
            attempted_count=attempted_count,
            terminal_failure_count=terminal_failure_count,
            retryable_failure_count=retryable_failure_count,
            pre_acceptance_failure_count=pre_acceptance_failure_count,
            pre_acceptance_retryable_failure_count=pre_acceptance_retryable_failure_count,
            pre_acceptance_terminal_failure_count=pre_acceptance_terminal_failure_count,
            allowed=False,
            reason=(
                "on_failure=stop_on_terminal blocks progress when non-retryable "
                "destination failures are present."
            ),
        )
    return FailureModeEvaluation(
        mode=mode,
        attempted_count=attempted_count,
        terminal_failure_count=terminal_failure_count,
        retryable_failure_count=retryable_failure_count,
        pre_acceptance_failure_count=pre_acceptance_failure_count,
        pre_acceptance_retryable_failure_count=pre_acceptance_retryable_failure_count,
        pre_acceptance_terminal_failure_count=pre_acceptance_terminal_failure_count,
        allowed=True,
        reason=f"on_failure={mode} allows progress for the evidenced destination failures.",
    )


def decide_request_batch_continuation(
    *,
    on_failure: FailureHandlingMode,
    submission: DestinationSubmissionEvidence,
) -> ContinuationDecision:
    failure_mode = _failure_mode_from_submission(mode=on_failure, submission=submission)
    if failure_mode.allowed:
        return ContinuationDecision(
            subject="request_batch",
            allowed=True,
            reason=(
                f"on_failure={on_failure} allows continuing to the next destination request batch."
            ),
            failure_mode_evaluation=failure_mode,
        )
    return ContinuationDecision(
        subject="request_batch",
        allowed=False,
        reason=failure_mode.reason,
        failure_mode_evaluation=failure_mode,
    )


def decide_page_continuation(
    *,
    on_failure: FailureHandlingMode,
    submission: DestinationSubmissionEvidence,
    progress_allowed: bool,
) -> ContinuationDecision:
    failure_mode = _failure_mode_from_submission(mode=on_failure, submission=submission)
    if not progress_allowed:
        return ContinuationDecision(
            subject="page",
            allowed=False,
            reason="Destination page continuation waits for durable progress allowance.",
            failure_mode_evaluation=failure_mode,
        )
    if failure_mode.allowed:
        return ContinuationDecision(
            subject="page",
            allowed=True,
            reason=f"on_failure={on_failure} allows continuing to the next staged page.",
            failure_mode_evaluation=failure_mode,
        )
    return ContinuationDecision(
        subject="page",
        allowed=False,
        reason=failure_mode.reason,
        failure_mode_evaluation=failure_mode,
    )


def _failure_mode_from_submission(
    *,
    mode: FailureHandlingMode,
    submission: DestinationSubmissionEvidence,
) -> FailureModeEvaluation:
    pre_acceptance_retryable = 0
    pre_acceptance_terminal = submission.pre_acceptance_failure_count
    if submission.pre_acceptance_failure_count and pre_acceptance_failure_retryable(
        submission.http_status
    ):
        pre_acceptance_retryable = submission.pre_acceptance_failure_count
        pre_acceptance_terminal = 0
    return evaluate_failure_mode(
        mode=mode,
        attempted_count=submission.attempted_count,
        terminal_failure_count=submission.terminal_record_failure_count,
        retryable_failure_count=submission.retryable_failure_count,
        pre_acceptance_failure_count=submission.pre_acceptance_failure_count,
        pre_acceptance_retryable_failure_count=pre_acceptance_retryable,
        pre_acceptance_terminal_failure_count=pre_acceptance_terminal,
    )


def decide_progress_commit(
    *,
    delivery_outcome: DeliveryOutcome,
    on_failure: FailureHandlingMode,
    destination_evidence: DestinationSyncEvidence | None,
    dry_run: bool,
) -> CommitDecision:
    if dry_run:
        return CommitDecision(
            subject="progress",
            allowed=False,
            reason="dry_run cannot advance destination progress.",
        )
    if destination_evidence is None:
        return CommitDecision(
            subject="progress",
            allowed=False,
            reason="destination submission evidence is deferred.",
        )

    failure_mode = evaluate_failure_mode(
        mode=on_failure,
        attempted_count=destination_evidence.attempted_count,
        terminal_failure_count=destination_evidence.terminal_failure_count,
        retryable_failure_count=destination_evidence.retryable_failure_count,
        pre_acceptance_failure_count=destination_evidence.pre_acceptance_failure_count,
        pre_acceptance_retryable_failure_count=(
            destination_evidence.pre_acceptance_retryable_failure_count
        ),
        pre_acceptance_terminal_failure_count=(
            destination_evidence.pre_acceptance_terminal_failure_count
        ),
    )
    if not failure_mode.allowed:
        return CommitDecision(
            subject="progress",
            allowed=False,
            reason=failure_mode.reason,
            failure_mode_evaluation=failure_mode,
        )

    required_successes = destination_evidence.attempted_count
    if on_failure in {"stop_on_terminal", "continue_on_any"}:
        required_successes -= destination_evidence.terminal_failure_count
        required_successes -= destination_evidence.retryable_failure_count
        required_successes -= destination_evidence.pre_acceptance_failure_count
    if delivery_outcome == "succeeded":
        successful = destination_evidence.confirmed_count
        label = "succeeded"
    elif delivery_outcome == "accepted":
        successful = destination_evidence.confirmed_count + destination_evidence.accepted_count
        label = "accepted or succeeded"
    else:
        raise ValueError("Unsupported surface delivery outcome.")

    if successful < required_successes:
        return CommitDecision(
            subject="progress",
            allowed=False,
            reason=(
                f"surface delivery outcome requires {required_successes} {label} "
                f"record(s), but only {successful} were evidenced."
            ),
            failure_mode_evaluation=failure_mode,
        )

    tolerated_failures = (
        destination_evidence.terminal_failure_count
        + destination_evidence.retryable_failure_count
        + destination_evidence.pre_acceptance_failure_count
    )
    tolerated = (
        f" with {tolerated_failures} failure(s) allowed by {on_failure}"
        if tolerated_failures
        else ""
    )
    return CommitDecision(
        subject="progress",
        allowed=True,
        reason=f"destination evidence allows progress{tolerated}.",
        failure_mode_evaluation=failure_mode,
    )


def pre_acceptance_failure_retryable(http_status: int | None) -> bool:
    return http_status in _RETRYABLE_HTTP_STATUSES


__all__ = [
    "CommitDecision",
    "ContinuationDecision",
    "DestinationSyncEvidence",
    "FailureModeEvaluation",
    "decide_page_continuation",
    "decide_progress_commit",
    "decide_request_batch_continuation",
    "evaluate_failure_mode",
    "pre_acceptance_failure_retryable",
]
