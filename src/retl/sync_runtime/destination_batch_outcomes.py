from __future__ import annotations

from dataclasses import dataclass

from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.stores.contracts import DestinationBatchStatus

_AUTH_ACCESS_HTTP_STATUSES = frozenset({401, 403, 407})
_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429, 599, *range(500, 600)})


@dataclass(frozen=True)
class DestinationBatchOutcome:
    status: DestinationBatchStatus
    retry_eligible: bool | None
    completed: bool


def classify_destination_batch_outcomes(
    *,
    row_counts: tuple[int, ...],
    submission: DestinationSubmissionEvidence,
) -> tuple[DestinationBatchOutcome, ...]:
    if submission.status == "planned":
        return tuple(classify_destination_batch_status(status="pending") for _ in row_counts)

    counted = _CountedOutcomes(
        confirmed=submission.confirmed_count,
        accepted=submission.accepted_count,
        terminal=submission.terminal_record_failure_count,
        retryable=submission.retryable_failure_count,
        blocking=submission.pre_acceptance_failure_count,
    )
    has_counted_outcomes = counted.has_outcomes
    fallback_status = _fallback_destination_batch_status(submission)
    fallback = (
        DestinationBatchOutcome(
            status="failed",
            retry_eligible=_failure_retry_eligible(submission),
            completed=False,
        )
        if fallback_status == "failed"
        else classify_destination_batch_status(status=fallback_status)
    )
    outcomes: list[DestinationBatchOutcome] = []
    for row_count in row_counts:
        counted_outcome = counted.consume(row_count, submission=submission)
        if counted_outcome is None:
            outcomes.append(
                classify_destination_batch_status(status="pending")
                if has_counted_outcomes
                else fallback
            )
            continue
        counted_status, counted_retry_eligible = counted_outcome
        if counted_status == "failed":
            outcomes.append(
                DestinationBatchOutcome(
                    status="failed",
                    retry_eligible=counted_retry_eligible,
                    completed=False,
                )
            )
            continue
        outcomes.append(classify_destination_batch_status(status=counted_status))
    return tuple(outcomes)


def classify_destination_batch_status(
    *,
    status: DestinationBatchStatus,
) -> DestinationBatchOutcome:
    if status in {"accepted", "succeeded", "skipped"}:
        return DestinationBatchOutcome(status=status, retry_eligible=False, completed=True)
    return DestinationBatchOutcome(status=status, retry_eligible=None, completed=False)


@dataclass
class _CountedOutcomes:
    confirmed: int
    accepted: int
    terminal: int
    retryable: int
    blocking: int

    @property
    def has_outcomes(self) -> bool:
        return any(
            count > 0
            for count in (
                self.confirmed,
                self.accepted,
                self.terminal,
                self.retryable,
                self.blocking,
            )
        )

    def consume(
        self,
        row_count: int,
        *,
        submission: DestinationSubmissionEvidence,
    ) -> tuple[DestinationBatchStatus, bool | None] | None:
        if self.confirmed >= row_count:
            self.confirmed -= row_count
            return ("succeeded", False)
        if self.accepted >= row_count:
            self.accepted -= row_count
            return ("accepted", False)
        if self.terminal >= row_count:
            self.terminal -= row_count
            return ("failed", _failure_retry_eligible(submission))
        if self.retryable >= row_count:
            self.retryable -= row_count
            return ("failed", True)
        if self.blocking >= row_count:
            self.blocking -= row_count
            return ("failed", False)
        return None


def _fallback_destination_batch_status(
    submission: DestinationSubmissionEvidence,
) -> DestinationBatchStatus:
    if submission.status == "confirmed":
        return "succeeded"
    if submission.status == "accepted":
        return "accepted"
    if submission.status in {
        "retryable_failure",
        "pre_acceptance_failure",
        "terminal_record_failure",
    }:
        return "failed"
    return _http_destination_batch_status(submission.http_status) or "pending"


def _failure_retry_eligible(
    submission: DestinationSubmissionEvidence,
) -> bool:
    http_status = submission.http_status
    if submission.status == "retryable_failure":
        return True
    return http_status in _RETRYABLE_HTTP_STATUSES


def _http_destination_batch_status(http_status: int | None) -> DestinationBatchStatus | None:
    if http_status in _AUTH_ACCESS_HTTP_STATUSES or http_status in _RETRYABLE_HTTP_STATUSES:
        return "failed"
    if _is_client_http_status(http_status):
        return "failed"
    return None


def _is_client_http_status(http_status: int | None) -> bool:
    return http_status is not None and 400 <= http_status < 500
