from __future__ import annotations

import pytest

from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.sync_runtime.destination_batch_outcomes import (
    classify_destination_batch_outcomes,
    classify_destination_batch_status,
)


@pytest.mark.parametrize("http_status", [400, 404, 422])
def test_client_http_failures_classify_as_failed_destination_batches(
    http_status: int,
) -> None:
    outcomes = classify_destination_batch_outcomes(
        row_counts=(2,),
        submission=DestinationSubmissionEvidence(
            status="terminal_record_failure",
            attempted_count=2,
            terminal_record_failure_count=2,
            http_status=http_status,
        ),
    )

    assert [
        (outcome.status, outcome.retry_eligible, outcome.completed) for outcome in outcomes
    ] == [("failed", False, False)]


@pytest.mark.parametrize("http_status", [401, 403, 407])
def test_auth_access_http_failures_classify_as_non_retryable_failed_destination_batches(
    http_status: int,
) -> None:
    outcomes = classify_destination_batch_outcomes(
        row_counts=(2,),
        submission=DestinationSubmissionEvidence(
            status="terminal_record_failure",
            attempted_count=2,
            terminal_record_failure_count=2,
            http_status=http_status,
            summary="access blocked",
        ),
    )

    assert [
        (outcome.status, outcome.retry_eligible, outcome.completed) for outcome in outcomes
    ] == [("failed", False, False)]


def test_pre_acceptance_failure_classifies_as_failed_destination_batch() -> None:
    outcomes = classify_destination_batch_outcomes(
        row_counts=(2,),
        submission=DestinationSubmissionEvidence(
            status="pre_acceptance_failure",
            attempted_count=2,
            pre_acceptance_failure_count=2,
            pre_acceptance_failure_category="submission",
        ),
    )

    assert [outcome.status for outcome in outcomes] == ["failed"]
    assert outcomes[0].retry_eligible is False
    assert outcomes[0].completed is False


@pytest.mark.parametrize("http_status", [408, 425, 429, 500, 503, 599])
def test_retryable_http_failures_classify_as_retryable_failed_destination_batches(
    http_status: int,
) -> None:
    outcomes = classify_destination_batch_outcomes(
        row_counts=(2,),
        submission=DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=2,
            retryable_failure_count=2,
            http_status=http_status,
        ),
    )

    assert [
        (outcome.status, outcome.retry_eligible, outcome.completed) for outcome in outcomes
    ] == [("failed", True, False)]


def test_mixed_counted_outcomes_allocate_by_batch_and_leave_unattempted_pending() -> None:
    outcomes = classify_destination_batch_outcomes(
        row_counts=(1, 1, 1, 1),
        submission=DestinationSubmissionEvidence(
            status="confirmed",
            attempted_count=3,
            confirmed_count=1,
            terminal_record_failure_count=1,
            retryable_failure_count=1,
            request_batch_count=4,
        ),
    )

    assert [outcome.status for outcome in outcomes] == [
        "succeeded",
        "failed",
        "failed",
        "pending",
    ]
    assert [outcome.retry_eligible for outcome in outcomes] == [False, False, True, None]


def test_planned_without_attempted_rows_leaves_batches_pending() -> None:
    outcomes = classify_destination_batch_outcomes(
        row_counts=(1, 1),
        submission=DestinationSubmissionEvidence.planned(attempted_count=0, dry_run=False),
    )

    assert [outcome.status for outcome in outcomes] == ["pending", "pending"]


def test_skipped_is_terminal_when_classifying_durable_status_directly() -> None:
    outcome = classify_destination_batch_status(status="skipped")

    assert outcome.status == "skipped"
    assert outcome.retry_eligible is False
    assert outcome.completed is True


def test_submission_outcome_classification_does_not_produce_skipped_status() -> None:
    outcomes = classify_destination_batch_outcomes(
        row_counts=(1, 1, 1),
        submission=DestinationSubmissionEvidence(
            status="confirmed",
            attempted_count=3,
            confirmed_count=1,
            accepted_count=1,
            terminal_record_failure_count=1,
            request_batch_count=3,
        ),
    )

    assert {outcome.status for outcome in outcomes} == {"succeeded", "accepted", "failed"}
    assert "skipped" not in {outcome.status for outcome in outcomes}
