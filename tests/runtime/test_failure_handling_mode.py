from __future__ import annotations

from typing import Any, cast

import pytest

from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.builtins.mock import mock_surfaces, submit_mock_destination
from retl.destinations.builtins.reference import reference_surfaces, submit_reference_destination
from retl.destinations.terminal_failures import (
    DestinationSyncEvidence,
    decide_page_continuation,
    decide_progress_commit,
    decide_request_batch_continuation,
)


@pytest.mark.parametrize(
    ("mode", "retryable", "terminal", "allowed"),
    (
        ("continue_on_any", 1, 1, True),
        ("stop_on_any", 1, 0, False),
        ("stop_on_any", 0, 1, False),
        ("stop_on_terminal", 1, 0, True),
        ("stop_on_terminal", 0, 1, False),
    ),
)
def test_on_failure_mode_controls_progress_for_failed_destination_batches(
    mode: str,
    retryable: int,
    terminal: int,
    allowed: bool,
) -> None:
    evidence = DestinationSyncEvidence(
        attempted_count=2,
        confirmed_count=2 - retryable - terminal,
        retryable_failure_count=retryable,
        terminal_failure_count=terminal,
    )

    decision = decide_progress_commit(
        delivery_outcome="succeeded",
        on_failure=mode,  # type: ignore[arg-type]
        destination_evidence=evidence,
        dry_run=False,
    )

    assert decision.allowed is allowed
    assert decision.failure_mode_evaluation is not None
    assert decision.failure_mode_evaluation.mode == mode


@pytest.mark.parametrize(
    ("mode", "retryable", "allowed"),
    (
        ("continue_on_any", False, True),
        ("stop_on_any", True, False),
        ("stop_on_terminal", True, True),
        ("stop_on_terminal", False, False),
    ),
)
def test_on_failure_mode_controls_progress_for_pre_acceptance_failures(
    mode: str,
    retryable: bool,
    allowed: bool,
) -> None:
    retryable_count = 1 if retryable else 0
    terminal_count = 0 if retryable else 1
    decision = decide_progress_commit(
        delivery_outcome="succeeded",
        on_failure=mode,  # type: ignore[arg-type]
        destination_evidence=DestinationSyncEvidence(
            attempted_count=1,
            pre_acceptance_failure_count=1,
            pre_acceptance_retryable_failure_count=retryable_count,
            pre_acceptance_terminal_failure_count=terminal_count,
        ),
        dry_run=False,
    )

    assert decision.allowed is allowed


def test_on_failure_mode_does_not_override_dry_run_progress_block() -> None:
    decision = decide_progress_commit(
        delivery_outcome="succeeded",
        on_failure="continue_on_any",
        destination_evidence=DestinationSyncEvidence(
            attempted_count=1,
            retryable_failure_count=1,
        ),
        dry_run=True,
    )

    assert decision.allowed is False
    assert decision.reason == "dry_run cannot advance destination progress."


@pytest.mark.parametrize(
    ("status", "extra"),
    (
        ("terminal_record_failure", {"terminal_record_failure_count": 1}),
        ("retryable_failure", {"retryable_failure_count": 1}),
        (
            "pre_acceptance_failure",
            {
                "pre_acceptance_failure_count": 1,
                "pre_acceptance_failure_category": "transport",
                "http_status": 503,
            },
        ),
    ),
)
def test_continue_on_any_continues_request_batches_after_failures(
    status: str,
    extra: dict[str, object],
) -> None:
    evidence = DestinationSubmissionEvidence(
        status=status,  # type: ignore[arg-type]
        attempted_count=1,
        **extra,  # type: ignore[arg-type]
    )

    decision = decide_request_batch_continuation(
        on_failure="continue_on_any",
        submission=evidence,
    )

    assert decision.allowed is True


def test_stop_on_terminal_continues_after_retryable_but_stops_after_terminal() -> None:
    retryable = decide_request_batch_continuation(
        on_failure="stop_on_terminal",
        submission=DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=1,
            retryable_failure_count=1,
        ),
    )
    terminal = decide_request_batch_continuation(
        on_failure="stop_on_terminal",
        submission=DestinationSubmissionEvidence(
            status="terminal_record_failure",
            attempted_count=1,
            terminal_record_failure_count=1,
        ),
    )

    assert retryable.allowed is True
    assert terminal.allowed is False


def test_stop_on_any_stops_request_batches_after_retryable_failure() -> None:
    decision = decide_request_batch_continuation(
        on_failure="stop_on_any",
        submission=DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=1,
            retryable_failure_count=1,
        ),
    )

    assert decision.allowed is False


def test_page_continuation_requires_policy_and_progress_allowance() -> None:
    evidence = DestinationSubmissionEvidence(
        status="terminal_record_failure",
        attempted_count=1,
        terminal_record_failure_count=1,
    )

    allowed = decide_page_continuation(
        on_failure="continue_on_any",
        submission=evidence,
        progress_allowed=True,
    )
    blocked = decide_page_continuation(
        on_failure="continue_on_any",
        submission=evidence,
        progress_allowed=False,
    )

    assert allowed.allowed is True
    assert blocked.allowed is False


def test_builtin_destinations_reject_multi_batch_direct_submission() -> None:
    selected_request_plans = (cast(Any, object()), cast(Any, object()))

    with pytest.raises(ValueError, match="exactly one selected request batch"):
        submit_mock_destination(
            surface=mock_surfaces()[0],
            delivery_outcome="succeeded",
            attempted_count=2,
            config={},
            selected_request_plans=selected_request_plans,
        )

    with pytest.raises(ValueError, match="exactly one selected request batch"):
        submit_reference_destination(
            surface=reference_surfaces()[0],
            delivery_outcome="succeeded",
            attempted_count=2,
            config={},
            selected_request_plans=selected_request_plans,
        )


def test_pre_acceptance_failure_split_must_match_total() -> None:
    with pytest.raises(ValueError, match="Pre-acceptance retryable and terminal counts"):
        DestinationSyncEvidence(
            attempted_count=1,
            pre_acceptance_failure_count=1,
        )
