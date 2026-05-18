from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import pytest

from retl.declarations import JSONValue
from retl.destinations.acknowledgements import RemoteHandle
from retl.destinations.async_http import (
    AsyncPollClassificationPolicy,
    AsyncPollRequestSpec,
    classify_async_submit_response,
    classify_poll_response,
    finalize_async_progress,
    render_poll_request,
)
from retl.destinations.receipts import RemoteHandlePolicy, ResponseClassificationPolicy


@dataclass(frozen=True)
class Response:
    status_code: int
    json_body: Mapping[str, JSONValue] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    body_text: str | None = None


def test_classifies_async_submit_and_resume_payload() -> None:
    result = classify_async_submit_response(
        Response(202, {"job": {"id": "job-123"}}, {"Retry-After": "12"}),
        policy=ResponseClassificationPolicy(
            error_code_prefix="partner.import",
            remote_handle=RemoteHandlePolicy(kind="import_job", value_path=("job", "id")),
        ),
    )

    assert result.classification.outcome == "accepted"
    assert result.remote_handle == RemoteHandle(kind="import_job", value="job-123")
    assert result.resume_payload == {
        "remote_handle_kind": "import_job",
        "remote_handle_value": "job-123",
        "retry_after_seconds": 12,
    }


def test_render_poll_request_uses_remote_handle_resume_payload() -> None:
    request = render_poll_request(
        base_url="https://api.example.test/",
        remote_handle=RemoteHandle(kind="import_job", value="job-123"),
        resume_payload={"account_id": "acct-9"},
        spec=AsyncPollRequestSpec(
            path="/accounts/{account_id}/jobs/{remote_handle_value}",
            query={"include": "summary"},
            headers={"X-Retl-Surface": "events"},
        ),
    )

    assert request.method == "GET"
    assert request.path == "/accounts/acct-9/jobs/job-123"
    assert request.url == "https://api.example.test/accounts/acct-9/jobs/job-123?include=summary"
    assert request.headers == {"X-Retl-Surface": "events"}


def test_async_poll_request_spec_rejects_auth_material() -> None:
    with pytest.raises(ValueError, match="headers.*auth material"):
        AsyncPollRequestSpec(headers={"Authorization": "Bearer token"})

    with pytest.raises(ValueError, match="headers.*auth material"):
        AsyncPollRequestSpec(headers={"X-Client_Secret": "secret"})

    with pytest.raises(ValueError, match="query.*auth material"):
        AsyncPollRequestSpec(query={"access_token": "token"})

    with pytest.raises(ValueError, match="query.*auth material"):
        AsyncPollRequestSpec(query={"x_access_token": "token"})


def test_classifies_poll_pending_confirmed_retryable_and_terminal() -> None:
    handle = RemoteHandle(kind="import_job", value="job-123")
    policy = AsyncPollClassificationPolicy(default_retry_after_seconds=30)

    pending = classify_poll_response(
        Response(200, {"status": "running"}), remote_handle=handle, policy=policy
    )
    confirmed = classify_poll_response(
        Response(200, {"status": "complete"}), remote_handle=handle, policy=policy
    )
    retryable = classify_poll_response(
        Response(503, {"error": {"message": "busy"}}), remote_handle=handle, policy=policy
    )
    terminal = classify_poll_response(
        Response(200, {"status": "invalid", "error": {"message": "bad id"}}),
        remote_handle=handle,
        policy=policy,
    )

    assert pending.outcome == "pending"
    assert pending.retry_after_seconds == 30
    assert confirmed.outcome == "confirmed"
    assert retryable.outcome == "retryable_failure"
    assert retryable.partner_message == "busy"
    assert terminal.outcome == "terminal_failure"
    assert terminal.partner_message == "bad id"


def test_finalize_async_progress_respects_delivery_outcome() -> None:
    submit = classify_async_submit_response(
        Response(202, {"job": {"id": "job-123"}}),
        policy=ResponseClassificationPolicy(
            error_code_prefix="partner.import",
            remote_handle=RemoteHandlePolicy(kind="import_job", value_path=("job", "id")),
        ),
    )
    poll = classify_poll_response(
        Response(200, {"status": "complete"}),
        remote_handle=RemoteHandle(kind="import_job", value="job-123"),
    )

    accepted = finalize_async_progress(
        delivery_outcome="accepted",
        submit=submit.classification,
    )
    succeeded_before_poll = finalize_async_progress(
        delivery_outcome="succeeded",
        submit=submit.classification,
    )
    succeeded_after_poll = finalize_async_progress(
        delivery_outcome="succeeded",
        submit=submit.classification,
        poll=poll,
    )

    assert accepted.progress_allowed
    assert accepted.progress_outcome == "accepted"
    assert not succeeded_before_poll.progress_allowed
    assert succeeded_after_poll.progress_allowed
    assert succeeded_after_poll.progress_outcome == "succeeded"


def test_pre_acceptance_submit_does_not_allow_accepted_finalization() -> None:
    submit = classify_async_submit_response(
        Response(401),
        policy=ResponseClassificationPolicy(error_code_prefix="partner.import"),
    )

    decision = finalize_async_progress(
        delivery_outcome="accepted",
        submit=submit.classification,
    )

    assert submit.classification.outcome == "pre_acceptance_failure"
    assert not decision.progress_allowed
