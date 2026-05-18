from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from retl.declarations import JSONValue
from retl.destinations.receipts import (
    RemoteHandlePolicy,
    ResponseClassificationPolicy,
    classify_response,
    retry_after_seconds,
    sanitize_partner_error_detail,
    sanitize_partner_message,
    submission_evidence_from_classification,
)


@dataclass(frozen=True)
class Response:
    status_code: int
    json_body: Mapping[str, JSONValue] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    body_text: str | None = None


def test_classifies_confirmed_response_and_remote_handle() -> None:
    classification = classify_response(
        Response(200, {"receipt": {"id": "remote-123"}}),
        policy=ResponseClassificationPolicy(
            error_code_prefix="partner.profile",
            remote_handle=RemoteHandlePolicy(kind="receipt", value_path=("receipt", "id")),
        ),
    )

    assert classification.outcome == "confirmed"
    assert classification.remote_handle is not None
    assert classification.remote_handle.kind == "receipt"
    assert classification.remote_handle.value == "remote-123"


def test_classifies_accepted_response_before_confirmed_when_status_overlaps() -> None:
    classification = classify_response(
        Response(202, {"job": {"id": "job-1"}, "message": "queued"}),
        policy=ResponseClassificationPolicy(
            error_code_prefix="partner.import",
            remote_handle=RemoteHandlePolicy(kind="import_job", value_path=("job", "id")),
        ),
    )

    assert classification.outcome == "accepted"
    assert classification.remote_handle is not None
    assert classification.remote_handle.value == "job-1"


def test_classifies_auth_pre_acceptance_and_rate_limit_retryable_failures() -> None:
    policy = ResponseClassificationPolicy(error_code_prefix="partner.submit")

    auth = classify_response(Response(401), policy=policy)
    proxy_auth = classify_response(Response(407), policy=policy)
    rate = classify_response(Response(429, headers={"Retry-After": "17"}), policy=policy)

    assert auth.outcome == "pre_acceptance_failure"
    assert auth.pre_acceptance_failure_category == "auth"
    assert auth.error_code == "partner.submit.auth_failed"
    assert proxy_auth.outcome == "pre_acceptance_failure"
    assert proxy_auth.pre_acceptance_failure_category == "auth"
    assert rate.outcome == "retryable_failure"
    assert rate.error_code == "partner.submit.retryable"
    assert rate.retry_after_seconds == 17


def test_classifies_retryable_and_terminal_response_statuses() -> None:
    policy = ResponseClassificationPolicy(error_code_prefix="partner.submit")

    timeout = classify_response(Response(408), policy=policy)
    too_early = classify_response(Response(425), policy=policy)
    retryable = classify_response(Response(503, {"error": {"message": "try later"}}), policy=policy)
    bad_request = classify_response(
        Response(400, {"error": {"message": "bad shape", "code": 100, "error_subcode": 2804003}}),
        policy=policy,
    )
    terminal = classify_response(Response(404, {"error": {"message": "missing"}}), policy=policy)
    unprocessable = classify_response(
        Response(422, {"error": {"message": "unprocessable"}}), policy=policy
    )

    assert timeout.outcome == "retryable_failure"
    assert too_early.outcome == "retryable_failure"
    assert retryable.outcome == "retryable_failure"
    assert retryable.error_code == "partner.submit.retryable"
    assert bad_request.outcome == "terminal_record_failure"
    assert bad_request.status_code == 400
    assert bad_request.partner_message == "bad shape"
    assert bad_request.partner_error_code == "100"
    assert bad_request.partner_error_subcode == "2804003"
    assert terminal.outcome == "terminal_record_failure"
    assert terminal.error_code == "partner.submit.terminal"
    assert unprocessable.outcome == "terminal_record_failure"


def test_sanitizes_partner_messages_before_evidence() -> None:
    raw = (
        ' failed\nwith access_token=abc123, {"client_secret":"json-secret"}, '
        "and Authorization: Bearer secret-value "
    )

    sanitized = sanitize_partner_message(raw)

    assert sanitized is not None
    assert "access_token=[redacted]" in sanitized
    assert "client_secret=[redacted]" in sanitized
    assert "Authorization=[redacted]" in sanitized
    assert "abc123" not in sanitized
    assert "json-secret" not in sanitized
    assert "secret-value" not in sanitized


def test_sanitizes_and_caps_partner_error_detail() -> None:
    raw = "error_data.blame_field_specs[0].fields[1]=custom_data.value access_token=abc123 " + (
        "x" * 5000
    )

    sanitized = sanitize_partner_error_detail(raw)

    assert sanitized is not None
    assert len(sanitized) == 4096
    assert "error_data.blame_field_specs[0].fields[1]" in sanitized
    assert "access_token=[redacted]" in sanitized
    assert "abc123" not in sanitized


def test_raw_body_fallback_sanitizes_json_style_secret_values() -> None:
    classification = classify_response(
        Response(503, body_text='{"access_token":"abc123","message":"temporary"}'),
        policy=ResponseClassificationPolicy(error_code_prefix="partner.submit"),
    )

    assert classification.partner_message is not None
    assert "access_token=[redacted]" in classification.partner_message
    assert "abc123" not in classification.partner_message


def test_submission_evidence_counts_match_classification() -> None:
    policy = ResponseClassificationPolicy(
        error_code_prefix="partner.import",
        remote_handle=RemoteHandlePolicy(kind="import_job", value_path=("job", "id")),
    )
    classification = classify_response(Response(202, {"job": {"id": "job-1"}}), policy=policy)

    evidence = submission_evidence_from_classification(classification, attempted_count=3)

    assert evidence.status == "accepted"
    assert evidence.accepted_count == 3
    assert evidence.http_status == 202
    assert evidence.remote_handles[0].value == "job-1"
    assert not evidence.blocks_accepted_progress


def test_submission_evidence_carries_sanitized_response_diagnostics() -> None:
    classification = classify_response(
        Response(
            400,
            {
                "error": {
                    "message": "Invalid parameter token=secret",
                    "code": 100,
                    "error_subcode": 2804003,
                }
            },
        ),
        policy=ResponseClassificationPolicy(error_code_prefix="partner.submit"),
    )

    evidence = submission_evidence_from_classification(classification, attempted_count=3)

    assert evidence.status == "terminal_record_failure"
    assert evidence.http_status == 400
    assert evidence.partner_error_code == "100"
    assert evidence.partner_error_subcode == "2804003"
    assert evidence.terminal_record_failure_count == 3
    assert evidence.summary == "Invalid parameter token=[redacted]"


def test_retry_after_can_be_read_from_body_when_header_absent() -> None:
    assert retry_after_seconds(Response(500, {"retry_after_seconds": 9})) == 9


def test_malformed_retry_after_header_is_ignored() -> None:
    response = Response(500, {"retry_after_seconds": 9}, {"Retry-After": "not a date"})

    assert retry_after_seconds(response) == 9
