from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest

from retl.destinations.http import (
    REDACTED,
    HttpRequest,
    HttpResponse,
    RedactedHttpRequestEvidence,
    redacted_request_evidence,
    retry_after_seconds,
    retry_after_seconds_from_response,
    transport_failure_evidence,
)
from retl.errors import DeclarationValidationError


def test_http_request_rejects_invalid_method_url_and_body_shape() -> None:
    with pytest.raises(DeclarationValidationError, match="Unsupported HTTP method"):
        HttpRequest(method="TRACE", url="https://api.example.test/import")

    with pytest.raises(DeclarationValidationError, match="absolute HTTP"):
        HttpRequest(method="POST", url="/import")

    with pytest.raises(DeclarationValidationError, match="both `body` and `json_body`"):
        HttpRequest(
            method="POST",
            url="https://api.example.test/import",
            body=b"raw",
            json_body={"records": []},
        )


def test_http_response_rejects_invalid_status_and_body_shape() -> None:
    with pytest.raises(DeclarationValidationError, match="between 100 and 599"):
        HttpResponse(status_code=99)

    with pytest.raises(DeclarationValidationError, match="json_body"):
        HttpResponse(status_code=200, json_body=["not", "a", "mapping"])  # type: ignore[arg-type]


def test_redacted_request_evidence_rejects_unredacted_auth_shapes() -> None:
    with pytest.raises(DeclarationValidationError, match="unredacted `access_token`"):
        RedactedHttpRequestEvidence(
            method="POST",
            url="https://api.example.test/import?access_token=secret",
            headers={},
            query={},
            body_kind="none",
            has_body=False,
        )

    with pytest.raises(DeclarationValidationError, match="user info"):
        RedactedHttpRequestEvidence(
            method="POST",
            url="https://user:password@api.example.test/import",
            headers={},
            query={},
            body_kind="none",
            has_body=False,
        )

    with pytest.raises(DeclarationValidationError, match="unredacted `Authorization`"):
        RedactedHttpRequestEvidence(
            method="POST",
            url="https://api.example.test/import",
            headers={"Authorization": "Bearer secret"},
            query={},
            body_kind="none",
            has_body=False,
        )

    with pytest.raises(DeclarationValidationError, match="auth-bearing value"):
        RedactedHttpRequestEvidence(
            method="POST",
            url="https://api.example.test/import",
            headers={"X-Partner-Auth": REDACTED},
            query={"debug": "Bearer secret"},
            body_kind="none",
            has_body=False,
        )


def test_retry_after_parses_numeric_seconds_and_http_dates() -> None:
    now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc)
    retry_at = now + timedelta(seconds=125)

    assert retry_after_seconds("120", now=now) == 120
    assert retry_after_seconds("-1", now=now) == 0
    assert retry_after_seconds(format_datetime(retry_at, usegmt=True), now=now) == 125
    assert retry_after_seconds("not a retry-after", now=now) is None


def test_retry_after_prefers_response_header_then_body() -> None:
    now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc)

    assert (
        retry_after_seconds_from_response(
            HttpResponse(
                status_code=429,
                headers={"retry-after": "30"},
                json_body={"retry_after_seconds": 90},
            ),
            now=now,
        )
        == 30
    )
    assert (
        retry_after_seconds_from_response(
            HttpResponse(status_code=429, json_body={"retry_after_seconds": 90}),
            now=now,
        )
        == 90
    )


def test_transport_failure_evidence_is_deterministic_and_redacted() -> None:
    request = HttpRequest(
        method="post",
        url="https://api.example.test/import?access_token=url-token&page=1",
        headers={"Authorization": "Bearer header-token", "X-Trace": "trace-1"},
        query={"api_key": "query-key"},
        json_body={"records": [{"email": "customer@example.test"}]},
    )

    evidence = transport_failure_evidence(request, RuntimeError("header-token leaked")).to_mapping()

    assert evidence == {
        "category": "transport",
        "request": {
            "method": "POST",
            "url": "https://api.example.test/import?access_token=%5Bredacted%5D&page=1",
            "headers": {"Authorization": REDACTED, "X-Trace": "trace-1"},
            "query": {"access_token": REDACTED, "page": "1", "api_key": REDACTED},
            "body_kind": "json",
            "has_body": True,
        },
        "error_type": "RuntimeError",
        "message": "HTTP transport request failed before response.",
        "retryable": True,
    }
    serialized = json.dumps(evidence, sort_keys=True)
    assert "header-token" not in serialized
    assert "query-key" not in serialized
    assert "customer@example.test" not in serialized


def test_request_evidence_redacts_sensitive_headers_queries_and_auth_bearing_urls() -> None:
    request = HttpRequest(
        method="POST",
        url=(
            "https://user:password@api.example.test/import"
            "?client_secret=url-client-secret&private_key=url-private-key&cursor=next"
            "#access_token=fragment-token"
        ),
        headers={
            "Authorization": "Bearer auth-header",
            "Cookie": "session=cookie-secret",
            "X-Api-Key": "header-api-key",
            "X-Client-Secret": "header-client-secret",
            "X-Client_Secret": "header-client-secret-underscore",
            "X-Private-Key": "-----BEGIN PRIVATE KEY-----header-private-key",
            "X-Private_Key": "header-private-key-underscore",
            "X-Auth_Token": "header-auth-token-underscore",
            "X-Request-Id": "request-1",
        },
        query={
            "access_token": "query-access-token",
            "x_access_token": "query-access-token-underscore",
            "api_key": "query-api-key",
            "token": "query-token",
            "client_secret": "query-client-secret",
            "client_Secret": "query-client-secret-case",
            "private_key": "query-private-key",
            "cursor": "override-next",
        },
    )

    evidence = redacted_request_evidence(request).to_mapping()
    serialized = json.dumps(evidence, sort_keys=True)

    assert evidence["url"] == (
        "https://redacted@api.example.test/import"
        "?client_secret=%5Bredacted%5D&cursor=next&private_key=%5Bredacted%5D"
    )
    assert evidence["headers"] == {
        "Authorization": REDACTED,
        "Cookie": REDACTED,
        "X-Api-Key": REDACTED,
        "X-Client-Secret": REDACTED,
        "X-Client_Secret": REDACTED,
        "X-Private-Key": REDACTED,
        "X-Private_Key": REDACTED,
        "X-Auth_Token": REDACTED,
        "X-Request-Id": "request-1",
    }
    assert evidence["query"] == {
        "client_secret": REDACTED,
        "private_key": REDACTED,
        "cursor": "override-next",
        "access_token": REDACTED,
        "x_access_token": REDACTED,
        "api_key": REDACTED,
        "token": REDACTED,
        "client_Secret": REDACTED,
    }
    for secret in (
        "password",
        "auth-header",
        "cookie-secret",
        "header-api-key",
        "header-client-secret",
        "header-client-secret-underscore",
        "header-private-key",
        "header-private-key-underscore",
        "header-auth-token-underscore",
        "query-access-token",
        "query-access-token-underscore",
        "query-api-key",
        "query-token",
        "query-client-secret",
        "query-client-secret-case",
        "query-private-key",
        "url-client-secret",
        "url-private-key",
        "fragment-token",
    ):
        assert secret not in serialized


def test_redacted_request_evidence_rejects_unredacted_sensitive_fragment_keys() -> None:
    with pytest.raises(DeclarationValidationError, match="fragment `access_token`"):
        RedactedHttpRequestEvidence(
            method="POST",
            url="https://api.example.test/import#access_token=secret",
            headers={},
            query={},
            body_kind="none",
            has_body=False,
        )
