from __future__ import annotations

from retl.runtime.redaction import (
    REDACTED,
    bounded_redacted_samples,
    is_sensitive_field,
    redact_text,
    redact_value,
)


def test_redact_text_removes_secret_shaped_fragments() -> None:
    text = (
        "request failed authorization=Bearer abc123 token=secret "
        '"client_secret":"json-secret" Bearer standalone-token'
    )

    redacted = redact_text(text)

    assert "authorization=[redacted]" in redacted
    assert "token=[redacted]" in redacted
    assert "client_secret=[redacted]" in redacted
    assert "abc123" not in redacted
    assert "token=secret" not in redacted
    assert "json-secret" not in redacted
    assert "standalone-token" not in redacted


def test_redact_text_removes_cookie_and_auth_url_fragments() -> None:
    text = (
        "request https://user:raw-url-secret@example.test/private "
        "mirror https://example.test/public Cookie: session=raw-cookie"
    )

    redacted = redact_text(text)

    assert "https://[redacted]@example.test/private" in redacted
    assert "https://example.test/public" in redacted
    assert "Cookie=[redacted]" in redacted
    assert "raw-url-secret" not in redacted
    assert "raw-cookie" not in redacted


def test_redact_value_redacts_sensitive_field_shapes_but_keeps_fingerprints() -> None:
    assert redact_value("payload_json", {"plan": "enterprise"}) == REDACTED
    assert redact_value("email", "person@example.test") == REDACTED
    assert redact_value("identifier_values", ("user-1",)) == REDACTED
    assert redact_value("payload_fingerprint", "payload:abc") == "payload:abc"
    assert redact_value("target_request_fingerprint", "request:redacted:abc") == (
        "request:redacted:abc"
    )


def test_partner_error_detail_uses_receipt_sanitization() -> None:
    detail = '{"error_data":{"field":"custom_data"},"access_token":"abc123"}'

    redacted = redact_value("partner_error_detail", detail)

    assert "custom_data" in str(redacted)
    assert "access_token=[redacted]" in str(redacted)
    assert "abc123" not in str(redacted)


def test_redact_value_recursively_redacts_neutral_mappings() -> None:
    redacted = redact_value(
        "context",
        {
            "event": "sync_failed",
            "authorization": "Bearer abc123",
            "nested": {
                "client_secret": "json-secret",
                "payload_fingerprint": "payload:abc",
            },
        },
    )

    assert redacted == {
        "event": "sync_failed",
        "authorization": REDACTED,
        "nested": {
            "client_secret": REDACTED,
            "payload_fingerprint": "payload:abc",
        },
    }


def test_redact_value_recursively_redacts_neutral_lists_and_tuples() -> None:
    redacted = redact_value(
        "context",
        [
            {"token": "secret-token"},
            ("safe", {"payload_json": '{"plan":"pro"}'}),
            "authorization=Bearer abc123",
        ],
    )

    assert redacted == [
        {"token": REDACTED},
        ("safe", {"payload_json": REDACTED}),
        "authorization=[redacted]",
    ]


def test_bounded_redacted_samples_limits_rows_and_redacts_values() -> None:
    samples = bounded_redacted_samples(
        (
            {"id": "1", "payload_json": '{"plan":"pro"}'},
            {"id": "2", "authorization": "Bearer token"},
            {"id": "3", "state_identity": "state-3"},
        ),
        limit=2,
    )

    assert samples == (
        {"id": "1", "payload_json": REDACTED},
        {"id": "2", "authorization": REDACTED},
    )


def test_sensitive_field_detection_is_conservative_for_log_context() -> None:
    assert is_sensitive_field("api_key")
    assert is_sensitive_field("event_identity")
    assert is_sensitive_field("request_payload")
    assert not is_sensitive_field("payload_fingerprint")
