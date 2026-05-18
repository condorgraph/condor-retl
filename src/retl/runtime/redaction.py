from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from retl.destinations.receipts import sanitize_partner_error_detail

MAX_DIAGNOSTIC_SAMPLES = 3
MAX_DIAGNOSTIC_TEXT = 160
REDACTED = "[redacted]"

SENSITIVE_FIELD_TOKENS = (
    "api_key",
    "authorization",
    "auth_locator",
    "auth_uri",
    "auth_url",
    "credential",
    "cookie",
    "email",
    "event_identity",
    "identifier",
    "key_json",
    "password",
    "payload",
    "secret",
    "state_identity",
    "token",
)

SECRET_TEXT_KEY_PATTERN = (
    r"authorization|access[_-]?token|api[_-]?key|client[_-]?secret|"
    r"cookie|credential|private[_-]?key|password|secret|set[_-]?cookie|token"
)
AUTH_BEARING_URL_PATTERN = re.compile(
    r"(?i)\b(?P<scheme>[a-z][a-z0-9+.-]*://)[^\s/@:]+:[^\s/@]+@(?P<host>[^\s/?#]+)"
)
SECRET_TEXT_PATTERNS = (
    re.compile(rf"(?i)(?P<key>{SECRET_TEXT_KEY_PATTERN})\s*[:=]\s*bearer\s+[a-z0-9._~+/=-]+"),
    re.compile(
        rf"(?i)[\"']?(?P<key>{SECRET_TEXT_KEY_PATTERN})[\"']?\s*[:=]\s*"
        r"[\"']?[^\s,;}\"]+[\"']?"
    ),
    re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/=-]+"),
)


def bounded_redacted_samples(
    rows: Iterable[Mapping[str, object]],
    *,
    limit: int = MAX_DIAGNOSTIC_SAMPLES,
) -> tuple[Mapping[str, object], ...]:
    samples: list[Mapping[str, object]] = []
    for row in rows:
        if len(samples) >= limit:
            break
        samples.append({str(key): redact_value(str(key), value) for key, value in row.items()})
    return tuple(samples)


def redact_text(value: object) -> str:
    text = str(value)
    text = AUTH_BEARING_URL_PATTERN.sub(_redacted_auth_url_match, text)
    for pattern in SECRET_TEXT_PATTERNS:
        text = pattern.sub(_redacted_secret_match, text)
    if len(text) > MAX_DIAGNOSTIC_TEXT:
        text = f"{text[:MAX_DIAGNOSTIC_TEXT]}..."
    return text


def redact_value(field_name: str, value: object) -> object:
    if value is None or isinstance(value, bool | int | float):
        return value
    if field_name in {"partner_error_detail", "last_error_detail"}:
        return sanitize_partner_error_detail(str(value))
    if is_sensitive_field(field_name):
        return REDACTED
    if isinstance(value, Mapping):
        return {str(key): redact_value(str(key), item) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(redact_value(field_name, item) for item in value)
    if isinstance(value, list):
        return [redact_value(field_name, item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return redact_text(value)


def is_sensitive_field(field_name: str) -> bool:
    normalized = field_name.lower()
    if "fingerprint" in normalized:
        return False
    return any(token in normalized for token in SENSITIVE_FIELD_TOKENS)


def _redacted_secret_match(match: re.Match[str]) -> str:
    key = match.groupdict().get("key")
    if key:
        return f"{key}={REDACTED}"
    return REDACTED


def _redacted_auth_url_match(match: re.Match[str]) -> str:
    return f"{match.group('scheme')}{REDACTED}@{match.group('host')}"


__all__ = [
    "MAX_DIAGNOSTIC_SAMPLES",
    "MAX_DIAGNOSTIC_TEXT",
    "REDACTED",
    "bounded_redacted_samples",
    "is_sensitive_field",
    "redact_text",
    "redact_value",
]
