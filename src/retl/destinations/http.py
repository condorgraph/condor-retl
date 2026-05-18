from __future__ import annotations

import email.utils
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import ceil
from types import MappingProxyType
from typing import Protocol
from urllib import parse

from retl.errors import DeclarationValidationError

REDACTED = "[redacted]"

_ALLOWED_HTTP_METHODS = frozenset({"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"})
_SENSITIVE_EXACT_NAMES = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "client_secret",
        "cookie",
        "id_token",
        "jwt",
        "private_key",
        "proxy_authorization",
        "refresh_token",
        "secret",
        "set_cookie",
        "sig",
        "signature",
        "token",
        "x_api_key",
    }
)
_SENSITIVE_NAME_FRAGMENTS = (
    "access-token",
    "api-key",
    "apikey",
    "auth-token",
    "authorization",
    "bearer",
    "client-secret",
    "private-key",
    "refresh-token",
)


class HttpTransport(Protocol):
    def send(self, request: "HttpRequest") -> "HttpResponse": ...


@dataclass(frozen=True)
class HttpRequest:
    method: str
    url: str
    headers: Mapping[str, str] = field(default_factory=dict)
    query: Mapping[str, str] = field(default_factory=dict)
    body: bytes | None = None
    json_body: object | None = None
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        method = self.method.strip().upper()
        if method not in _ALLOWED_HTTP_METHODS:
            raise DeclarationValidationError(f"Unsupported HTTP method `{self.method}`.")
        parsed_url = parse.urlsplit(self.url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise DeclarationValidationError("HTTP request `url` must be an absolute HTTP(S) URL.")
        if self.body is not None and self.json_body is not None:
            raise DeclarationValidationError(
                "HTTP request must not set both `body` and `json_body`."
            )
        if self.timeout_seconds <= 0:
            raise DeclarationValidationError("HTTP request `timeout_seconds` must be positive.")
        object.__setattr__(self, "method", method)
        object.__setattr__(
            self,
            "headers",
            MappingProxyType(_string_mapping(self.headers, "headers")),
        )
        object.__setattr__(self, "query", MappingProxyType(_string_mapping(self.query, "query")))


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    headers: Mapping[str, str] = field(default_factory=dict)
    json_body: Mapping[str, object] = field(default_factory=dict)
    body_text: str | None = None

    def __post_init__(self) -> None:
        if self.status_code < 100 or self.status_code > 599:
            raise DeclarationValidationError(
                "HTTP response `status_code` must be between 100 and 599."
            )
        object.__setattr__(
            self,
            "headers",
            MappingProxyType(_string_mapping(self.headers, "headers")),
        )
        if not isinstance(self.json_body, Mapping):
            raise DeclarationValidationError("HTTP response `json_body` must be a mapping.")
        object.__setattr__(self, "json_body", MappingProxyType(dict(self.json_body)))


@dataclass(frozen=True)
class RedactedHttpRequestEvidence:
    method: str
    url: str
    headers: Mapping[str, str]
    query: Mapping[str, str]
    body_kind: str
    has_body: bool

    def __post_init__(self) -> None:
        method = self.method.strip().upper()
        if method not in _ALLOWED_HTTP_METHODS:
            raise DeclarationValidationError(f"Unsupported HTTP method `{self.method}`.")
        _ensure_redacted_url(self.url)
        _ensure_redacted_mapping(self.headers, field_name="headers")
        _ensure_redacted_mapping(self.query, field_name="query")
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))
        object.__setattr__(self, "query", MappingProxyType(dict(self.query)))

    def to_mapping(self) -> Mapping[str, object]:
        return {
            "method": self.method,
            "url": self.url,
            "headers": dict(self.headers),
            "query": dict(self.query),
            "body_kind": self.body_kind,
            "has_body": self.has_body,
        }


@dataclass(frozen=True)
class HttpTransportFailureEvidence:
    category: str
    request: RedactedHttpRequestEvidence
    error_type: str
    message: str = "HTTP transport request failed before response."
    retryable: bool = True

    def to_mapping(self) -> Mapping[str, object]:
        return {
            "category": self.category,
            "request": self.request.to_mapping(),
            "error_type": self.error_type,
            "message": self.message,
            "retryable": self.retryable,
        }


def redacted_request_evidence(request: HttpRequest) -> RedactedHttpRequestEvidence:
    parsed_url = parse.urlsplit(request.url)
    url_query = dict(parse.parse_qsl(parsed_url.query, keep_blank_values=True))
    redacted_query = {
        key: REDACTED if _is_sensitive_name(key) else value
        for key, value in {**url_query, **dict(request.query)}.items()
    }
    if request.json_body is not None:
        body_kind = "json"
    elif request.body is not None:
        body_kind = "bytes"
    else:
        body_kind = "none"
    return RedactedHttpRequestEvidence(
        method=request.method,
        url=_redacted_url(request.url),
        headers={
            key: REDACTED if _is_sensitive_name(key) or _looks_auth_bearing(value) else value
            for key, value in request.headers.items()
        },
        query=redacted_query,
        body_kind=body_kind,
        has_body=request.body is not None or request.json_body is not None,
    )


def transport_failure_evidence(
    request: HttpRequest,
    error: BaseException,
    *,
    retryable: bool = True,
) -> HttpTransportFailureEvidence:
    return HttpTransportFailureEvidence(
        category="transport",
        request=redacted_request_evidence(request),
        error_type=type(error).__name__,
        retryable=retryable,
    )


def retry_after_seconds(value: str | None, *, now: datetime | None = None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return max(0, int(stripped))
    except ValueError:
        pass
    try:
        retry_at = email.utils.parsedate_to_datetime(stripped)
    except (TypeError, ValueError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return max(0, ceil((retry_at - reference).total_seconds()))


def retry_after_seconds_from_response(
    response: HttpResponse,
    *,
    now: datetime | None = None,
) -> int | None:
    for key, value in response.headers.items():
        if key.lower() == "retry-after":
            return retry_after_seconds(value, now=now)
    body_value = response.json_body.get("retry_after_seconds")
    return retry_after_seconds(str(body_value), now=now) if body_value is not None else None


def _redacted_url(url: str) -> str:
    parsed_url = parse.urlsplit(url)
    hostname = parsed_url.hostname or ""
    netloc = hostname
    if parsed_url.port is not None:
        netloc = f"{netloc}:{parsed_url.port}"
    if parsed_url.username is not None or parsed_url.password is not None:
        netloc = f"redacted@{netloc}"
    redacted_query = parse.urlencode(
        sorted(
            (
                key,
                REDACTED if _is_sensitive_name(key) else value,
            )
            for key, value in parse.parse_qsl(parsed_url.query, keep_blank_values=True)
        )
    )
    return parse.urlunsplit((parsed_url.scheme, netloc, parsed_url.path or "/", redacted_query, ""))


def _string_mapping(value: Mapping[str, str], field_name: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise DeclarationValidationError(
                f"HTTP `{field_name}` must contain string keys and values."
            )
        if not key.strip():
            raise DeclarationValidationError(f"HTTP `{field_name}` must not contain empty keys.")
        result[key] = item
    return result


def _ensure_redacted_mapping(value: Mapping[str, str], *, field_name: str) -> None:
    for key, item in value.items():
        if _is_sensitive_name(key) and item != REDACTED:
            raise DeclarationValidationError(
                f"Redacted HTTP evidence `{field_name}` contains unredacted `{key}`."
            )
        if _looks_auth_bearing(item) and item != REDACTED:
            raise DeclarationValidationError(
                f"Redacted HTTP evidence `{field_name}` contains auth-bearing value."
            )


def _ensure_redacted_url(url: str) -> None:
    parsed_url = parse.urlsplit(url)
    if parsed_url.username is not None and parsed_url.username != "redacted":
        raise DeclarationValidationError("Redacted HTTP evidence `url` contains user info.")
    if parsed_url.password is not None:
        raise DeclarationValidationError("Redacted HTTP evidence `url` contains user info.")
    for key, value in parse.parse_qsl(parsed_url.query, keep_blank_values=True):
        if _is_sensitive_name(key) and value != REDACTED:
            raise DeclarationValidationError(
                f"Redacted HTTP evidence `url` contains unredacted `{key}`."
            )
    for key, value in parse.parse_qsl(parsed_url.fragment, keep_blank_values=True):
        if _is_sensitive_name(key) and value != REDACTED:
            raise DeclarationValidationError(
                f"Redacted HTTP evidence `url` contains unredacted fragment `{key}`."
            )


def _is_sensitive_name(name: str) -> bool:
    normalized = name.strip().lower().replace("-", "_")
    if normalized in _SENSITIVE_EXACT_NAMES:
        return True
    return any(fragment.replace("-", "_") in normalized for fragment in _SENSITIVE_NAME_FRAGMENTS)


def is_sensitive_evidence_name(name: str) -> bool:
    return _is_sensitive_name(name)


def _looks_auth_bearing(value: str) -> bool:
    lowered = value.strip().lower()
    return (
        lowered.startswith(("bearer ", "basic ", "token "))
        or "-----begin private key-----" in lowered
    )


__all__ = [
    "HttpRequest",
    "HttpResponse",
    "HttpTransport",
    "HttpTransportFailureEvidence",
    "REDACTED",
    "RedactedHttpRequestEvidence",
    "is_sensitive_evidence_name",
    "redacted_request_evidence",
    "retry_after_seconds",
    "retry_after_seconds_from_response",
    "transport_failure_evidence",
]
