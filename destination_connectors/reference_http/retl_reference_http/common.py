from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast
from urllib import parse

from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.http import (
    REDACTED,
    HttpTransport,
    is_sensitive_evidence_name,
)
from retl.errors import DeclarationValidationError

REFERENCE_HTTP_DEFAULT_BASE_URL = "https://reference-http.example.test"
REFERENCE_HTTP_DEFAULT_MAX_ROWS_PER_REQUEST = 1000


@dataclass(frozen=True)
class ReferenceHttpConfig:
    base_url: str = REFERENCE_HTTP_DEFAULT_BASE_URL
    request_batch_max_rows: int = REFERENCE_HTTP_DEFAULT_MAX_ROWS_PER_REQUEST


def reference_http_config(binding: DestinationBinding) -> ReferenceHttpConfig:
    return ReferenceHttpConfig(
        base_url=_base_url(binding.config.get("base_url", REFERENCE_HTTP_DEFAULT_BASE_URL)),
        request_batch_max_rows=_request_batch_max_rows(
            binding.config.get(
                "request_batch_max_rows",
                REFERENCE_HTTP_DEFAULT_MAX_ROWS_PER_REQUEST,
            )
        ),
    )


def join_url(config: ReferenceHttpConfig, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{config.base_url}{normalized_path}"


def transport_from_config(config: Mapping[str, JSONValue]) -> HttpTransport | None:
    candidate = config.get("transport")
    if candidate is None:
        return None
    if callable(getattr(candidate, "send", None)):
        return cast(HttpTransport, candidate)
    raise DeclarationValidationError("Reference HTTP config `transport` must expose send(request).")


def public_config(config: Mapping[str, JSONValue]) -> Mapping[str, JSONValue]:
    return {
        key: value
        for key, value in config.items()
        if key != "transport" and not is_sensitive_evidence_name(key)
    }


def _base_url(raw: JSONValue | object) -> str:
    if not isinstance(raw, str):
        raise DeclarationValidationError(
            "Reference HTTP config `base_url` must be an absolute HTTP(S) URL."
        )
    parsed = parse.urlsplit(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise DeclarationValidationError(
            "Reference HTTP config `base_url` must be an absolute HTTP(S) URL."
        )
    if parsed.username is not None or parsed.password is not None:
        raise DeclarationValidationError(
            "Reference HTTP config `base_url` must not include user info."
        )
    if parsed.query:
        sensitive = sorted(
            key
            for key, value in parse.parse_qsl(parsed.query, keep_blank_values=True)
            if is_sensitive_evidence_name(key) or value == REDACTED
        )
        detail = f": {sensitive}" if sensitive else "."
        raise DeclarationValidationError(
            f"Reference HTTP config `base_url` must not include query parameters{detail}"
        )
    if parsed.fragment:
        raise DeclarationValidationError(
            "Reference HTTP config `base_url` must not include fragments."
        )
    return parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _request_batch_max_rows(raw: JSONValue | object) -> int:
    if isinstance(raw, str) and raw.strip().isdigit():
        raw = int(raw.strip())
    if not isinstance(raw, int) or isinstance(raw, bool):
        raise DeclarationValidationError(
            "Reference HTTP config `request_batch_max_rows` must be an integer."
        )
    if raw < 1:
        raise DeclarationValidationError(
            "Reference HTTP config `request_batch_max_rows` must be at least 1."
        )
    return raw


__all__ = [
    "REFERENCE_HTTP_DEFAULT_BASE_URL",
    "REFERENCE_HTTP_DEFAULT_MAX_ROWS_PER_REQUEST",
    "ReferenceHttpConfig",
    "join_url",
    "public_config",
    "reference_http_config",
    "transport_from_config",
]
