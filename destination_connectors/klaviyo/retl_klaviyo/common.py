from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

import requests

from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.http import (
    HttpRequest,
    HttpResponse,
    HttpTransport,
)
from retl.destinations.receipts import sanitize_partner_error_detail
from retl.errors import DeclarationValidationError

KLAVIYO_DEFAULT_BASE_URL = "https://a.klaviyo.com"
KLAVIYO_DEFAULT_API_REVISION = "2026-04-15"
MAX_PROFILES_PER_REQUEST = 10_000
MAX_LIST_RELATIONSHIP_PROFILES_PER_REQUEST = 1_000
MAX_PROFILE_IMPORT_PAYLOAD_BYTES = 5_000_000


@dataclass(frozen=True)
class KlaviyoConfig:
    api_revision: str = KLAVIYO_DEFAULT_API_REVISION


class RequestsKlaviyoTransport:
    def send(self, request: HttpRequest) -> HttpResponse:
        response = requests.request(
            request.method,
            request.url,
            params=dict(request.query),
            headers=dict(request.headers),
            json=_plain_json(request.json_body),
            data=request.body,
            timeout=request.timeout_seconds,
        )
        json_body: Mapping[str, object] = {}
        try:
            parsed = response.json()
        except ValueError:
            parsed = None
        if isinstance(parsed, Mapping):
            json_body = cast(Mapping[str, object], parsed)
        return HttpResponse(
            status_code=response.status_code,
            headers={key: value for key, value in response.headers.items()},
            json_body=json_body,
            body_text=response.text[:512] if response.text else None,
        )


def klaviyo_config(binding: DestinationBinding) -> KlaviyoConfig:
    raw_api_revision = binding.config.get("api_revision", KLAVIYO_DEFAULT_API_REVISION)
    if not isinstance(raw_api_revision, str) or not raw_api_revision.strip():
        raise DeclarationValidationError("Klaviyo config requires non-empty `api_revision`.")
    return KlaviyoConfig(api_revision=raw_api_revision.strip())


def join_url(config: KlaviyoConfig, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{KLAVIYO_DEFAULT_BASE_URL}{normalized_path}"


def transport_from_config(config: Mapping[str, JSONValue]) -> HttpTransport | None:
    candidate = config.get("transport")
    if candidate is not None:
        if callable(getattr(candidate, "send", None)):
            return cast(HttpTransport, candidate)
        raise DeclarationValidationError("Klaviyo config `transport` must expose send(request).")
    return RequestsKlaviyoTransport()


def klaviyo_partner_message(response: HttpResponse) -> str | None:
    errors = response.json_body.get("errors")
    if isinstance(errors, list):
        parts: list[str] = []
        for error in errors[:3]:
            if not isinstance(error, Mapping):
                continue
            title = error.get("title")
            detail = error.get("detail")
            code = error.get("code")
            message_parts = [
                str(value) for value in (title, detail) if isinstance(value, str) and value.strip()
            ]
            if code is not None:
                message_parts.append(f"code={code}")
            if message_parts:
                parts.append(" ".join(message_parts))
        return "; ".join(parts) or None
    message = response.json_body.get("message")
    return str(message) if isinstance(message, str) and message else None


def klaviyo_partner_error_detail(response: HttpResponse) -> str | None:
    errors = response.json_body.get("errors")
    if isinstance(errors, list):
        return sanitize_partner_error_detail(json.dumps(errors[:3], sort_keys=True, default=str))
    return sanitize_partner_error_detail(klaviyo_partner_message(response))


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_plain_json(item) for item in value]
    return value


__all__ = [
    "KLAVIYO_DEFAULT_API_REVISION",
    "KLAVIYO_DEFAULT_BASE_URL",
    "MAX_LIST_RELATIONSHIP_PROFILES_PER_REQUEST",
    "MAX_PROFILE_IMPORT_PAYLOAD_BYTES",
    "MAX_PROFILES_PER_REQUEST",
    "KlaviyoConfig",
    "RequestsKlaviyoTransport",
    "join_url",
    "klaviyo_config",
    "klaviyo_partner_error_detail",
    "klaviyo_partner_message",
    "transport_from_config",
]
