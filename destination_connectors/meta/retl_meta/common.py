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
from retl.destinations.identifiers import sha256_hex
from retl.destinations.receipts import sanitize_partner_error_detail
from retl.errors import DeclarationValidationError

META_DEFAULT_BASE_URL = "https://graph.facebook.com"
META_DEFAULT_API_VERSION = "v25.0"
MAX_CUSTOM_AUDIENCE_ROWS_PER_REQUEST = 10_000
MAX_EVENT_ROWS_PER_REQUEST = 1_000


@dataclass(frozen=True)
class MetaConfig:
    ad_account_id: str
    api_version: str = META_DEFAULT_API_VERSION

    @property
    def normalized_ad_account_id(self) -> str:
        if self.ad_account_id.startswith("act_"):
            return self.ad_account_id
        return f"act_{self.ad_account_id}"


class RequestsMetaTransport:
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


def meta_config(binding: DestinationBinding) -> MetaConfig:
    raw_ad_account_id = binding.config.get("ad_account_id")
    if not isinstance(raw_ad_account_id, str) or not raw_ad_account_id.strip():
        raise DeclarationValidationError("Meta config requires non-empty `ad_account_id`.")
    raw_api_version = binding.config.get("api_version", META_DEFAULT_API_VERSION)
    if not isinstance(raw_api_version, str) or not raw_api_version.strip():
        raise DeclarationValidationError("Meta config requires non-empty `api_version`.")
    api_version = raw_api_version.strip().strip("/")
    if not api_version:
        raise DeclarationValidationError("Meta config requires non-empty `api_version`.")
    return MetaConfig(
        ad_account_id=raw_ad_account_id.strip(),
        api_version=api_version,
    )


def join_url(config: MetaConfig, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{META_DEFAULT_BASE_URL}{normalized_path}"


def transport_from_config(config: Mapping[str, JSONValue]) -> HttpTransport | None:
    candidate = config.get("transport")
    if candidate is not None:
        if callable(getattr(candidate, "send", None)):
            return cast(HttpTransport, candidate)
        raise DeclarationValidationError("Meta config `transport` must expose send(request).")
    return RequestsMetaTransport()


def meta_partner_message(response: HttpResponse) -> str | None:
    error = response.json_body.get("error")
    if isinstance(error, Mapping):
        message = error.get("message")
        code = error.get("code")
        subcode = error.get("error_subcode")
        parts = [str(message)] if isinstance(message, str) and message else []
        if code is not None:
            parts.append(f"code={code}")
        if subcode is not None:
            parts.append(f"subcode={subcode}")
        return " ".join(parts) or None
    message = response.json_body.get("message")
    return str(message) if isinstance(message, str) and message else None


def meta_partner_error_detail(response: HttpResponse) -> str | None:
    error = response.json_body.get("error")
    if isinstance(error, Mapping):
        return sanitize_partner_error_detail(json.dumps(error, sort_keys=True, default=str))
    return sanitize_partner_error_detail(meta_partner_message(response))


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_plain_json(item) for item in value]
    return value


__all__ = [
    "MAX_CUSTOM_AUDIENCE_ROWS_PER_REQUEST",
    "MAX_EVENT_ROWS_PER_REQUEST",
    "META_DEFAULT_API_VERSION",
    "META_DEFAULT_BASE_URL",
    "MetaConfig",
    "RequestsMetaTransport",
    "join_url",
    "meta_config",
    "meta_partner_error_detail",
    "meta_partner_message",
    "sha256_hex",
    "transport_from_config",
]
