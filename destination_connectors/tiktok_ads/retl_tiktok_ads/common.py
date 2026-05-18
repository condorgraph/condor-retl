from __future__ import annotations

import hashlib
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
from retl.destinations.identifiers import hash_or_preserve_sha256_hex
from retl.destinations.receipts import sanitize_partner_error_detail
from retl.errors import DeclarationValidationError

TIKTOK_ADS_DEFAULT_BASE_URL = "https://business-api.tiktok.com"
TIKTOK_ADS_API_VERSION = "v" + "1.3"
MAX_CUSTOM_AUDIENCE_FILE_ROWS_PER_REQUEST = 100_000
TIKTOK_ADS_ID_TYPES = frozenset(
    {
        "EMAIL_SHA256",
        "PHONE_SHA256",
        "IDFA_SHA256",
        "GAID_SHA256",
        "MAID_SHA256",
    }
)


@dataclass(frozen=True)
class TikTokAdsConfig:
    advertiser_id: str
    api_version: str = TIKTOK_ADS_API_VERSION
    mobile_advertising_id_type: str = "MAID_SHA256"


class RequestsTikTokAdsTransport:
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

    def upload_audience_file(
        self,
        *,
        url: str,
        headers: Mapping[str, str],
        advertiser_id: str,
        calculate_type: str,
        filename: str,
        content: bytes,
    ) -> HttpResponse:
        response = requests.post(
            url,
            headers=dict(headers),
            data={
                "advertiser_id": advertiser_id,
                "file_signature": hashlib.md5(content).hexdigest(),
                "calculate_type": calculate_type,
            },
            files={"file": (filename, content, "text/plain")},
            timeout=30,
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


def tiktok_ads_config(binding: DestinationBinding) -> TikTokAdsConfig:
    advertiser_id = _required_string(binding.config.get("advertiser_id"), "advertiser_id")
    return TikTokAdsConfig(
        advertiser_id=advertiser_id,
        api_version=_api_version(binding.config.get("api_version", TIKTOK_ADS_API_VERSION)),
        mobile_advertising_id_type=_id_type(
            binding.config.get("mobile_advertising_id_type", "MAID_SHA256"),
            field_name="mobile_advertising_id_type",
        ),
    )


def join_url(config: TikTokAdsConfig, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{TIKTOK_ADS_DEFAULT_BASE_URL}{normalized_path}"


def transport_from_config(config: Mapping[str, JSONValue]) -> HttpTransport | None:
    candidate = config.get("transport")
    if candidate is not None:
        if callable(getattr(candidate, "send", None)):
            return cast(HttpTransport, candidate)
        raise DeclarationValidationError("TikTok Ads config `transport` must expose send(request).")
    return RequestsTikTokAdsTransport()


def tiktok_identifier_value(identifier_type: str, value: str) -> str:
    return hash_or_preserve_sha256_hex(value, normalizer=_normalizer(identifier_type))


def tiktok_partner_message(response: HttpResponse) -> str | None:
    message = response.json_body.get("message")
    code = response.json_body.get("code")
    parts = [str(message)] if isinstance(message, str) and message.strip() else []
    if code is not None and code not in (0, "0"):
        parts.append(f"code={code}")
    return " ".join(parts) or None


def tiktok_partner_error_detail(response: HttpResponse) -> str | None:
    if response.json_body:
        return sanitize_partner_error_detail(
            json.dumps(_plain_json(response.json_body), sort_keys=True)
        )
    return sanitize_partner_error_detail(response.body_text)


def _required_string(raw: object, field_name: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise DeclarationValidationError(f"TikTok Ads config requires non-empty `{field_name}`.")
    return raw.strip()


def _api_version(raw: object) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise DeclarationValidationError("TikTok Ads config requires non-empty `api_version`.")
    value = raw.strip().strip("/")
    if value != TIKTOK_ADS_API_VERSION:
        raise DeclarationValidationError(
            f"TikTok Ads config `api_version` must be `{TIKTOK_ADS_API_VERSION}`."
        )
    return value


def _id_type(raw: object, *, field_name: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise DeclarationValidationError(f"TikTok Ads config requires non-empty `{field_name}`.")
    value = raw.strip().upper()
    if value not in TIKTOK_ADS_ID_TYPES:
        raise DeclarationValidationError(
            f"TikTok Ads config `{field_name}` must be one of "
            f"{', '.join(sorted(TIKTOK_ADS_ID_TYPES))}."
        )
    return value


def _normalizer(identifier_type: str):
    if identifier_type in {"email", "phone_e164"}:
        return lambda value: value.strip().lower()
    return lambda value: value.strip().lower()


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_plain_json(item) for item in value]
    return value


__all__ = [
    "MAX_CUSTOM_AUDIENCE_FILE_ROWS_PER_REQUEST",
    "TIKTOK_ADS_API_VERSION",
    "TIKTOK_ADS_DEFAULT_BASE_URL",
    "TikTokAdsConfig",
    "RequestsTikTokAdsTransport",
    "join_url",
    "tiktok_ads_config",
    "tiktok_identifier_value",
    "tiktok_partner_error_detail",
    "tiktok_partner_message",
    "transport_from_config",
]
