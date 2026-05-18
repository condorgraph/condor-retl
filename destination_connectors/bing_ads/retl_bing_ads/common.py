from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

import requests

from retl.auth import ResolvedAuth
from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.http import (
    HttpRequest,
    HttpResponse,
    HttpTransport,
)
from retl.destinations.identifiers import hash_or_preserve_sha256_hex
from retl.destinations.receipts import sanitize_partner_error_detail
from retl.errors import DeclarationValidationError

BING_ADS_DEFAULT_BASE_URL = "https://campaign.api.bingads.microsoft.com"
BING_ADS_API_VERSION = "v13"
MAX_CUSTOMER_LIST_ITEMS_PER_REQUEST = 1_000


@dataclass(frozen=True)
class BingAdsConfig:
    customer_account_id: str
    customer_id: str
    api_version: str = BING_ADS_API_VERSION
    target_scope: str = "Account"
    membership_duration: int = -1
    accept_customer_match_terms: bool = True


class RequestsBingAdsTransport:
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


def microsoft_advertising_auth(
    values: Mapping[str, str],
    context: Mapping[str, object],
) -> ResolvedAuth:
    access_token = values.get("access_token", "").strip()
    developer_token = values.get("developer_token", "").strip()
    if not access_token:
        raise DeclarationValidationError("Bing Ads auth requires non-empty `access_token`.")
    if not developer_token:
        raise DeclarationValidationError("Bing Ads auth requires non-empty `developer_token`.")
    return ResolvedAuth(
        mode="microsoft_advertising",
        headers={
            "Authorization": f"Bearer {access_token}",
            "DeveloperToken": developer_token,
        },
    )


def bing_ads_config(binding: DestinationBinding) -> BingAdsConfig:
    customer_account_id = _required_string(
        binding.config.get("customer_account_id"),
        "customer_account_id",
    )
    customer_id = _required_string(binding.config.get("customer_id"), "customer_id")
    api_version = _api_version(binding.config.get("api_version", BING_ADS_API_VERSION))
    return BingAdsConfig(
        customer_account_id=customer_account_id,
        customer_id=customer_id,
        api_version=api_version,
        target_scope=_target_scope(binding.config.get("target_scope", "Account")),
        membership_duration=_membership_duration(binding.config.get("membership_duration", -1)),
        accept_customer_match_terms=_optional_bool(
            binding.config.get("accept_customer_match_terms", True),
            field_name="accept_customer_match_terms",
        ),
    )


def join_url(config: BingAdsConfig, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{BING_ADS_DEFAULT_BASE_URL}{normalized_path}"


def bing_ads_headers(*, config: BingAdsConfig, resolved_auth: object) -> Mapping[str, str]:
    headers = getattr(resolved_auth, "headers", {})
    if not isinstance(headers, Mapping):
        headers = {}
    return {
        **{str(key): str(value) for key, value in headers.items()},
        "CustomerAccountId": config.customer_account_id,
        "CustomerId": config.customer_id,
    }


def hashed_customer_list_item(identifier_type: str, value: str) -> str:
    if identifier_type == "mobile_advertising_id":
        return value.strip()
    return hash_or_preserve_sha256_hex(value, normalizer=_lowercase_identifier)


def transport_from_config(config: Mapping[str, JSONValue]) -> HttpTransport | None:
    candidate = config.get("transport")
    if candidate is not None:
        if callable(getattr(candidate, "send", None)):
            return cast(HttpTransport, candidate)
        raise DeclarationValidationError("Bing Ads config `transport` must expose send(request).")
    return RequestsBingAdsTransport()


def bing_ads_partner_message(response: HttpResponse) -> str | None:
    errors = response.json_body.get("PartialErrors")
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, Mapping):
            message = first.get("Message")
            code = first.get("Code")
            error_code = first.get("ErrorCode")
            parts = [str(message)] if isinstance(message, str) and message else []
            if code is not None:
                parts.append(f"code={code}")
            if error_code is not None:
                parts.append(f"error_code={error_code}")
            return " ".join(parts) or None
    message = response.json_body.get("Message")
    if isinstance(message, str) and message:
        return str(message)
    return response.body_text if response.body_text else None


def bing_ads_partner_error_detail(response: HttpResponse) -> str | None:
    errors = response.json_body.get("PartialErrors")
    if isinstance(errors, list) and errors:
        return sanitize_partner_error_detail(json.dumps(errors[:3], sort_keys=True, default=str))
    return sanitize_partner_error_detail(bing_ads_partner_message(response))


def _required_string(raw: object, field_name: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise DeclarationValidationError(f"Bing Ads config requires non-empty `{field_name}`.")
    return raw.strip()


def _api_version(raw: object) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise DeclarationValidationError("Bing Ads config requires non-empty `api_version`.")
    value = raw.strip().strip("/")
    if value != BING_ADS_API_VERSION:
        raise DeclarationValidationError("Bing Ads config `api_version` must be `v13`.")
    return value


def _target_scope(raw: object) -> str:
    if not isinstance(raw, str):
        raise DeclarationValidationError(
            "Bing Ads config `target_scope` must be `Account` or `Customer`."
        )
    value = raw.strip()
    if value not in {"Account", "Customer"}:
        raise DeclarationValidationError(
            "Bing Ads config `target_scope` must be `Account` or `Customer`."
        )
    return value


def _membership_duration(raw: object) -> int:
    if isinstance(raw, str):
        value = raw.strip()
        try:
            raw = int(value)
        except ValueError as exc:
            raise DeclarationValidationError(
                "Bing Ads config `membership_duration` must be an integer."
            ) from exc
    if not isinstance(raw, int) or isinstance(raw, bool):
        raise DeclarationValidationError(
            "Bing Ads config `membership_duration` must be an integer."
        )
    if raw != -1 and not 1 <= raw <= 390:
        raise DeclarationValidationError(
            "Bing Ads config `membership_duration` must be -1 or between 1 and 390."
        )
    return raw


def _optional_bool(raw: object, *, field_name: str) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        value = raw.strip().lower()
        if value == "true":
            return True
        if value == "false":
            return False
    raise DeclarationValidationError(f"Bing Ads config `{field_name}` must be a boolean.")


def _lowercase_identifier(value: str) -> str:
    return value.strip().lower()


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_plain_json(item) for item in value]
    return value


__all__ = [
    "BING_ADS_API_VERSION",
    "BING_ADS_DEFAULT_BASE_URL",
    "BingAdsConfig",
    "MAX_CUSTOMER_LIST_ITEMS_PER_REQUEST",
    "RequestsBingAdsTransport",
    "bing_ads_config",
    "bing_ads_headers",
    "bing_ads_partner_error_detail",
    "bing_ads_partner_message",
    "hashed_customer_list_item",
    "join_url",
    "microsoft_advertising_auth",
    "transport_from_config",
]
