from __future__ import annotations

import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import cast

import requests

from retl.auth import ResolvedAuth
from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.http import (
    HttpRequest,
    HttpResponse,
    HttpTransport,
)
from retl.destinations.identifiers import hash_or_preserve_sha256_hex, sha256_hex
from retl.destinations.receipts import sanitize_partner_error_detail
from retl.errors import DeclarationValidationError

GOOGLE_ADS_DATA_MANAGER_DEFAULT_BASE_URL = "https://datamanager.googleapis.com"
DATA_MANAGER_API_VERSION = "v" + "1"
GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
MAX_AUDIENCE_MEMBERS_PER_REQUEST = 10_000
MAX_EVENTS_PER_REQUEST = 2_000
MAX_USER_IDENTIFIERS_PER_MEMBER = 10
GOOGLE_ADS_ACCOUNT_TYPE = "GOOGLE_ADS"
DATA_MANAGER_SCOPE = "https://www.googleapis.com/auth/datamanager"
CUSTOMER_MATCH_DOCS_URL = (
    "https://developers.google.com/data-manager/api/devguides/audiences/google-ads/customer-match"
)

_ALLOWED_ACCOUNT_TYPES = {
    "GOOGLE_ADS",
    "DATA_PARTNER",
    "DISPLAY_VIDEO_ADVERTISER",
    "DISPLAY_VIDEO_PARTNER",
    "GOOGLE_ANALYTICS_PROPERTY",
}
_ALLOWED_CONSENT_STATUSES = {
    "CONSENT_GRANTED",
    "CONSENT_DENIED",
    "CONSENT_STATUS_UNSPECIFIED",
}
GOOGLE_ADS_DATA_MANAGER_CONSENT_STATUSES = frozenset(_ALLOWED_CONSENT_STATUSES)


@dataclass(frozen=True)
class GoogleAdsDataManagerConfig:
    operating_account_id: str
    operating_account_type: str = GOOGLE_ADS_ACCOUNT_TYPE
    login_account_id: str | None = None
    login_account_type: str = GOOGLE_ADS_ACCOUNT_TYPE
    linked_account_id: str | None = None
    linked_account_type: str | None = None
    event_destination_id: str | None = None
    encoding: str = "HEX"
    customer_match_terms_accepted: bool = False
    ad_user_data_consent: str | None = None
    ad_personalization_consent: str | None = None
    request_status_poll_interval_seconds: float = 1.0
    request_status_poll_timeout_seconds: float = 16.0


class RequestsGoogleAdsDataManagerTransport:
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


def google_ads_data_manager_config(binding: DestinationBinding) -> GoogleAdsDataManagerConfig:
    raw_operating_account_id = binding.config.get("operating_account_id")
    if not isinstance(raw_operating_account_id, str) or not raw_operating_account_id.strip():
        raise DeclarationValidationError(
            "Google Ads Data Manager config requires non-empty `operating_account_id`."
        )
    operating_account_type = _account_type(
        binding.config.get("operating_account_type", GOOGLE_ADS_ACCOUNT_TYPE),
        field_name="operating_account_type",
    )
    login_account_id = _optional_string(binding.config.get("login_account_id"))
    login_account_type = _account_type(
        binding.config.get("login_account_type", GOOGLE_ADS_ACCOUNT_TYPE),
        field_name="login_account_type",
    )
    linked_account_id = _optional_string(binding.config.get("linked_account_id"))
    linked_account_type = None
    if linked_account_id is not None:
        linked_account_type = _account_type(
            binding.config.get("linked_account_type"),
            field_name="linked_account_type",
        )
    return GoogleAdsDataManagerConfig(
        operating_account_id=raw_operating_account_id.strip(),
        operating_account_type=operating_account_type,
        login_account_id=login_account_id,
        login_account_type=login_account_type,
        linked_account_id=linked_account_id,
        linked_account_type=linked_account_type,
        event_destination_id=_optional_string(binding.config.get("event_destination_id")),
        encoding=_encoding(binding.config.get("encoding", "HEX")),
        customer_match_terms_accepted=_optional_bool(
            binding.config.get("customer_match_terms_accepted")
        ),
        ad_user_data_consent=_consent_status(binding.config.get("ad_user_data_consent")),
        ad_personalization_consent=_consent_status(
            binding.config.get("ad_personalization_consent")
        ),
        request_status_poll_interval_seconds=_nonnegative_float(
            binding.config.get("request_status_poll_interval_seconds", 1.0),
            field_name="request_status_poll_interval_seconds",
        ),
        request_status_poll_timeout_seconds=_nonnegative_float(
            binding.config.get("request_status_poll_timeout_seconds", 16.0),
            field_name="request_status_poll_timeout_seconds",
        ),
    )


def service_account_auth(values: Mapping[str, str], context: Mapping[str, object]) -> ResolvedAuth:
    try:
        service_account = importlib.import_module("google.oauth2.service_account")
        transport_requests = importlib.import_module("google.auth.transport.requests")
    except ImportError as exc:
        raise DeclarationValidationError(
            "Google Ads Data Manager auth mode `service_account` requires `google-auth`."
        ) from exc
    info: dict[str, str] = {
        "type": "service_account",
        "project_id": values["project_id"],
        "client_email": values["client_email"],
        "private_key": values["private_key"].replace("\\n", "\n"),
        "token_uri": values.get("token_uri", GOOGLE_OAUTH_TOKEN_URL),
    }
    private_key_id = values.get("private_key_id")
    if private_key_id:
        info["private_key_id"] = private_key_id
    try:
        credentials = service_account.Credentials.from_service_account_info(
            info,
            scopes=[DATA_MANAGER_SCOPE],
        )
        credentials.refresh(transport_requests.Request())
    except Exception as exc:
        raise DeclarationValidationError(
            "Google Ads Data Manager service account token exchange failed."
        ) from exc
    token = getattr(credentials, "token", None)
    if not isinstance(token, str) or not token.strip():
        raise DeclarationValidationError(
            "Google Ads Data Manager service account token exchange returned no access token."
        )
    expiry = getattr(credentials, "expiry", None)
    return ResolvedAuth(
        mode=str(context.get("mode", "service_account")),
        headers={"Authorization": f"Bearer {token}"},
        token_expires_at=_timestamp(expiry),
    )


def join_url(config: GoogleAdsDataManagerConfig, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{GOOGLE_ADS_DATA_MANAGER_DEFAULT_BASE_URL}{normalized_path}"


def hashed_or_normalized_email(value: str) -> str:
    return hash_or_preserve_sha256_hex(value, normalizer=_normalize_google_email)


def hashed_or_normalized_phone(value: str) -> str:
    return hash_or_preserve_sha256_hex(value)


def _normalize_google_email(value: str) -> str:
    normalized = "".join(value.lower().split())
    local, sep, domain = normalized.partition("@")
    if sep and domain in {"gmail.com", "googlemail.com"}:
        return f"{local.replace('.', '')}@{domain}"
    return normalized


def transport_from_config(config: Mapping[str, JSONValue]) -> HttpTransport | None:
    candidate = config.get("transport")
    if candidate is not None:
        if callable(getattr(candidate, "send", None)):
            return cast(HttpTransport, candidate)
        raise DeclarationValidationError(
            "Google Ads Data Manager config `transport` must expose send(request)."
        )
    return RequestsGoogleAdsDataManagerTransport()


def google_ads_data_manager_partner_message(response: HttpResponse) -> str | None:
    error = response.json_body.get("error")
    if isinstance(error, Mapping):
        message = error.get("message")
        status = error.get("status")
        code = error.get("code")
        parts = [str(message)] if isinstance(message, str) and message else []
        if status is not None:
            parts.append(f"status={status}")
        if code is not None:
            parts.append(f"code={code}")
        return " ".join(parts) or None
    message = response.json_body.get("message")
    return str(message) if isinstance(message, str) and message else None


def google_ads_data_manager_partner_error_detail(response: HttpResponse) -> str | None:
    error = response.json_body.get("error")
    if isinstance(error, Mapping):
        return sanitize_partner_error_detail(json.dumps(error, sort_keys=True, default=str))
    return sanitize_partner_error_detail(google_ads_data_manager_partner_message(response))


def _account_type(value: JSONValue | None, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"Google Ads Data Manager config `{field_name}` must be a non-empty account type."
        )
    normalized = value.strip().upper()
    if normalized not in _ALLOWED_ACCOUNT_TYPES:
        allowed = ", ".join(sorted(_ALLOWED_ACCOUNT_TYPES))
        raise DeclarationValidationError(
            f"Google Ads Data Manager config `{field_name}` must be one of: {allowed}."
        )
    return normalized


def _optional_bool(value: JSONValue | None) -> bool:
    if value is True:
        return True
    if value is False or value is None:
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no", ""}:
            return False
    raise DeclarationValidationError(
        "Google Ads Data Manager config `customer_match_terms_accepted` must be a boolean."
    )


def _nonnegative_float(value: JSONValue | None, *, field_name: str) -> float:
    if isinstance(value, bool) or value is None:
        raise DeclarationValidationError(
            f"Google Ads Data Manager config `{field_name}` must be a non-negative number."
        )
    if isinstance(value, int | float):
        parsed = float(value)
    elif isinstance(value, str):
        try:
            parsed = float(value.strip())
        except ValueError as exc:
            raise DeclarationValidationError(
                f"Google Ads Data Manager config `{field_name}` must be a non-negative number."
            ) from exc
    else:
        raise DeclarationValidationError(
            f"Google Ads Data Manager config `{field_name}` must be a non-negative number."
        )
    if parsed < 0:
        raise DeclarationValidationError(
            f"Google Ads Data Manager config `{field_name}` must be a non-negative number."
        )
    return parsed


def _encoding(value: JSONValue | None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            "Google Ads Data Manager config `encoding` must be `HEX` or `BASE64`."
        )
    normalized = value.strip().upper()
    if normalized not in {"HEX", "BASE64"}:
        raise DeclarationValidationError(
            "Google Ads Data Manager config `encoding` must be `HEX` or `BASE64`."
        )
    return normalized


def _consent_status(value: JSONValue | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            "Google Ads Data Manager consent config values must be non-empty strings."
        )
    try:
        return normalize_consent_status(value)
    except ValueError as exc:
        allowed = ", ".join(sorted(GOOGLE_ADS_DATA_MANAGER_CONSENT_STATUSES))
        raise DeclarationValidationError(
            f"Google Ads Data Manager consent config values must be one of: {allowed}."
        ) from exc


def normalize_consent_status(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in GOOGLE_ADS_DATA_MANAGER_CONSENT_STATUSES:
        raise ValueError(f"Unsupported Google Ads Data Manager consent status: {value!r}")
    return normalized


def _optional_string(value: JSONValue | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DeclarationValidationError("Google Ads Data Manager account ids must be strings.")
    stripped = value.strip()
    return stripped or None


def _timestamp(value: object) -> float | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_plain_json(item) for item in value]
    return value


__all__ = [
    "CUSTOMER_MATCH_DOCS_URL",
    "DATA_MANAGER_API_VERSION",
    "DATA_MANAGER_SCOPE",
    "GOOGLE_ADS_DATA_MANAGER_DEFAULT_BASE_URL",
    "GOOGLE_OAUTH_TOKEN_URL",
    "GoogleAdsDataManagerConfig",
    "MAX_AUDIENCE_MEMBERS_PER_REQUEST",
    "MAX_USER_IDENTIFIERS_PER_MEMBER",
    "RequestsGoogleAdsDataManagerTransport",
    "google_ads_data_manager_config",
    "google_ads_data_manager_partner_error_detail",
    "google_ads_data_manager_partner_message",
    "hashed_or_normalized_email",
    "hashed_or_normalized_phone",
    "join_url",
    "service_account_auth",
    "sha256_hex",
    "transport_from_config",
]
