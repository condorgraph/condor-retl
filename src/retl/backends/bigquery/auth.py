from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any

from retl.auth import AuthMode, AuthResolutionError, SecretResolver, native, none
from retl.declarations import CredentialValue, SecretLiteral, SecretRef
from retl.errors import DeclarationValidationError

BIGQUERY_APPLICATION_DEFAULT_AUTH = none("application_default")
BIGQUERY_SERVICE_ACCOUNT_JSON_AUTH = native(
    "service_account_json",
    required_fields=("credentials_json",),
)
BIGQUERY_SERVICE_ACCOUNT_FILE_AUTH = native(
    "service_account_file",
    required_fields=("credentials_path",),
)
BIGQUERY_AUTH_MODES = (
    BIGQUERY_APPLICATION_DEFAULT_AUTH,
    BIGQUERY_SERVICE_ACCOUNT_JSON_AUTH,
    BIGQUERY_SERVICE_ACCOUNT_FILE_AUTH,
)
_NAMESPACE_SEGMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class BigQueryBackendAuth:
    mode: AuthMode = BIGQUERY_APPLICATION_DEFAULT_AUTH
    credentials: Mapping[str, CredentialValue] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.mode.name not in {
            "application_default",
            "service_account_json",
            "service_account_file",
        }:
            raise DeclarationValidationError(
                "BigQuery backend auth mode must be `application_default`, "
                "`service_account_json`, or `service_account_file`."
            )
        missing = tuple(
            field for field in self.mode.required_fields if field not in self.credentials
        )
        if missing:
            rendered = ", ".join(sorted(missing))
            raise DeclarationValidationError(
                f"BigQuery auth mode `{self.mode.name}` missing required credential "
                f"field(s): {rendered}."
            )
        unknown = tuple(sorted(set(self.credentials) - set(self.mode.field_names)))
        if unknown:
            rendered = ", ".join(unknown)
            raise DeclarationValidationError(
                f"BigQuery auth mode `{self.mode.name}` received undeclared credential "
                f"field(s): {rendered}."
            )
        object.__setattr__(self, "credentials", MappingProxyType(dict(self.credentials)))

    @classmethod
    def application_default(cls) -> BigQueryBackendAuth:
        return cls(mode=BIGQUERY_APPLICATION_DEFAULT_AUTH, credentials={})

    @classmethod
    def from_namespace(
        cls,
        *,
        auth_mode: str,
        credential_namespace: str,
    ) -> BigQueryBackendAuth:
        mode = bigquery_auth_mode(auth_mode)
        if mode.name == "application_default":
            return cls.application_default()
        namespace = _validate_namespace(credential_namespace)
        credentials = {
            field_name: SecretRef(f"{namespace}.{field_name}") for field_name in mode.field_names
        }
        return cls(mode=mode, credentials=credentials)

    @property
    def evidence(self) -> Mapping[str, object]:
        return {
            "mode": self.mode.name,
            "required_fields": {
                field_name: field_name in self.credentials
                for field_name in self.mode.required_fields
            },
            "resolved": False,
        }


def bigquery_auth_mode(name: str) -> AuthMode:
    for mode in BIGQUERY_AUTH_MODES:
        if mode.name == name:
            return mode
    available = ", ".join(mode.name for mode in BIGQUERY_AUTH_MODES)
    raise DeclarationValidationError(
        f"Unknown BigQuery backend auth_mode `{name}`. Available auth modes: {available}."
    )


def bigquery_client_kwargs(
    auth: BigQueryBackendAuth,
    *,
    resolver: SecretResolver,
    scopes: tuple[str, ...] = ("https://www.googleapis.com/auth/cloud-platform",),
) -> dict[str, object]:
    if auth.mode.name == "application_default":
        return {}
    values = _resolve_bigquery_credentials(auth=auth, resolver=resolver)
    credentials_info: Mapping[str, Any]
    if auth.mode.name == "service_account_json":
        try:
            credentials_info = json.loads(values["credentials_json"])
        except json.JSONDecodeError as exc:
            raise AuthResolutionError(
                "BigQuery service_account_json credentials_json could not be parsed."
            ) from exc
    elif auth.mode.name == "service_account_file":
        try:
            credentials_info = json.loads(
                Path(values["credentials_path"]).read_text(encoding="utf-8")
            )
        except OSError as exc:
            raise AuthResolutionError(
                "BigQuery service_account_file credentials_path could not be read."
            ) from exc
        except json.JSONDecodeError as exc:
            raise AuthResolutionError(
                "BigQuery service_account_file credentials_path did not contain JSON."
            ) from exc
    else:
        raise DeclarationValidationError(f"Unsupported BigQuery auth mode `{auth.mode.name}`.")

    try:
        from google.oauth2 import service_account
    except ImportError as exc:
        raise AuthResolutionError(
            "BigQuery service-account auth requires the optional `bigquery` dependency."
        ) from exc

    credentials = service_account.Credentials.from_service_account_info(
        dict(credentials_info),
        scopes=scopes,
    )
    return {"credentials": credentials}


def _resolve_bigquery_credentials(
    *,
    auth: BigQueryBackendAuth,
    resolver: SecretResolver,
) -> Mapping[str, str]:
    resolved: dict[str, str] = {}
    for field_name in auth.mode.required_fields:
        value = auth.credentials[field_name]
        if isinstance(value, SecretRef):
            secret = resolver.resolve(value)
        elif isinstance(value, SecretLiteral):
            secret = value.value
        else:
            raise DeclarationValidationError(
                f"BigQuery backend auth credential `{field_name}` must be a SecretRef "
                "or SecretLiteral."
            )
        if not secret:
            raise DeclarationValidationError(
                f"BigQuery backend auth mode `{auth.mode.name}` has empty credential "
                f"field `{field_name}`."
            )
        resolved[field_name] = secret
    return MappingProxyType(resolved)


def _validate_namespace(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            "BigQuery backend credential_namespace must be a non-empty dotted namespace."
        )
    namespace = value.strip()
    for segment in namespace.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                "BigQuery backend credential_namespace must contain only dotted "
                "identifier segments."
            )
    return namespace


__all__ = [
    "BIGQUERY_APPLICATION_DEFAULT_AUTH",
    "BIGQUERY_AUTH_MODES",
    "BIGQUERY_SERVICE_ACCOUNT_FILE_AUTH",
    "BIGQUERY_SERVICE_ACCOUNT_JSON_AUTH",
    "BigQueryBackendAuth",
    "bigquery_auth_mode",
    "bigquery_client_kwargs",
]
