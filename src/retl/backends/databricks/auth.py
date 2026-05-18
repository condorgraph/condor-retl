from __future__ import annotations

import importlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from retl.auth import AuthMode, SecretResolver, native, validate_required_credentials
from retl.declarations import CredentialValue, SecretLiteral, SecretRef
from retl.errors import DeclarationValidationError

DATABRICKS_PAT_AUTH = native("pat", required_fields=("token",))
DATABRICKS_OAUTH_M2M_AUTH = native(
    "oauth_m2m",
    required_fields=("client_id", "client_secret"),
)
DATABRICKS_AUTH_MODES = (DATABRICKS_PAT_AUTH, DATABRICKS_OAUTH_M2M_AUTH)
_NAMESPACE_SEGMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class DatabricksBackendAuth:
    mode: AuthMode
    credentials: Mapping[str, CredentialValue] = field(repr=False)

    def __post_init__(self) -> None:
        if self.mode.kind != "native":
            raise DeclarationValidationError("Databricks backend auth requires a native Auth Mode.")
        if self.mode.name not in {"pat", "oauth_m2m"}:
            raise DeclarationValidationError(
                "Databricks backend auth mode must be `pat` or `oauth_m2m`."
            )
        validate_required_credentials(self.mode, self.credentials)
        object.__setattr__(self, "credentials", MappingProxyType(dict(self.credentials)))

    @classmethod
    def from_namespace(
        cls,
        *,
        auth_mode: str,
        credential_namespace: str,
    ) -> DatabricksBackendAuth:
        mode = databricks_auth_mode(auth_mode)
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


def databricks_auth_mode(name: str) -> AuthMode:
    for mode in DATABRICKS_AUTH_MODES:
        if mode.name == name:
            return mode
    available = ", ".join(mode.name for mode in DATABRICKS_AUTH_MODES)
    raise DeclarationValidationError(
        f"Unknown Databricks backend auth_mode `{name}`. Available auth modes: {available}."
    )


def databricks_auth_connect_kwargs(
    auth: DatabricksBackendAuth,
    *,
    server_hostname: str,
    resolver: SecretResolver,
) -> dict[str, object]:
    values = _resolve_databricks_credentials(auth=auth, resolver=resolver)
    if auth.mode.name == "pat":
        return {"access_token": values["token"]}
    if auth.mode.name == "oauth_m2m":
        client_id = values["client_id"]
        client_secret = values["client_secret"]

        def credential_provider() -> object:
            try:
                sdk_core = importlib.import_module("databricks.sdk.core")
            except ImportError as exc:
                from retl.auth import AuthResolutionError

                raise AuthResolutionError(
                    "Databricks OAuth M2M auth requires the optional `databricks` dependency."
                ) from exc

            config = sdk_core.Config(
                host=f"https://{server_hostname}",
                client_id=client_id,
                client_secret=client_secret,
            )
            return sdk_core.oauth_service_principal(config)

        return {"credentials_provider": credential_provider}
    raise DeclarationValidationError(
        f"Unsupported Databricks backend auth mode `{auth.mode.name}`."
    )


def _resolve_databricks_credentials(
    *,
    auth: DatabricksBackendAuth,
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
                f"Databricks backend auth credential `{field_name}` must be a SecretRef "
                "or SecretLiteral."
            )
        if not secret:
            raise DeclarationValidationError(
                f"Databricks backend auth mode `{auth.mode.name}` has empty credential "
                f"field `{field_name}`."
            )
        resolved[field_name] = secret
    return MappingProxyType(resolved)


def _validate_namespace(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            "Databricks backend credential_namespace must be a non-empty dotted namespace."
        )
    namespace = value.strip()
    for segment in namespace.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                "Databricks backend credential_namespace must contain only dotted "
                "identifier segments."
            )
    return namespace


__all__ = [
    "DATABRICKS_AUTH_MODES",
    "DATABRICKS_OAUTH_M2M_AUTH",
    "DATABRICKS_PAT_AUTH",
    "DatabricksBackendAuth",
    "databricks_auth_connect_kwargs",
    "databricks_auth_mode",
]
