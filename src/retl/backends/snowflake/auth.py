from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

from retl.auth import (
    AuthMode,
    AuthResolutionError,
    SecretResolver,
    native,
    validate_required_credentials,
)
from retl.declarations import CredentialValue, SecretLiteral, SecretRef
from retl.errors import DeclarationValidationError

SNOWFLAKE_PASSWORD_AUTH = native(
    "password",
    required_fields=("user", "password"),
    optional_fields=("role",),
)
SNOWFLAKE_KEY_PAIR_AUTH = native(
    "key_pair",
    required_fields=("user",),
    optional_fields=("private_key", "private_key_path", "private_key_passphrase", "role"),
)
SNOWFLAKE_AUTH_MODES = (SNOWFLAKE_PASSWORD_AUTH, SNOWFLAKE_KEY_PAIR_AUTH)
_NAMESPACE_SEGMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class SnowflakeBackendAuth:
    mode: AuthMode
    credentials: Mapping[str, CredentialValue] = field(repr=False)

    def __post_init__(self) -> None:
        if self.mode.kind != "native":
            raise DeclarationValidationError("Snowflake backend auth requires a native Auth Mode.")
        if self.mode.name not in {"password", "key_pair"}:
            raise DeclarationValidationError(
                "Snowflake backend auth mode must be `password` or `key_pair`."
            )
        validate_required_credentials(self.mode, self.credentials)
        object.__setattr__(self, "credentials", MappingProxyType(dict(self.credentials)))

    @classmethod
    def from_namespace(cls, *, auth_mode: str, credential_namespace: str) -> SnowflakeBackendAuth:
        mode = snowflake_auth_mode(auth_mode)
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
            "optional_fields": {
                field_name: field_name in self.credentials
                for field_name in self.mode.optional_fields
            },
            "resolved": False,
        }


def snowflake_auth_mode(name: str) -> AuthMode:
    for mode in SNOWFLAKE_AUTH_MODES:
        if mode.name == name:
            return mode
    available = ", ".join(mode.name for mode in SNOWFLAKE_AUTH_MODES)
    raise DeclarationValidationError(
        f"Unknown Snowflake backend auth_mode `{name}`. Available auth modes: {available}."
    )


def snowflake_auth_connect_kwargs(
    auth: SnowflakeBackendAuth,
    *,
    resolver: SecretResolver,
) -> dict[str, object]:
    values = _resolve_snowflake_credentials(auth=auth, resolver=resolver)
    if auth.mode.name == "password":
        return _without_none(
            {
                "user": values["user"],
                "password": values["password"],
                "role": values.get("role"),
            }
        )
    if auth.mode.name == "key_pair":
        private_key = _private_key_material(values)
        return _without_none(
            {
                "user": values["user"],
                "private_key": _private_key_der(
                    _normalize_private_key_pem(private_key),
                    passphrase=values.get("private_key_passphrase"),
                ),
                "role": values.get("role"),
            }
        )
    raise DeclarationValidationError(f"Unsupported Snowflake backend auth mode `{auth.mode.name}`.")


def _resolve_snowflake_credentials(
    *,
    auth: SnowflakeBackendAuth,
    resolver: SecretResolver,
) -> Mapping[str, str]:
    resolved: dict[str, str] = {}
    for field_name in auth.mode.required_fields:
        resolved[field_name] = _resolve_credential(
            auth=auth,
            field_name=field_name,
            resolver=resolver,
            required=True,
        )
    for field_name in auth.mode.optional_fields:
        if field_name not in auth.credentials:
            continue
        value = _resolve_credential(
            auth=auth,
            field_name=field_name,
            resolver=resolver,
            required=False,
        )
        if value:
            resolved[field_name] = value
    return MappingProxyType(resolved)


def _resolve_credential(
    *,
    auth: SnowflakeBackendAuth,
    field_name: str,
    resolver: SecretResolver,
    required: bool,
) -> str:
    value = auth.credentials[field_name]
    try:
        if isinstance(value, SecretRef):
            resolved = resolver.resolve(value)
        elif isinstance(value, SecretLiteral):
            resolved = value.value
        else:
            raise DeclarationValidationError(
                f"Snowflake backend auth credential `{field_name}` must be a SecretRef "
                "or SecretLiteral."
            )
    except AuthResolutionError:
        if required:
            raise
        return ""
    if not resolved:
        if required:
            raise DeclarationValidationError(
                f"Snowflake backend auth mode `{auth.mode.name}` has empty credential "
                f"field `{field_name}`."
            )
        return ""
    return resolved


def _private_key_der(private_key_pem: str, *, passphrase: str | None) -> bytes:
    try:
        from cryptography.hazmat.primitives import serialization
    except ImportError as exc:
        raise AuthResolutionError(
            "Snowflake key-pair auth requires the optional `cryptography` dependency."
        ) from exc

    try:
        private_key = serialization.load_pem_private_key(
            private_key_pem.encode("utf-8"),
            password=passphrase.encode("utf-8") if passphrase else None,
        )
    except Exception as exc:
        raise AuthResolutionError("Snowflake key-pair private key could not be parsed.") from exc
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _private_key_material(values: Mapping[str, str]) -> str:
    private_key = values.get("private_key")
    private_key_path = values.get("private_key_path")
    if private_key and private_key_path:
        raise DeclarationValidationError(
            "Snowflake key-pair auth must provide exactly one of `private_key` "
            "or `private_key_path`."
        )
    if private_key:
        return private_key
    if private_key_path:
        try:
            return Path(private_key_path).read_text(encoding="utf-8")
        except OSError as exc:
            raise AuthResolutionError(
                "Snowflake key-pair private_key_path could not be read."
            ) from exc
    raise DeclarationValidationError(
        "Snowflake key-pair auth requires `private_key` or `private_key_path`."
    )


def _without_none(values: Mapping[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in values.items() if value is not None}


def _validate_namespace(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            "Snowflake backend credential_namespace must be a non-empty dotted namespace."
        )
    namespace = value.strip()
    for segment in namespace.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                "Snowflake backend credential_namespace must contain only dotted "
                "identifier segments."
            )
    return namespace


def _normalize_private_key_pem(value: str) -> str:
    return value.replace("\\n", "\n")


__all__ = [
    "SNOWFLAKE_AUTH_MODES",
    "SNOWFLAKE_KEY_PAIR_AUTH",
    "SNOWFLAKE_PASSWORD_AUTH",
    "SnowflakeBackendAuth",
    "snowflake_auth_connect_kwargs",
    "snowflake_auth_mode",
]
