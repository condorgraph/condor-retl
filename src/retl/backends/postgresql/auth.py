from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from retl.auth import AuthMode, SecretResolver, native, validate_required_credentials
from retl.declarations import CredentialValue, SecretLiteral, SecretRef
from retl.errors import DeclarationValidationError

POSTGRESQL_PASSWORD_AUTH = native(
    "password",
    required_fields=("user", "password"),
)
POSTGRESQL_AUTH_MODES = (POSTGRESQL_PASSWORD_AUTH,)
_NAMESPACE_SEGMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class PostgreSqlBackendAuth:
    mode: AuthMode
    credentials: Mapping[str, CredentialValue] = field(repr=False)

    def __post_init__(self) -> None:
        if self.mode.kind != "native":
            raise DeclarationValidationError("PostgreSQL backend auth requires a native Auth Mode.")
        if self.mode.name != "password":
            raise DeclarationValidationError("PostgreSQL backend auth mode must be `password`.")
        validate_required_credentials(self.mode, self.credentials)
        object.__setattr__(self, "credentials", MappingProxyType(dict(self.credentials)))

    @classmethod
    def from_namespace(cls, *, auth_mode: str, credential_namespace: str) -> PostgreSqlBackendAuth:
        mode = postgresql_auth_mode(auth_mode)
        namespace = _validate_namespace(credential_namespace)
        return cls(
            mode=mode,
            credentials={
                field_name: SecretRef(f"{namespace}.{field_name}")
                for field_name in mode.field_names
            },
        )

    @property
    def evidence(self) -> Mapping[str, object]:
        return {
            "mode": self.mode.name,
            "required_fields": {
                field_name: field_name in self.credentials
                for field_name in self.mode.required_fields
            },
            "optional_fields": {},
            "resolved": False,
        }


def postgresql_auth_mode(name: str) -> AuthMode:
    for mode in POSTGRESQL_AUTH_MODES:
        if mode.name == name:
            return mode
    available = ", ".join(mode.name for mode in POSTGRESQL_AUTH_MODES)
    raise DeclarationValidationError(
        f"Unknown PostgreSQL backend auth_mode `{name}`. Available auth modes: {available}."
    )


def postgresql_auth_connect_kwargs(
    auth: PostgreSqlBackendAuth,
    *,
    resolver: SecretResolver,
) -> dict[str, object]:
    values: dict[str, str] = {}
    for field_name in auth.mode.required_fields:
        value = auth.credentials[field_name]
        if isinstance(value, SecretRef):
            resolved = resolver.resolve(value)
        elif isinstance(value, SecretLiteral):
            resolved = value.value
        else:
            raise DeclarationValidationError(
                f"PostgreSQL backend auth credential `{field_name}` must be a SecretRef "
                "or SecretLiteral."
            )
        if not resolved:
            raise DeclarationValidationError(
                f"PostgreSQL backend auth mode `{auth.mode.name}` has empty credential "
                f"field `{field_name}`."
            )
        values[field_name] = resolved
    return {"user": values["user"], "password": values["password"]}


def _validate_namespace(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            "PostgreSQL backend credential_namespace must be a non-empty dotted namespace."
        )
    namespace = value.strip()
    for segment in namespace.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                "PostgreSQL backend credential_namespace must contain only dotted "
                "identifier segments."
            )
    return namespace


__all__ = [
    "POSTGRESQL_AUTH_MODES",
    "POSTGRESQL_PASSWORD_AUTH",
    "PostgreSqlBackendAuth",
    "postgresql_auth_connect_kwargs",
    "postgresql_auth_mode",
]
