from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from retl.backends.postgresql.auth import PostgreSqlBackendAuth
from retl.backends.postgresql.source import PostgreSqlSourceAdapter, PostgreSqlSourceBackend
from retl.config import ConfigResolutionError, ConfigResolver, configured_config_resolver
from retl.errors import DeclarationValidationError
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace

if TYPE_CHECKING:
    from retl.backends.postgresql.store import PostgreSqlRuntimeStore

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_NAMESPACE_SEGMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_DEFAULT_PORT = 5432
_DEFAULT_SOURCE_SCHEMA = "public"
_DEFAULT_RUNTIME_SCHEMA = "retl"
_DEFAULT_SSLMODE = "require"


@dataclass(frozen=True)
class PostgreSqlBackend:
    host: str
    database: str
    source_schema: str
    runtime_schema: str
    auth: PostgreSqlBackendAuth = field(repr=False, compare=False)
    port: int = _DEFAULT_PORT
    sslmode: str | None = _DEFAULT_SSLMODE
    connect_timeout: int | None = None

    def __post_init__(self) -> None:
        host = _validate_required_string(self.host, "host")
        database = _validate_identifier(self.database, "database")
        source_schema = _validate_identifier(self.source_schema, "source_schema")
        runtime_schema = _validate_identifier(self.runtime_schema, "runtime_schema")
        port = _validate_port(self.port)
        sslmode = _validate_optional_string(self.sslmode, "sslmode")
        connect_timeout = _validate_optional_positive_int(self.connect_timeout, "connect_timeout")
        if not isinstance(self.auth, PostgreSqlBackendAuth):
            raise DeclarationValidationError(
                "PostgreSQL SQL backend `auth` must be a PostgreSqlBackendAuth."
            )
        if source_schema.casefold() == runtime_schema.casefold():
            raise DeclarationValidationError(
                "PostgreSQL SQL backend source and runtime relation spaces must be distinct."
            )
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "database", database)
        object.__setattr__(self, "source_schema", source_schema)
        object.__setattr__(self, "runtime_schema", runtime_schema)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "sslmode", sslmode)
        object.__setattr__(self, "connect_timeout", connect_timeout)

    @classmethod
    def from_config(
        cls,
        *,
        namespace: str = "backends.postgresql",
        auth_mode: str = "password",
        credential_namespace: str | None = None,
        config_resolver: ConfigResolver | None = None,
    ) -> PostgreSqlBackend:
        namespace = _validate_namespace(namespace, field_name="namespace")
        credential_namespace = _validate_namespace(
            credential_namespace or f"{namespace}.{auth_mode}",
            field_name="credential_namespace",
        )
        resolver = config_resolver or configured_config_resolver()
        port = _optional_int_config(resolver, f"{namespace}.port", default=_DEFAULT_PORT)
        source_schema = _optional_config(
            resolver, f"{namespace}.source_schema", default=_DEFAULT_SOURCE_SCHEMA
        )
        runtime_schema = _optional_config(
            resolver, f"{namespace}.runtime_schema", default=_DEFAULT_RUNTIME_SCHEMA
        )
        assert port is not None
        assert source_schema is not None
        assert runtime_schema is not None
        return cls(
            host=_required_config(resolver, f"{namespace}.host"),
            port=port,
            database=_required_config(resolver, f"{namespace}.database"),
            source_schema=source_schema,
            runtime_schema=runtime_schema,
            sslmode=_optional_config(resolver, f"{namespace}.sslmode", default=_DEFAULT_SSLMODE),
            connect_timeout=_optional_int_config(
                resolver, f"{namespace}.connect_timeout", default=None
            ),
            auth=PostgreSqlBackendAuth.from_namespace(
                auth_mode=auth_mode,
                credential_namespace=credential_namespace,
            ),
        )

    @property
    def name(self) -> str:
        return "postgresql"

    @property
    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="postgresql",
            database=self.database,
            schema=self.source_schema,
            access="read_only",
        )

    @property
    def runtime_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="postgresql",
            database=self.database,
            schema=self.runtime_schema,
            access="read_write",
        )

    @property
    def placement(self) -> SqlCollectPlacement:
        return SqlCollectPlacement(source=self.source_space, runtime=self.runtime_space)

    def source_backend(self) -> PostgreSqlSourceBackend:
        return PostgreSqlSourceBackend(
            host=self.host,
            port=self.port,
            database=self.database,
            schema=self.source_schema,
            auth=self.auth,
            sslmode=self.sslmode,
            connect_timeout=self.connect_timeout,
        )

    def source_adapter(self) -> PostgreSqlSourceAdapter:
        return self.source_backend().adapter()

    def runtime_store(
        self,
        *,
        connection: Any | None = None,
        connector: Any | None = None,
        autocommit: bool | None = True,
    ) -> PostgreSqlRuntimeStore:
        from retl.backends.postgresql.store import PostgreSqlRuntimeStore

        return PostgreSqlRuntimeStore(
            backend=self,
            connection=connection,
            connector=connector,
            autocommit=autocommit,
        )


def _validate_required_string(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"PostgreSQL SQL backend `{field_name}` must be non-empty."
        )
    return value.strip()


def _validate_optional_string(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _validate_required_string(value, field_name)


def _validate_identifier(value: str, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"PostgreSQL SQL backend `{field_name}` must be a simple SQL identifier."
        )
    return value


def _validate_port(value: int) -> int:
    if not isinstance(value, int) or value <= 0 or value > 65535:
        raise DeclarationValidationError("PostgreSQL SQL backend `port` must be 1-65535.")
    return value


def _validate_optional_positive_int(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or value <= 0:
        raise DeclarationValidationError(
            f"PostgreSQL SQL backend `{field_name}` must be a positive integer."
        )
    return value


def _required_config(resolver: ConfigResolver, name: str) -> str:
    value = resolver.resolve(name)
    if value is None:
        raise ConfigResolutionError(
            f"Missing public config `{name}` for PostgreSQL backend construction."
        )
    return value


def _optional_config(resolver: ConfigResolver, name: str, *, default: str | None) -> str | None:
    value = resolver.resolve(name)
    if value is None or not value.strip():
        return default
    return value


def _optional_int_config(resolver: ConfigResolver, name: str, *, default: int | None) -> int | None:
    value = resolver.resolve(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigResolutionError(f"Public config `{name}` must be an integer.") from exc


def _validate_namespace(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"PostgreSQL SQL backend `{field_name}` must be a non-empty dotted namespace."
        )
    namespace = value.strip()
    for segment in namespace.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                f"PostgreSQL SQL backend `{field_name}` must contain only dotted "
                "identifier segments."
            )
    return namespace


__all__ = ["PostgreSqlBackend"]
