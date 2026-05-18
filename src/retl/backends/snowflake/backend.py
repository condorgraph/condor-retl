from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from retl.backends.snowflake.auth import SnowflakeBackendAuth
from retl.backends.snowflake.source import SnowflakeSourceAdapter, SnowflakeSourceBackend
from retl.config import ConfigResolutionError, ConfigResolver, configured_config_resolver
from retl.errors import DeclarationValidationError
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace

if TYPE_CHECKING:
    from retl.backends.snowflake.store import SnowflakeRuntimeStore

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_NAMESPACE_SEGMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_DEFAULT_SOURCE_SCHEMA = "PUBLIC"
_DEFAULT_RUNTIME_SCHEMA = "RETL"


@dataclass(frozen=True)
class SnowflakeSqlBackend:
    account: str
    warehouse: str
    source_database: str
    source_schema: str
    runtime_database: str
    runtime_schema: str
    auth: SnowflakeBackendAuth = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        account = _validate_required_string(self.account, "account")
        warehouse = _validate_identifier(self.warehouse, "warehouse")
        source_database = _validate_identifier(self.source_database, "source_database")
        source_schema = _validate_identifier(self.source_schema, "source_schema")
        runtime_database = _validate_identifier(self.runtime_database, "runtime_database")
        runtime_schema = _validate_identifier(self.runtime_schema, "runtime_schema")
        if not isinstance(self.auth, SnowflakeBackendAuth):
            raise DeclarationValidationError(
                "Snowflake SQL backend `auth` must be a SnowflakeBackendAuth."
            )

        if (
            source_database.casefold(),
            source_schema.casefold(),
        ) == (
            runtime_database.casefold(),
            runtime_schema.casefold(),
        ):
            raise DeclarationValidationError(
                "Snowflake SQL backend source and runtime relation spaces must be distinct."
            )

        object.__setattr__(self, "account", account)
        object.__setattr__(self, "warehouse", warehouse)
        object.__setattr__(self, "source_database", source_database)
        object.__setattr__(self, "source_schema", source_schema)
        object.__setattr__(self, "runtime_database", runtime_database)
        object.__setattr__(self, "runtime_schema", runtime_schema)

    @classmethod
    def from_config(
        cls,
        *,
        namespace: str = "backends.snowflake",
        auth_mode: str = "password",
        credential_namespace: str | None = None,
        config_resolver: ConfigResolver | None = None,
    ) -> SnowflakeSqlBackend:
        namespace = _validate_namespace(namespace, field_name="namespace")
        credential_namespace = _validate_namespace(
            credential_namespace or f"{namespace}.{auth_mode}",
            field_name="credential_namespace",
        )
        resolver = config_resolver or configured_config_resolver()
        return cls(
            account=_required_config(resolver, f"{namespace}.account"),
            warehouse=_required_config(resolver, f"{namespace}.warehouse"),
            source_database=_required_config(resolver, f"{namespace}.source_database"),
            source_schema=_optional_config(
                resolver,
                f"{namespace}.source_schema",
                default=_DEFAULT_SOURCE_SCHEMA,
            ),
            runtime_database=_required_config(resolver, f"{namespace}.runtime_database"),
            runtime_schema=_optional_config(
                resolver,
                f"{namespace}.runtime_schema",
                default=_DEFAULT_RUNTIME_SCHEMA,
            ),
            auth=SnowflakeBackendAuth.from_namespace(
                auth_mode=auth_mode,
                credential_namespace=credential_namespace,
            ),
        )

    @property
    def name(self) -> str:
        return "snowflake"

    @property
    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="snowflake",
            database=self.source_database,
            schema=self.source_schema,
            access="read_only",
        )

    @property
    def runtime_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="snowflake",
            database=self.runtime_database,
            schema=self.runtime_schema,
            access="read_write",
        )

    @property
    def placement(self) -> SqlCollectPlacement:
        return SqlCollectPlacement(source=self.source_space, runtime=self.runtime_space)

    def source_backend(self) -> SnowflakeSourceBackend:
        return SnowflakeSourceBackend(
            account=self.account,
            warehouse=self.warehouse,
            database=self.source_database,
            schema=self.source_schema,
            auth=self.auth,
        )

    def source_adapter(self) -> SnowflakeSourceAdapter:
        return self.source_backend().adapter()

    def runtime_store(
        self,
        *,
        connection: Any | None = None,
        connector: Any | None = None,
        session_parameters: dict[str, object] | None = None,
        autocommit: bool | None = True,
    ) -> SnowflakeRuntimeStore:
        from retl.backends.snowflake.store import SnowflakeRuntimeStore

        return SnowflakeRuntimeStore(
            backend=self,
            connection=connection,
            connector=connector,
            session_parameters=session_parameters,
            autocommit=autocommit,
        )


def _validate_required_string(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"Snowflake SQL backend `{field_name}` must be non-empty.")
    return value.strip()


def _validate_identifier(value: str, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"Snowflake SQL backend `{field_name}` must be a simple SQL identifier."
        )
    return value


def _required_config(resolver: ConfigResolver, name: str) -> str:
    value = resolver.resolve(name)
    if value is None:
        raise ConfigResolutionError(
            f"Missing public config `{name}` for Snowflake backend construction."
        )
    return value


def _optional_config(resolver: ConfigResolver, name: str, *, default: str) -> str:
    value = resolver.resolve(name)
    if value is None or not value.strip():
        return default
    return value


def _validate_namespace(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"Snowflake SQL backend `{field_name}` must be a non-empty dotted namespace."
        )
    namespace = value.strip()
    for segment in namespace.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                f"Snowflake SQL backend `{field_name}` must contain only dotted "
                "identifier segments."
            )
    return namespace


__all__ = ["SnowflakeSqlBackend"]
