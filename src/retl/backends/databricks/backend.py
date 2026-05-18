from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from retl.backends.databricks.auth import DatabricksBackendAuth
from retl.backends.databricks.source import DatabricksSourceAdapter, DatabricksSourceBackend
from retl.config import ConfigResolutionError, ConfigResolver, configured_config_resolver
from retl.errors import DeclarationValidationError
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace

if TYPE_CHECKING:
    from retl.backends.databricks.store import DatabricksRuntimeStore

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_NAMESPACE_SEGMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_DEFAULT_AUTH_MODE = "pat"


@dataclass(frozen=True)
class DatabricksSqlBackend:
    server_hostname: str
    http_path: str
    source_catalog: str
    source_schema: str
    runtime_catalog: str
    runtime_schema: str
    auth: DatabricksBackendAuth = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        server_hostname = _validate_required_string(self.server_hostname, "server_hostname")
        http_path = _validate_required_string(self.http_path, "http_path")
        source_catalog = _validate_catalog(self.source_catalog, "source_catalog")
        source_schema = _validate_identifier(self.source_schema, "source_schema")
        runtime_catalog = _validate_catalog(self.runtime_catalog, "runtime_catalog")
        runtime_schema = _validate_identifier(self.runtime_schema, "runtime_schema")
        if not isinstance(self.auth, DatabricksBackendAuth):
            raise DeclarationValidationError(
                "Databricks SQL backend `auth` must be a DatabricksBackendAuth."
            )
        if (
            source_catalog.casefold(),
            source_schema.casefold(),
        ) == (
            runtime_catalog.casefold(),
            runtime_schema.casefold(),
        ):
            raise DeclarationValidationError(
                "Databricks SQL backend source and runtime relation spaces must be distinct."
            )
        object.__setattr__(self, "server_hostname", server_hostname)
        object.__setattr__(self, "http_path", http_path)
        object.__setattr__(self, "source_catalog", source_catalog)
        object.__setattr__(self, "source_schema", source_schema)
        object.__setattr__(self, "runtime_catalog", runtime_catalog)
        object.__setattr__(self, "runtime_schema", runtime_schema)

    @classmethod
    def from_config(
        cls,
        *,
        namespace: str = "backends.databricks",
        auth_mode: str = _DEFAULT_AUTH_MODE,
        credential_namespace: str | None = None,
        config_resolver: ConfigResolver | None = None,
    ) -> DatabricksSqlBackend:
        namespace = _validate_namespace(namespace, field_name="namespace")
        credential_namespace = _validate_namespace(
            credential_namespace or f"{namespace}.{auth_mode}",
            field_name="credential_namespace",
        )
        resolver = config_resolver or configured_config_resolver()
        return cls(
            server_hostname=_required_config(resolver, f"{namespace}.server_hostname"),
            http_path=_required_config(resolver, f"{namespace}.http_path"),
            source_catalog=_required_config(resolver, f"{namespace}.source_catalog"),
            source_schema=_required_config(resolver, f"{namespace}.source_schema"),
            runtime_catalog=_required_config(resolver, f"{namespace}.runtime_catalog"),
            runtime_schema=_required_config(resolver, f"{namespace}.runtime_schema"),
            auth=DatabricksBackendAuth.from_namespace(
                auth_mode=auth_mode,
                credential_namespace=credential_namespace,
            ),
        )

    @property
    def name(self) -> str:
        return "databricks"

    @property
    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="databricks",
            database=self.source_catalog,
            schema=self.source_schema,
            access="read_only",
        )

    @property
    def runtime_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="databricks",
            database=self.runtime_catalog,
            schema=self.runtime_schema,
            access="read_write",
        )

    @property
    def placement(self) -> SqlCollectPlacement:
        return SqlCollectPlacement(source=self.source_space, runtime=self.runtime_space)

    def source_backend(self) -> DatabricksSourceBackend:
        return DatabricksSourceBackend(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            catalog=self.source_catalog,
            schema=self.source_schema,
            auth=self.auth,
        )

    def source_adapter(self) -> DatabricksSourceAdapter:
        return self.source_backend().adapter()

    def runtime_store(
        self,
        *,
        connection: Any | None = None,
        connector: Any | None = None,
        session_configuration: dict[str, object] | None = None,
    ) -> DatabricksRuntimeStore:
        from retl.backends.databricks.store import DatabricksRuntimeStore

        return DatabricksRuntimeStore(
            backend=self,
            connection=connection,
            connector=connector,
            session_configuration=session_configuration,
        )


def _validate_required_string(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"Databricks SQL backend `{field_name}` must be non-empty."
        )
    return value.strip()


def _validate_identifier(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"Databricks SQL backend `{field_name}` must be a simple SQL identifier."
        )
    return value


def _validate_catalog(value: str | None, field_name: str) -> str:
    value = _validate_identifier(value, field_name)
    if value.casefold() == "hive_metastore":
        raise DeclarationValidationError(
            "Databricks SQL backend supports only Unity Catalog managed Delta tables; "
            "`hive_metastore` is not supported."
        )
    return value


def _required_config(resolver: ConfigResolver, name: str) -> str:
    value = resolver.resolve(name)
    if value is None:
        raise ConfigResolutionError(
            f"Missing public config `{name}` for Databricks backend construction."
        )
    return value


def _validate_namespace(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"Databricks SQL backend `{field_name}` must be a non-empty dotted namespace."
        )
    namespace = value.strip()
    for segment in namespace.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                f"Databricks SQL backend `{field_name}` must contain only dotted "
                "identifier segments."
            )
    return namespace


__all__ = ["DatabricksSqlBackend"]
