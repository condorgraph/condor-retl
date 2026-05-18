from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar

from retl.backends.databricks.auth import DatabricksBackendAuth, databricks_auth_connect_kwargs
from retl.backends.databricks.dialect import DATABRICKS_DIALECT
from retl.backends.databricks.schema import initialize_databricks_runtime_schema
from retl.config import configured_secret_resolver
from retl.errors import DeclarationValidationError
from retl.runtime.recovery import (
    AttemptRecord,
    CommitDecisionRecord,
    ReceiptRecord,
    RemoteHandleRecord,
)
from retl.stores.contracts import (
    DestinationBatchRecord,
    SqlRelationSpace,
)
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.errors import RuntimeStoreError
from retl.stores.sql_runtime.store import SqlRuntimeStore


@dataclass
class DatabricksRuntimeStore(SqlRuntimeStore):
    """Databricks runtime-store adapter for one SQL warehouse session."""

    runtime_store_not_initialized_message: ClassVar[str] = (
        "Databricks runtime store is not initialized."
    )
    server_hostname: str | None = None
    http_path: str | None = None
    catalog: str | None = None
    schema: str | None = None
    auth: DatabricksBackendAuth | None = field(default=None, repr=False, compare=False)
    backend: object | None = field(default=None, repr=False, compare=False)
    connection: Any | None = field(default=None, repr=False, compare=False)
    connector: Any | None = field(default=None, repr=False, compare=False)
    session_configuration: Mapping[str, object] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    attempts: list[AttemptRecord] = field(default_factory=list, repr=False)
    receipts: list[ReceiptRecord] = field(default_factory=list, repr=False)
    remote_handles: list[RemoteHandleRecord] = field(default_factory=list, repr=False)
    commit_decisions: list[CommitDecisionRecord] = field(default_factory=list, repr=False)
    sync_reports: list[object] = field(default_factory=list, repr=False)
    destination_batches: list[DestinationBatchRecord] = field(default_factory=list, repr=False)
    _next_attempt_number: int = field(default=1, init=False, repr=False)
    _connection: Any = field(default=None, init=False, repr=False)
    _runtime_context: SqlRuntimeContext | None = field(default=None, init=False, repr=False)
    _sql_backend: object | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.backend is not None:
            from retl.backends.databricks.backend import DatabricksSqlBackend

            if not isinstance(self.backend, DatabricksSqlBackend):
                raise DeclarationValidationError(
                    "Databricks runtime store `backend` must be a DatabricksSqlBackend."
                )
            self.server_hostname = self.backend.server_hostname
            self.http_path = self.backend.http_path
            self.catalog = self.backend.runtime_catalog
            self.schema = self.backend.runtime_schema
            self.auth = self.backend.auth
            self._sql_backend = self.backend

        self.server_hostname = _validate_required_string(self.server_hostname, "server_hostname")
        self.http_path = _validate_required_string(self.http_path, "http_path")
        self.catalog = _validate_runtime_catalog(self.catalog, "catalog")
        self.schema = _validate_runtime_identifier(self.schema, "schema")
        if not isinstance(self.auth, DatabricksBackendAuth):
            raise DeclarationValidationError(
                "Databricks runtime store `auth` must be a DatabricksBackendAuth."
            )
        self._initialize_runtime_store()

    @property
    def name(self) -> str:
        return "databricks"

    @property
    def runtime_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="databricks",
            database=self.catalog or "",
            schema=self.schema or "",
            access="read_write",
        )

    def initialize(self) -> None:
        self._initialize_runtime_store()

    def close(self) -> None:
        if self._connection is not None and hasattr(self._connection, "close"):
            self._connection.close()
        self._connection = None
        self._runtime_context = None

    def _initialize_runtime_store(self) -> None:
        if self._runtime_context is not None:
            self._next_attempt_number = initialize_databricks_runtime_schema(
                self._runtime_context.connection,
                runtime_space=self._runtime_context.runtime_space,
                dialect=self._runtime_context.dialect,
            )
            return

        self._connection = self._open_connection()
        runtime_space = self.runtime_space
        collect_placement = None
        if self._sql_backend is not None:
            from retl.backends.databricks.backend import DatabricksSqlBackend

            if not isinstance(self._sql_backend, DatabricksSqlBackend):
                raise RuntimeStoreError(
                    "Databricks executable collect requires a DatabricksSqlBackend-owned "
                    "runtime store."
                )
            runtime_space = self._sql_backend.runtime_space
            collect_placement = self._sql_backend.placement

        self._runtime_context = SqlRuntimeContext(
            connection=self._connection,
            dialect=DATABRICKS_DIALECT,
            runtime_space=runtime_space,
            collect_placement=collect_placement,
        )
        self._next_attempt_number = initialize_databricks_runtime_schema(
            self._runtime_context.connection,
            runtime_space=runtime_space,
            dialect=self._runtime_context.dialect,
        )

    def _open_connection(self) -> Any:
        from retl.backends.databricks.connection import DatabricksConnection
        from retl.sql.contracts import SqlConnection

        if self.connection is not None:
            if isinstance(self.connection, SqlConnection):
                return self.connection
            return DatabricksConnection(connection=self.connection)

        auth = self.auth
        if not isinstance(auth, DatabricksBackendAuth):
            raise DeclarationValidationError(
                "Databricks runtime store `auth` must be a DatabricksBackendAuth."
            )
        server_hostname = self.server_hostname
        if not isinstance(server_hostname, str):
            raise DeclarationValidationError(
                "Databricks runtime store `server_hostname` is required."
            )

        return DatabricksConnection(
            server_hostname=server_hostname,
            http_path=self.http_path,
            catalog=self.catalog,
            schema=self.schema,
            session_configuration=self.session_configuration,
            connect_kwargs=databricks_auth_connect_kwargs(
                auth,
                server_hostname=server_hostname,
                resolver=configured_secret_resolver(),
            ),
            connector=self.connector,
        )


def databricks(
    *,
    server_hostname: str,
    http_path: str,
    catalog: str,
    schema: str,
    auth: DatabricksBackendAuth,
    connection: Any | None = None,
    connector: Any | None = None,
    session_configuration: Mapping[str, object] | None = None,
) -> DatabricksRuntimeStore:
    return DatabricksRuntimeStore(
        server_hostname=server_hostname,
        http_path=http_path,
        catalog=catalog,
        schema=schema,
        auth=auth,
        connection=connection,
        connector=connector,
        session_configuration=session_configuration,
    )


def _validate_required_string(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"Databricks runtime store `{field_name}` is required.")
    return value.strip()


def _validate_runtime_identifier(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    validation_helpers.validate_identifier(value, field_name)
    return value


def _validate_runtime_catalog(value: str | None, field_name: str) -> str:
    value = _validate_runtime_identifier(value, field_name)
    if value.casefold() == "hive_metastore":
        raise DeclarationValidationError(
            "Databricks runtime store supports only Unity Catalog runtime spaces."
        )
    return value


__all__ = ["DatabricksRuntimeStore", "databricks"]
