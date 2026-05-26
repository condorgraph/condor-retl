from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar

from retl.backends.postgresql.auth import PostgreSqlBackendAuth, postgresql_auth_connect_kwargs
from retl.backends.postgresql.dialect import POSTGRESQL_DIALECT
from retl.backends.postgresql.schema import initialize_postgresql_runtime_schema
from retl.config import configured_secret_resolver
from retl.errors import DeclarationValidationError
from retl.runtime.recovery import (
    AttemptRecord,
    CommitDecisionRecord,
)
from retl.stores.contracts import DestinationBatchRecord, SqlRelationSpace
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.errors import RuntimeStoreError
from retl.stores.sql_runtime.store import SqlRuntimeStore


@dataclass
class PostgreSqlRuntimeStore(SqlRuntimeStore):
    """PostgreSQL runtime-store adapter for shared SQL runtime semantics."""

    runtime_store_not_initialized_message: ClassVar[str] = (
        "PostgreSQL runtime store is not initialized."
    )
    host: str | None = None
    port: int | None = None
    database: str | None = None
    schema: str | None = None
    auth: PostgreSqlBackendAuth | None = field(default=None, repr=False, compare=False)
    sslmode: str | None = None
    connect_timeout: int | None = None
    backend: object | None = field(default=None, repr=False, compare=False)
    connection: Any | None = field(default=None, repr=False, compare=False)
    connector: Any | None = field(default=None, repr=False, compare=False)
    autocommit: bool | None = field(default=True, repr=False, compare=False)
    attempts: list[AttemptRecord] = field(default_factory=list, repr=False)
    commit_decisions: list[CommitDecisionRecord] = field(default_factory=list, repr=False)
    sync_reports: list[object] = field(default_factory=list, repr=False)
    destination_batches: list[DestinationBatchRecord] = field(default_factory=list, repr=False)
    _next_attempt_number: int = field(default=1, init=False, repr=False)
    _connection: Any = field(default=None, init=False, repr=False)
    _runtime_context: SqlRuntimeContext | None = field(default=None, init=False, repr=False)
    _sql_backend: object | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.backend is not None:
            from retl.backends.postgresql.backend import PostgreSqlBackend

            if not isinstance(self.backend, PostgreSqlBackend):
                raise DeclarationValidationError(
                    "PostgreSQL runtime store `backend` must be a PostgreSqlBackend."
                )
            self.host = self.backend.host
            self.port = self.backend.port
            self.database = self.backend.database
            self.schema = self.backend.runtime_schema
            self.auth = self.backend.auth
            self.sslmode = self.backend.sslmode
            self.connect_timeout = self.backend.connect_timeout
            self._sql_backend = self.backend

        self.host = _validate_required_string(self.host, "host")
        self.database = _validate_runtime_identifier(self.database, "database")
        self.schema = _validate_runtime_identifier(self.schema, "schema")
        if self.port is None or self.port <= 0:
            raise DeclarationValidationError("PostgreSQL runtime store `port` is required.")
        if not isinstance(self.auth, PostgreSqlBackendAuth):
            raise DeclarationValidationError(
                "PostgreSQL runtime store `auth` must be a PostgreSqlBackendAuth."
            )
        self._initialize_runtime_store()

    @property
    def name(self) -> str:
        return "postgresql"

    @property
    def runtime_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="postgresql",
            database=self.database or "",
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
            self._next_attempt_number = initialize_postgresql_runtime_schema(
                self._runtime_context.connection,
                runtime_space=self._runtime_context.runtime_space,
                dialect=self._runtime_context.dialect,
            )
            return

        self._connection = self._open_connection()
        runtime_space = self.runtime_space
        collect_placement = None
        if self._sql_backend is not None:
            from retl.backends.postgresql.backend import PostgreSqlBackend

            if not isinstance(self._sql_backend, PostgreSqlBackend):
                raise RuntimeStoreError(
                    "PostgreSQL executable collect requires a PostgreSqlBackend-owned "
                    "runtime store."
                )
            runtime_space = self._sql_backend.runtime_space
            collect_placement = self._sql_backend.placement

        self._runtime_context = SqlRuntimeContext(
            connection=self._connection,
            dialect=POSTGRESQL_DIALECT,
            runtime_space=runtime_space,
            collect_placement=collect_placement,
        )
        self._next_attempt_number = initialize_postgresql_runtime_schema(
            self._runtime_context.connection,
            runtime_space=runtime_space,
            dialect=self._runtime_context.dialect,
        )

    def _open_connection(self) -> Any:
        from retl.backends.postgresql.connection import PostgreSqlConnection
        from retl.sql.contracts import SqlConnection

        if self.connection is not None:
            if isinstance(self.connection, SqlConnection):
                return self.connection
            return PostgreSqlConnection(connection=self.connection, autocommit=self.autocommit)

        auth = self.auth
        if not isinstance(auth, PostgreSqlBackendAuth):
            raise DeclarationValidationError(
                "PostgreSQL runtime store `auth` must be a PostgreSqlBackendAuth."
            )
        return PostgreSqlConnection(
            host=self.host,
            port=self.port,
            dbname=self.database,
            sslmode=self.sslmode,
            connect_timeout=self.connect_timeout,
            autocommit=self.autocommit,
            connect_kwargs=postgresql_auth_connect_kwargs(
                auth,
                resolver=configured_secret_resolver(),
            ),
            connector=self.connector,
        )


def postgresql(
    *,
    host: str,
    port: int,
    database: str,
    schema: str,
    auth: PostgreSqlBackendAuth,
    sslmode: str | None = None,
    connect_timeout: int | None = None,
    connection: Any | None = None,
    connector: Any | None = None,
    autocommit: bool | None = True,
) -> PostgreSqlRuntimeStore:
    return PostgreSqlRuntimeStore(
        host=host,
        port=port,
        database=database,
        schema=schema,
        auth=auth,
        sslmode=sslmode,
        connect_timeout=connect_timeout,
        connection=connection,
        connector=connector,
        autocommit=autocommit,
    )


def _validate_required_string(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"PostgreSQL runtime store `{field_name}` is required.")
    return value.strip()


def _validate_runtime_identifier(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    validation_helpers.validate_identifier(value, field_name)
    return value


__all__ = ["PostgreSqlRuntimeStore", "postgresql"]
