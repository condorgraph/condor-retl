from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar

from retl.backends.snowflake.auth import SnowflakeBackendAuth, snowflake_auth_connect_kwargs
from retl.backends.snowflake.dialect import SNOWFLAKE_DIALECT
from retl.backends.snowflake.schema import initialize_snowflake_runtime_schema
from retl.config import configured_secret_resolver
from retl.errors import DeclarationValidationError
from retl.runtime.recovery import (
    AttemptRecord,
    CommitDecisionRecord,
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
class SnowflakeRuntimeStore(SqlRuntimeStore):
    """Snowflake runtime-store scaffold for placement contracts."""

    runtime_store_not_initialized_message: ClassVar[str] = (
        "Snowflake runtime store is not initialized."
    )
    account: str | None = None
    warehouse: str | None = None
    database: str | None = None
    schema: str | None = None
    auth: SnowflakeBackendAuth | None = field(default=None, repr=False, compare=False)
    backend: object | None = field(default=None, repr=False, compare=False)
    connection: Any | None = field(default=None, repr=False, compare=False)
    connector: Any | None = field(default=None, repr=False, compare=False)
    session_parameters: Mapping[str, object] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
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
            from retl.backends.snowflake.backend import SnowflakeSqlBackend

            if not isinstance(self.backend, SnowflakeSqlBackend):
                raise DeclarationValidationError(
                    "Snowflake runtime store `backend` must be a SnowflakeSqlBackend."
                )
            self.account = self.backend.account
            self.warehouse = self.backend.warehouse
            self.database = self.backend.runtime_database
            self.schema = self.backend.runtime_schema
            self.auth = self.backend.auth
            self._sql_backend = self.backend

        self.account = _validate_required_string(self.account, "account")
        self.warehouse = _validate_runtime_identifier(self.warehouse, "warehouse")
        self.database = _validate_runtime_identifier(self.database, "database")
        self.schema = _validate_runtime_identifier(self.schema, "schema")
        if not isinstance(self.auth, SnowflakeBackendAuth):
            raise DeclarationValidationError(
                "Snowflake runtime store `auth` must be a SnowflakeBackendAuth."
            )
        self._initialize_runtime_store()

    @property
    def name(self) -> str:
        return "snowflake"

    @property
    def runtime_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="snowflake",
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
            self._next_attempt_number = initialize_snowflake_runtime_schema(
                self._runtime_context.connection,
                runtime_space=self._runtime_context.runtime_space,
                dialect=self._runtime_context.dialect,
            )
            return

        self._connection = self._open_connection()
        runtime_space = self.runtime_space
        collect_placement = None
        if self._sql_backend is not None:
            from retl.backends.snowflake.backend import SnowflakeSqlBackend

            if not isinstance(self._sql_backend, SnowflakeSqlBackend):
                raise RuntimeStoreError(
                    "Snowflake executable collect requires a SnowflakeSqlBackend-owned "
                    "runtime store."
                )
            runtime_space = self._sql_backend.runtime_space
            collect_placement = self._sql_backend.placement

        self._runtime_context = SqlRuntimeContext(
            connection=self._connection,
            dialect=SNOWFLAKE_DIALECT,
            runtime_space=runtime_space,
            collect_placement=collect_placement,
        )
        self._next_attempt_number = initialize_snowflake_runtime_schema(
            self._runtime_context.connection,
            runtime_space=runtime_space,
            dialect=self._runtime_context.dialect,
        )

    def _open_connection(self) -> Any:
        from retl.backends.snowflake.connection import SnowflakeConnection
        from retl.sql.contracts import SqlConnection

        if self.connection is not None:
            if isinstance(self.connection, SqlConnection):
                return self.connection
            return SnowflakeConnection(connection=self.connection, autocommit=self.autocommit)

        auth = self.auth
        if not isinstance(auth, SnowflakeBackendAuth):
            raise DeclarationValidationError(
                "Snowflake runtime store `auth` must be a SnowflakeBackendAuth."
            )

        return SnowflakeConnection(
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            autocommit=self.autocommit,
            session_parameters=self.session_parameters,
            connect_kwargs=snowflake_auth_connect_kwargs(
                auth,
                resolver=configured_secret_resolver(),
            ),
            connector=self.connector,
        )


def snowflake(
    *,
    account: str,
    warehouse: str,
    database: str,
    schema: str,
    auth: SnowflakeBackendAuth,
    connection: Any | None = None,
    connector: Any | None = None,
    session_parameters: Mapping[str, object] | None = None,
    autocommit: bool | None = True,
) -> SnowflakeRuntimeStore:
    return SnowflakeRuntimeStore(
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        auth=auth,
        connection=connection,
        connector=connector,
        session_parameters=session_parameters,
        autocommit=autocommit,
    )


def _validate_required_string(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"Snowflake runtime store `{field_name}` is required.")
    return value.strip()


def _validate_runtime_identifier(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    validation_helpers.validate_identifier(value, field_name)
    return value


__all__ = ["SnowflakeRuntimeStore", "snowflake"]
