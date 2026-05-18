from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar

from retl.backends.bigquery.auth import BigQueryBackendAuth, bigquery_client_kwargs
from retl.backends.bigquery.dialect import BIGQUERY_DIALECT
from retl.backends.bigquery.schema import initialize_bigquery_runtime_schema
from retl.backends.bigquery.write_api import BigQueryRuntimeAppendWriter
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
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.errors import RuntimeStoreError
from retl.stores.sql_runtime.store import SqlRuntimeStore


@dataclass
class BigQueryRuntimeStore(SqlRuntimeStore):
    """BigQuery runtime-store wiring for shared SQL runtime semantics."""

    runtime_store_not_initialized_message: ClassVar[str] = (
        "BigQuery runtime store is not initialized."
    )
    project: str | None = None
    location: str | None = None
    runtime_project: str | None = None
    runtime_dataset: str | None = None
    auth: BigQueryBackendAuth | None = field(default=None, repr=False, compare=False)
    backend: object | None = field(default=None, repr=False, compare=False)
    client: Any | None = field(default=None, repr=False, compare=False)
    read_client: Any | None = field(default=None, repr=False, compare=False)
    bigquery_module: Any | None = field(default=None, repr=False, compare=False)
    bigquery_storage_module: Any | None = field(default=None, repr=False, compare=False)
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
            from retl.backends.bigquery.backend import BigQuerySqlBackend

            if not isinstance(self.backend, BigQuerySqlBackend):
                raise DeclarationValidationError(
                    "BigQuery runtime store `backend` must be a BigQuerySqlBackend."
                )
            self.project = self.backend.project
            self.location = self.backend.location
            self.runtime_project = self.backend.runtime_project
            self.runtime_dataset = self.backend.runtime_dataset
            self.auth = self.backend.auth
            self._sql_backend = self.backend

        self.project = _validate_required_string(self.project, "project")
        self.runtime_project = _validate_runtime_project(self.runtime_project, "runtime_project")
        self.runtime_dataset = _validate_runtime_dataset(self.runtime_dataset, "runtime_dataset")
        if not isinstance(self.auth, BigQueryBackendAuth):
            raise DeclarationValidationError(
                "BigQuery runtime store `auth` must be a BigQueryBackendAuth."
            )
        self._initialize_runtime_store()

    @property
    def name(self) -> str:
        return "bigquery"

    @property
    def runtime_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="bigquery",
            database=self.runtime_project or "",
            schema=self.runtime_dataset or "",
            access="read_write",
        )

    def initialize(self) -> None:
        self._initialize_runtime_store()

    def close(self) -> None:
        if self._runtime_context is not None and self._runtime_context.append_writer is not None:
            close = getattr(self._runtime_context.append_writer, "close", None)
            if close is not None:
                close()
        if self._connection is not None and hasattr(self._connection, "close"):
            self._connection.close()
        self._connection = None
        self._runtime_context = None

    def _initialize_runtime_store(self) -> None:
        if self._runtime_context is not None:
            self._next_attempt_number = initialize_bigquery_runtime_schema(
                self._runtime_context.connection,
                runtime_space=self._runtime_context.runtime_space,
                dialect=self._runtime_context.dialect,
            )
            return

        self._connection = self._open_connection()
        runtime_space = self.runtime_space
        collect_placement = None
        if self._sql_backend is not None:
            from retl.backends.bigquery.backend import BigQuerySqlBackend

            if not isinstance(self._sql_backend, BigQuerySqlBackend):
                raise RuntimeStoreError(
                    "BigQuery executable collect requires a BigQuerySqlBackend-owned runtime store."
                )
            runtime_space = self._sql_backend.runtime_space
            collect_placement = self._sql_backend.placement

        self._runtime_context = SqlRuntimeContext(
            connection=self._connection,
            dialect=BIGQUERY_DIALECT,
            runtime_space=runtime_space,
            collect_placement=collect_placement,
            append_writer=BigQueryRuntimeAppendWriter(
                project=runtime_space.database,
                dataset=runtime_space.schema,
                client_kwargs=self._client_kwargs(),
                bigquery_storage_module=self.bigquery_storage_module,
            ),
        )
        self._next_attempt_number = initialize_bigquery_runtime_schema(
            self._runtime_context.connection,
            runtime_space=runtime_space,
            dialect=self._runtime_context.dialect,
        )

    def _open_connection(self) -> Any:
        from retl.backends.bigquery.connection import BigQueryConnection
        from retl.sql.contracts import SqlConnection

        if self.client is not None:
            if isinstance(self.client, SqlConnection):
                return self.client
            return BigQueryConnection(
                project=self.project or "",
                location=self.location,
                client=self.client,
                read_client=self.read_client,
                bigquery_module=self.bigquery_module,
                bigquery_storage_module=self.bigquery_storage_module,
                use_session=True,
            )

        auth = self.auth
        if not isinstance(auth, BigQueryBackendAuth):
            raise DeclarationValidationError(
                "BigQuery runtime store `auth` must be a BigQueryBackendAuth."
            )
        return BigQueryConnection(
            project=self.project or "",
            location=self.location,
            read_client=self.read_client,
            client_kwargs=self._client_kwargs(),
            bigquery_module=self.bigquery_module,
            bigquery_storage_module=self.bigquery_storage_module,
            use_session=True,
        )

    def _client_kwargs(self) -> dict[str, object]:
        auth = self.auth
        if not isinstance(auth, BigQueryBackendAuth):
            raise DeclarationValidationError(
                "BigQuery runtime store `auth` must be a BigQueryBackendAuth."
            )
        return bigquery_client_kwargs(
            auth,
            resolver=configured_secret_resolver(),
        )


def bigquery(
    *,
    project: str,
    runtime_project: str,
    runtime_dataset: str,
    auth: BigQueryBackendAuth | None = None,
    location: str | None = None,
    client: Any | None = None,
    read_client: Any | None = None,
) -> BigQueryRuntimeStore:
    return BigQueryRuntimeStore(
        project=project,
        location=location,
        runtime_project=runtime_project,
        runtime_dataset=runtime_dataset,
        auth=auth or BigQueryBackendAuth.application_default(),
        client=client,
        read_client=read_client,
    )


def _validate_required_string(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"BigQuery runtime store `{field_name}` is required.")
    return value.strip()


def _validate_runtime_project(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    from retl.backends.bigquery.backend import _validate_project_id

    _validate_project_id(value, field_name)
    return value


def _validate_runtime_dataset(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    from retl.sql.contracts import validate_sql_identifier

    try:
        validate_sql_identifier(value)
    except ValueError as exc:
        raise DeclarationValidationError(
            f"BigQuery runtime store `{field_name}` must be a simple SQL identifier."
        ) from exc
    return value


__all__ = ["BigQueryRuntimeStore", "bigquery"]
