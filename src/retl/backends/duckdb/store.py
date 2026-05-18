from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from retl.backends.duckdb.connection import DuckDBConnection, DuckDBConnectionError
from retl.backends.duckdb.dialect import DUCKDB_DIALECT
from retl.backends.duckdb.schema import initialize_duckdb_runtime_schema
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
from retl.stores.sql_runtime import collect as collect_store
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.errors import RuntimeStoreError
from retl.stores.sql_runtime.store import SqlRuntimeStore


@dataclass
class DuckDBRuntimeStore(SqlRuntimeStore):
    """DuckDB-backed runtime store behind the backend-neutral store contracts."""

    runtime_store_not_initialized_message: ClassVar[str] = (
        "DuckDB runtime store is not initialized."
    )
    database: str | Path = ".retl/state.duckdb"
    schema: str = "retl"
    backend: object | None = None
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
            from retl.backends.duckdb.backend import DuckDBSqlBackend

            if not isinstance(self.backend, DuckDBSqlBackend):
                raise DeclarationValidationError(
                    "DuckDB runtime store `backend` must be a DuckDBSqlBackend."
                )
            self.database = self.backend.database
            self.schema = self.backend.runtime_schema
            self._sql_backend = self.backend
        self.database = str(self.database)
        if not self.database.strip():
            raise DeclarationValidationError("DuckDB runtime store `database` is required.")
        validation_helpers.validate_identifier(self.schema, "schema")
        if self.database != ":memory:":
            Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self._initialize_ordered_work_store()

    def _validate_duckdb_collect_source_space(self, source_space: SqlRelationSpace) -> None:
        collect_store.validate_duckdb_collect_source_space(self._context(), source_space)

    def _initialize_ordered_work_store(self) -> None:
        try:
            self._connection = DuckDBConnection(self.database)
        except DuckDBConnectionError as exc:  # pragma: no cover - exercised only without the extra
            raise RuntimeStoreError(
                "DuckDB runtime storage requires the optional `duckdb` dependency."
            ) from exc

        runtime_space = SqlRelationSpace(
            backend_name="duckdb",
            database=str(self.database),
            schema=self.schema,
            access="read_write",
        )
        collect_placement = None
        if self._sql_backend is not None:
            from retl.backends.duckdb.backend import DuckDBSqlBackend

            if not isinstance(self._sql_backend, DuckDBSqlBackend):
                raise RuntimeStoreError(
                    "DuckDB executable collect requires a DuckDBSqlBackend-owned runtime store."
                )
            runtime_space = self._sql_backend.runtime_space
            collect_placement = self._sql_backend.placement
        self._runtime_context = SqlRuntimeContext(
            connection=self._connection,
            dialect=DUCKDB_DIALECT,
            runtime_space=runtime_space,
            collect_placement=collect_placement,
        )
        self._next_attempt_number = initialize_duckdb_runtime_schema(
            self._runtime_context.connection,
            schema=self.schema,
            dialect=self._runtime_context.dialect,
        )


def duckdb(
    *,
    database: str | Path = ".retl/state.duckdb",
    schema: str = "retl",
) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=database, schema=schema)


DuckDbRuntimeStore = DuckDBRuntimeStore


__all__ = [
    "DuckDBRuntimeStore",
    "DuckDbRuntimeStore",
    "duckdb",
]
