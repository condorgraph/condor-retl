from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from retl.errors import DeclarationValidationError
from retl.sources.contracts import SourceCapabilities
from retl.stores.contracts import (
    EventSourceWindowHandle,
    EventSourceWindowRequest,
    SqlRelationSpace,
    StateSnapshotHandle,
    StateSnapshotRequest,
)


@dataclass(frozen=True)
class DuckDBSourceBackend:
    database: str = ":memory:"
    read_only: bool = False
    default_schema: str | None = None
    config: Mapping[str, object] = field(default_factory=dict, repr=False)
    capabilities: SourceCapabilities = field(
        default_factory=lambda: SourceCapabilities(snapshots=True, checkpointed_windows=True)
    )

    def __post_init__(self) -> None:
        if not self.database.strip():
            raise DeclarationValidationError("DuckDB source `database` must be non-empty.")
        if self.default_schema is not None and not self.default_schema.strip():
            raise DeclarationValidationError("DuckDB source `default_schema` must be non-empty.")

    @property
    def name(self) -> str:
        return "duckdb"

    def identity(self) -> Mapping[str, object]:
        identity: dict[str, object] = {
            "backend": "duckdb",
            "database": self.database,
        }
        if self.default_schema is not None:
            identity["default_schema"] = self.default_schema
        config_keys = tuple(str(key) for key in sorted(self.config))
        if config_keys:
            identity["config_keys"] = ",".join(config_keys)
        return identity

    @property
    def sanitized_identity(self) -> Mapping[str, object]:
        return self.identity()

    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="duckdb",
            database=self.database,
            schema=self.default_schema or "main",
            access="read_only",
        )

    def adapter(self) -> DuckDBSourceAdapter:
        return DuckDBSourceAdapter(backend=self)


@dataclass(frozen=True)
class DuckDBSourceAdapter:
    backend: DuckDBSourceBackend

    def prepare_state_snapshot(self, request: StateSnapshotRequest) -> StateSnapshotHandle:
        if not isinstance(request, StateSnapshotRequest):
            raise DeclarationValidationError(
                "State snapshot preparation requires a StateSnapshotRequest."
            )
        if not request.source_name.strip():
            raise DeclarationValidationError("State snapshot source name is required.")
        if not request.query.strip():
            raise DeclarationValidationError("State snapshot query is required.")
        return StateSnapshotHandle(
            backend="duckdb",
            source_name=request.source_name,
            source_identity=self.backend.identity(),
            query=request.query,
            source_space=self.backend.source_space(),
        )

    def prepare_event_source_window(
        self, request: EventSourceWindowRequest
    ) -> EventSourceWindowHandle:
        if not isinstance(request, EventSourceWindowRequest):
            raise DeclarationValidationError(
                "Event source window preparation requires an EventSourceWindowRequest."
            )
        if not request.source_name.strip():
            raise DeclarationValidationError("Event source window source name is required.")
        if not request.query.strip():
            raise DeclarationValidationError("Event source window query is required.")
        if not request.cursor_column.strip():
            raise DeclarationValidationError("Event source window cursor column is required.")
        if not request.primary_key_column.strip():
            raise DeclarationValidationError("Event source window primary key column is required.")
        return EventSourceWindowHandle(
            backend="duckdb",
            source_name=request.source_name,
            source_identity=self.backend.identity(),
            query=request.query,
            cursor_column=request.cursor_column,
            primary_key_column=request.primary_key_column,
            scan_after=request.scan_after,
            scan_through=request.scan_through,
            source_space=self.backend.source_space(),
            limit=request.limit,
        )


def duckdb(
    *,
    database: str = ":memory:",
    read_only: bool = False,
    default_schema: str | None = None,
    config: Mapping[str, object] | None = None,
) -> DuckDBSourceBackend:
    return DuckDBSourceBackend(
        database=database,
        read_only=read_only,
        default_schema=default_schema,
        config=config or {},
    )


__all__ = [
    "DuckDBSourceAdapter",
    "DuckDBSourceBackend",
    "duckdb",
]
