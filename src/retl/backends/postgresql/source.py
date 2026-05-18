from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field

from retl.backends.postgresql.auth import PostgreSqlBackendAuth
from retl.errors import DeclarationValidationError
from retl.sources.contracts import SourceCapabilities
from retl.stores.contracts import (
    EventSourceWindowHandle,
    EventSourceWindowRequest,
    SqlRelationSpace,
    StateSnapshotHandle,
    StateSnapshotRequest,
)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class PostgreSqlSourceBackend:
    host: str
    port: int
    database: str
    schema: str
    auth: PostgreSqlBackendAuth = field(repr=False, compare=False)
    sslmode: str | None = None
    connect_timeout: int | None = None
    capabilities: SourceCapabilities = field(
        default_factory=lambda: SourceCapabilities(snapshots=True, checkpointed_windows=True)
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "host", _validate_required_string(self.host, "host"))
        object.__setattr__(self, "database", _validate_identifier(self.database, "database"))
        object.__setattr__(self, "schema", _validate_identifier(self.schema, "schema"))
        if not isinstance(self.auth, PostgreSqlBackendAuth):
            raise DeclarationValidationError(
                "PostgreSQL source backend `auth` must be a PostgreSqlBackendAuth."
            )

    @property
    def name(self) -> str:
        return "postgresql"

    def identity(self) -> Mapping[str, object]:
        return {
            "backend": "postgresql",
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "schema": self.schema,
            "sslmode": self.sslmode,
            "auth": self.auth.evidence,
        }

    @property
    def sanitized_identity(self) -> Mapping[str, object]:
        return self.identity()

    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="postgresql",
            database=self.database,
            schema=self.schema,
            access="read_only",
        )

    def adapter(self) -> PostgreSqlSourceAdapter:
        return PostgreSqlSourceAdapter(backend=self)


@dataclass(frozen=True)
class PostgreSqlSourceAdapter:
    backend: PostgreSqlSourceBackend

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
            backend="postgresql",
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
            backend="postgresql",
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


def postgresql(
    *,
    host: str,
    port: int,
    database: str,
    schema: str,
    auth: PostgreSqlBackendAuth,
    sslmode: str | None = None,
    connect_timeout: int | None = None,
) -> PostgreSqlSourceBackend:
    return PostgreSqlSourceBackend(
        host=host,
        port=port,
        database=database,
        schema=schema,
        auth=auth,
        sslmode=sslmode,
        connect_timeout=connect_timeout,
    )


def _validate_required_string(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"PostgreSQL source backend `{field_name}` must be non-empty."
        )
    return value.strip()


def _validate_identifier(value: str, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"PostgreSQL source backend `{field_name}` must be a simple SQL identifier."
        )
    return value


__all__ = [
    "PostgreSqlSourceAdapter",
    "PostgreSqlSourceBackend",
    "postgresql",
]
