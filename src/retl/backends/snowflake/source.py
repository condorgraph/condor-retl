from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field

from retl.backends.snowflake.auth import SnowflakeBackendAuth
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
class SnowflakeSourceBackend:
    account: str
    warehouse: str
    database: str
    schema: str
    auth: SnowflakeBackendAuth = field(repr=False, compare=False)
    capabilities: SourceCapabilities = field(
        default_factory=lambda: SourceCapabilities(snapshots=True, checkpointed_windows=True)
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "account", _validate_required_string(self.account, "account"))
        object.__setattr__(self, "warehouse", _validate_identifier(self.warehouse, "warehouse"))
        object.__setattr__(self, "database", _validate_identifier(self.database, "database"))
        object.__setattr__(self, "schema", _validate_identifier(self.schema, "schema"))
        if not isinstance(self.auth, SnowflakeBackendAuth):
            raise DeclarationValidationError(
                "Snowflake source backend `auth` must be a SnowflakeBackendAuth."
            )

    @property
    def name(self) -> str:
        return "snowflake"

    def identity(self) -> Mapping[str, object]:
        identity: dict[str, object] = {
            "backend": "snowflake",
            "account": self.account,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "auth": self.auth.evidence,
        }
        return identity

    @property
    def sanitized_identity(self) -> Mapping[str, object]:
        return self.identity()

    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="snowflake",
            database=self.database,
            schema=self.schema,
            access="read_only",
        )

    def adapter(self) -> SnowflakeSourceAdapter:
        return SnowflakeSourceAdapter(backend=self)


@dataclass(frozen=True)
class SnowflakeSourceAdapter:
    backend: SnowflakeSourceBackend

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
            backend="snowflake",
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
            backend="snowflake",
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


def snowflake(
    *,
    account: str,
    warehouse: str,
    database: str,
    schema: str,
    auth: SnowflakeBackendAuth,
) -> SnowflakeSourceBackend:
    return SnowflakeSourceBackend(
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        auth=auth,
    )


def _validate_required_string(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"Snowflake source backend `{field_name}` must be non-empty."
        )
    return value.strip()


def _validate_identifier(value: str, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"Snowflake source backend `{field_name}` must be a simple SQL identifier."
        )
    return value


__all__ = [
    "SnowflakeSourceAdapter",
    "SnowflakeSourceBackend",
    "snowflake",
]
