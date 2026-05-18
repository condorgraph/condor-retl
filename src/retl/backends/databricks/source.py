from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field

from retl.backends.databricks.auth import DatabricksBackendAuth
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
class DatabricksSourceBackend:
    server_hostname: str
    http_path: str
    catalog: str
    schema: str
    auth: DatabricksBackendAuth = field(repr=False, compare=False)
    capabilities: SourceCapabilities = field(
        default_factory=lambda: SourceCapabilities(snapshots=True, checkpointed_windows=True)
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "server_hostname",
            _validate_required_string(self.server_hostname, "server_hostname"),
        )
        object.__setattr__(
            self,
            "http_path",
            _validate_required_string(self.http_path, "http_path"),
        )
        object.__setattr__(self, "catalog", _validate_catalog(self.catalog, "catalog"))
        object.__setattr__(self, "schema", _validate_identifier(self.schema, "schema"))
        if not isinstance(self.auth, DatabricksBackendAuth):
            raise DeclarationValidationError(
                "Databricks source backend `auth` must be a DatabricksBackendAuth."
            )

    @property
    def name(self) -> str:
        return "databricks"

    def identity(self) -> Mapping[str, object]:
        return {
            "backend": "databricks",
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "catalog": self.catalog,
            "schema": self.schema,
            "auth": self.auth.evidence,
        }

    @property
    def sanitized_identity(self) -> Mapping[str, object]:
        return self.identity()

    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="databricks",
            database=self.catalog,
            schema=self.schema,
            access="read_only",
        )

    def adapter(self) -> DatabricksSourceAdapter:
        return DatabricksSourceAdapter(backend=self)


@dataclass(frozen=True)
class DatabricksSourceAdapter:
    backend: DatabricksSourceBackend

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
            backend="databricks",
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
            backend="databricks",
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


def databricks(
    *,
    server_hostname: str,
    http_path: str,
    catalog: str,
    schema: str,
    auth: DatabricksBackendAuth,
) -> DatabricksSourceBackend:
    return DatabricksSourceBackend(
        server_hostname=server_hostname,
        http_path=http_path,
        catalog=catalog,
        schema=schema,
        auth=auth,
    )


def _validate_required_string(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"Databricks source backend `{field_name}` must be non-empty."
        )
    return value.strip()


def _validate_identifier(value: str, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"Databricks source backend `{field_name}` must be a simple SQL identifier."
        )
    return value


def _validate_catalog(value: str, field_name: str) -> str:
    value = _validate_identifier(value, field_name)
    if value.casefold() == "hive_metastore":
        raise DeclarationValidationError(
            "Databricks source backend supports only Unity Catalog managed Delta tables."
        )
    return value


__all__ = [
    "DatabricksSourceAdapter",
    "DatabricksSourceBackend",
    "databricks",
]
