from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field

from retl.backends.bigquery.auth import BigQueryBackendAuth
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
_PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


@dataclass(frozen=True)
class BigQuerySourceBackend:
    project: str
    location: str | None
    source_project: str
    source_dataset: str
    auth: BigQueryBackendAuth = field(repr=False, compare=False)
    capabilities: SourceCapabilities = field(
        default_factory=lambda: SourceCapabilities(snapshots=True, checkpointed_windows=True)
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "project", _validate_project_id(self.project, "project"))
        object.__setattr__(
            self,
            "location",
            _validate_optional_string(self.location, "location"),
        )
        object.__setattr__(
            self,
            "source_project",
            _validate_project_id(self.source_project, "source_project"),
        )
        object.__setattr__(
            self,
            "source_dataset",
            _validate_identifier(self.source_dataset, "source_dataset"),
        )
        if not isinstance(self.auth, BigQueryBackendAuth):
            raise DeclarationValidationError(
                "BigQuery source backend `auth` must be a BigQueryBackendAuth."
            )

    @property
    def name(self) -> str:
        return "bigquery"

    def identity(self) -> Mapping[str, object]:
        return {
            "backend": "bigquery",
            "project": self.project,
            "location": self.location,
            "source_project": self.source_project,
            "source_dataset": self.source_dataset,
            "auth": self.auth.evidence,
        }

    @property
    def sanitized_identity(self) -> Mapping[str, object]:
        return self.identity()

    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="bigquery",
            database=self.source_project,
            schema=self.source_dataset,
            access="read_only",
        )

    def adapter(self) -> BigQuerySourceAdapter:
        return BigQuerySourceAdapter(backend=self)


@dataclass(frozen=True)
class BigQuerySourceAdapter:
    backend: BigQuerySourceBackend

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
            backend="bigquery",
            source_name=request.source_name,
            source_identity=self.backend.identity(),
            query=request.query,
            source_space=self.backend.source_space(),
        )

    def prepare_event_source_window(
        self,
        request: EventSourceWindowRequest,
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
            backend="bigquery",
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


def bigquery(
    *,
    project: str,
    source_project: str,
    source_dataset: str,
    auth: BigQueryBackendAuth | None = None,
    location: str | None = None,
) -> BigQuerySourceBackend:
    return BigQuerySourceBackend(
        project=project,
        location=location,
        source_project=source_project,
        source_dataset=source_dataset,
        auth=auth or BigQueryBackendAuth.application_default(),
    )


def _validate_required_string(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"BigQuery source backend `{field_name}` must be non-empty."
        )
    return value.strip()


def _validate_optional_string(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _validate_required_string(value, field_name)


def _validate_identifier(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"BigQuery source backend `{field_name}` must be a simple SQL identifier."
        )
    return value


def _validate_project_id(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if (
        _PROJECT_ID_PATTERN.fullmatch(value) is None
        and _IDENTIFIER_PATTERN.fullmatch(value) is None
    ):
        raise DeclarationValidationError(
            f"BigQuery source backend `{field_name}` must be a Google project ID or "
            "a simple SQL identifier."
        )
    return value


__all__ = [
    "BigQuerySourceAdapter",
    "BigQuerySourceBackend",
    "bigquery",
]
