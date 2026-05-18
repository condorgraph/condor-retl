from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from retl.backends.bigquery.auth import BigQueryBackendAuth
from retl.backends.bigquery.source import BigQuerySourceAdapter, BigQuerySourceBackend
from retl.config import ConfigResolutionError, ConfigResolver, configured_config_resolver
from retl.errors import DeclarationValidationError
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace

if TYPE_CHECKING:
    from retl.backends.bigquery.store import BigQueryRuntimeStore

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")
_NAMESPACE_SEGMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_DEFAULT_AUTH_MODE = "application_default"
_DEFAULT_CREDENTIAL_NAMESPACE = "backends.bigquery.service_account"


@dataclass(frozen=True)
class BigQuerySqlBackend:
    project: str
    location: str | None
    source_project: str
    source_dataset: str
    runtime_project: str
    runtime_dataset: str
    auth: BigQueryBackendAuth = field(
        default_factory=BigQueryBackendAuth.application_default,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        project = _validate_project_id(self.project, "project")
        location = _validate_optional_string(self.location, "location")
        source_project = _validate_project_id(self.source_project, "source_project")
        source_dataset = _validate_identifier(self.source_dataset, "source_dataset")
        runtime_project = _validate_project_id(self.runtime_project, "runtime_project")
        runtime_dataset = _validate_identifier(self.runtime_dataset, "runtime_dataset")
        if not isinstance(self.auth, BigQueryBackendAuth):
            raise DeclarationValidationError(
                "BigQuery SQL backend `auth` must be a BigQueryBackendAuth."
            )
        if (
            source_project.casefold(),
            source_dataset.casefold(),
        ) == (
            runtime_project.casefold(),
            runtime_dataset.casefold(),
        ):
            raise DeclarationValidationError(
                "BigQuery SQL backend source and runtime relation spaces must be distinct."
            )
        object.__setattr__(self, "project", project)
        object.__setattr__(self, "location", location)
        object.__setattr__(self, "source_project", source_project)
        object.__setattr__(self, "source_dataset", source_dataset)
        object.__setattr__(self, "runtime_project", runtime_project)
        object.__setattr__(self, "runtime_dataset", runtime_dataset)

    @classmethod
    def from_config(
        cls,
        *,
        namespace: str = "backends.bigquery",
        auth_mode: str = _DEFAULT_AUTH_MODE,
        credential_namespace: str | None = None,
        config_resolver: ConfigResolver | None = None,
    ) -> BigQuerySqlBackend:
        namespace = _validate_namespace(namespace, field_name="namespace")
        resolver = config_resolver or configured_config_resolver()
        credential_namespace = _validate_namespace(
            credential_namespace or _DEFAULT_CREDENTIAL_NAMESPACE,
            field_name="credential_namespace",
        )
        project = _required_config(resolver, f"{namespace}.project")
        return cls(
            project=project,
            location=_optional_config(resolver, f"{namespace}.location"),
            source_project=_optional_config(
                resolver,
                f"{namespace}.source_project",
                default=project,
            )
            or project,
            source_dataset=_required_config(resolver, f"{namespace}.source_dataset"),
            runtime_project=_optional_config(
                resolver,
                f"{namespace}.runtime_project",
                default=project,
            )
            or project,
            runtime_dataset=_required_config(resolver, f"{namespace}.runtime_dataset"),
            auth=BigQueryBackendAuth.from_namespace(
                auth_mode=auth_mode,
                credential_namespace=credential_namespace,
            ),
        )

    @property
    def name(self) -> str:
        return "bigquery"

    @property
    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="bigquery",
            database=self.source_project,
            schema=self.source_dataset,
            access="read_only",
        )

    @property
    def runtime_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="bigquery",
            database=self.runtime_project,
            schema=self.runtime_dataset,
            access="read_write",
        )

    @property
    def placement(self) -> SqlCollectPlacement:
        return SqlCollectPlacement(source=self.source_space, runtime=self.runtime_space)

    def source_backend(self) -> BigQuerySourceBackend:
        return BigQuerySourceBackend(
            project=self.project,
            location=self.location,
            source_project=self.source_project,
            source_dataset=self.source_dataset,
            auth=self.auth,
        )

    def source_adapter(self) -> BigQuerySourceAdapter:
        return self.source_backend().adapter()

    def runtime_store(
        self,
        *,
        client: Any | None = None,
        read_client: Any | None = None,
        bigquery_module: Any | None = None,
        bigquery_storage_module: Any | None = None,
    ) -> BigQueryRuntimeStore:
        from retl.backends.bigquery.store import BigQueryRuntimeStore

        return BigQueryRuntimeStore(
            backend=self,
            client=client,
            read_client=read_client,
            bigquery_module=bigquery_module,
            bigquery_storage_module=bigquery_storage_module,
        )


def _validate_required_string(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"BigQuery SQL backend `{field_name}` must be non-empty.")
    return value.strip()


def _validate_optional_string(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    value = _validate_required_string(value, field_name)
    return value


def _validate_identifier(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"BigQuery SQL backend `{field_name}` must be a simple SQL identifier."
        )
    return value


def _validate_project_id(value: str | None, field_name: str) -> str:
    value = _validate_required_string(value, field_name)
    if (
        _PROJECT_ID_PATTERN.fullmatch(value) is None
        and _IDENTIFIER_PATTERN.fullmatch(value) is None
    ):
        raise DeclarationValidationError(
            f"BigQuery SQL backend `{field_name}` must be a Google project ID or "
            "a simple SQL identifier."
        )
    return value


def _required_config(resolver: ConfigResolver, name: str) -> str:
    value = resolver.resolve(name)
    if value is None:
        raise ConfigResolutionError(
            f"Missing public config `{name}` for BigQuery backend construction."
        )
    return value


def _optional_config(
    resolver: ConfigResolver, name: str, *, default: str | None = None
) -> str | None:
    value = resolver.resolve(name)
    if value is None or not value.strip():
        return default
    return value


def _validate_namespace(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(
            f"BigQuery SQL backend `{field_name}` must be a non-empty dotted namespace."
        )
    namespace = value.strip()
    for segment in namespace.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                f"BigQuery SQL backend `{field_name}` must contain only dotted identifier segments."
            )
    return namespace


__all__ = ["BigQuerySqlBackend"]
