from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from retl.config import MappingConfigResolver
from retl.declarations import SecretRef
from retl.errors import DeclarationValidationError
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace


def _google_bigquery_module_names() -> set[str]:
    return {
        name
        for name in sys.modules
        if name == "google.cloud.bigquery"
        or name.startswith("google.cloud.bigquery.")
        or name == "google.cloud.bigquery_storage_v1"
        or name.startswith("google.cloud.bigquery_storage_v1.")
    }


def _example_backend() -> Any:
    bigquery_module = importlib.import_module("retl.backends.bigquery")
    return bigquery_module.BigQuerySqlBackend(
        project="example-analytics-project",
        location="US",
        source_project="example-source-project",
        source_dataset="mart",
        runtime_project="example-runtime-project",
        runtime_dataset="retl_runtime",
        auth=bigquery_module.BigQueryBackendAuth.application_default(),
    )


def test_bigquery_public_exports_are_backend_package_owned() -> None:
    before = _google_bigquery_module_names()

    bigquery_backend = importlib.import_module("retl.backends.bigquery")
    expected = {
        "BIGQUERY_DIALECT",
        "BigQueryBackendAuth",
        "BigQueryConnection",
        "BigQueryConnectionError",
        "BigQueryRuntimeStore",
        "BigQuerySourceAdapter",
        "BigQuerySourceBackend",
        "BigQuerySqlBackend",
        "BigQuerySqlDialect",
    }

    assert expected <= set(bigquery_backend.__all__)
    for name in expected:
        assert hasattr(bigquery_backend, name)
    assert _google_bigquery_module_names() == before


def test_bigquery_backend_config_constructs_sql_collect_placement_without_driver_import() -> None:
    before = _google_bigquery_module_names()
    backend = _example_backend()

    assert backend.name == "bigquery"
    assert backend.source_space == SqlRelationSpace(
        backend_name="bigquery",
        database="example-source-project",
        schema="mart",
        access="read_only",
    )
    assert backend.runtime_space == SqlRelationSpace(
        backend_name="bigquery",
        database="example-runtime-project",
        schema="retl_runtime",
        access="read_write",
    )
    assert backend.placement == SqlCollectPlacement(
        source=backend.source_space,
        runtime=backend.runtime_space,
    )
    assert backend.placement.source.backend_name == backend.placement.runtime.backend_name
    assert backend.placement.source != backend.placement.runtime
    assert _google_bigquery_module_names() == before


@pytest.mark.parametrize(
    "field",
    [
        "project",
        "source_project",
        "source_dataset",
        "runtime_project",
        "runtime_dataset",
    ],
)
def test_bigquery_backend_config_rejects_blank_required_fields(field: str) -> None:
    bigquery_module = importlib.import_module("retl.backends.bigquery")
    config = {
        "project": "example-analytics-project",
        "location": "US",
        "source_project": "example-source-project",
        "source_dataset": "mart",
        "runtime_project": "example-runtime-project",
        "runtime_dataset": "retl_runtime",
        "auth": bigquery_module.BigQueryBackendAuth.application_default(),
    }
    config[field] = " "

    with pytest.raises(DeclarationValidationError, match=field):
        bigquery_module.BigQuerySqlBackend(**config)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("project", "Billing-Project"),
        ("source_project", "source.project"),
        ("source_dataset", "source-dataset"),
        ("runtime_project", "runtime project"),
        ("runtime_dataset", "retl-runtime"),
    ],
)
def test_bigquery_backend_config_rejects_non_simple_relation_identifiers(
    field: str,
    value: str,
) -> None:
    bigquery_module = importlib.import_module("retl.backends.bigquery")
    config = {
        "project": "example-analytics-project",
        "location": "US",
        "source_project": "example-source-project",
        "source_dataset": "mart",
        "runtime_project": "example-runtime-project",
        "runtime_dataset": "retl_runtime",
        "auth": bigquery_module.BigQueryBackendAuth.application_default(),
    }
    config[field] = value

    with pytest.raises(DeclarationValidationError, match=field):
        bigquery_module.BigQuerySqlBackend(**config)


def test_bigquery_backend_config_rejects_identical_source_and_runtime_spaces() -> None:
    bigquery_module = importlib.import_module("retl.backends.bigquery")

    with pytest.raises(DeclarationValidationError, match="source and runtime"):
        bigquery_module.BigQuerySqlBackend(
            project="example-analytics-project",
            location="US",
            source_project="example-runtime-project",
            source_dataset="retl_runtime",
            runtime_project="example-runtime-project",
            runtime_dataset="retl_runtime",
            auth=bigquery_module.BigQueryBackendAuth.application_default(),
        )


def test_bigquery_auth_expands_service_account_namespace() -> None:
    bigquery_module = importlib.import_module("retl.backends.bigquery")

    auth = bigquery_module.BigQueryBackendAuth.from_namespace(
        auth_mode="service_account_json",
        credential_namespace="backends.bigquery.service_account",
    )

    assert auth.mode.name == "service_account_json"
    assert auth.credentials == {
        "credentials_json": SecretRef("backends.bigquery.service_account.credentials_json"),
    }
    assert "backends.bigquery.service_account.credentials_json" not in repr(auth)


def test_bigquery_backend_from_config_reads_public_config_and_defaults_projects() -> None:
    bigquery_module = importlib.import_module("retl.backends.bigquery")

    backend = bigquery_module.BigQuerySqlBackend.from_config(
        namespace="backends.bigquery",
        auth_mode="application_default",
        config_resolver=MappingConfigResolver(
            {
                "backends.bigquery.project": "example-analytics-project",
                "backends.bigquery.location": "US",
                "backends.bigquery.source_dataset": "mart",
                "backends.bigquery.runtime_dataset": "retl_runtime",
            }
        ),
    )

    assert backend.project == "example-analytics-project"
    assert backend.source_project == "example-analytics-project"
    assert backend.runtime_project == "example-analytics-project"
    assert backend.location == "US"
    assert backend.auth.mode.name == "application_default"


def test_bigquery_connection_adapter_translates_missing_optional_driver() -> None:
    connection_module = importlib.import_module("retl.backends.bigquery.connection")

    def missing_bigquery_import(name: str) -> Any:
        if name == "google.cloud.bigquery":
            raise ImportError("No module named google.cloud.bigquery")
        raise AssertionError(name)

    with pytest.raises(connection_module.BigQueryConnectionError) as exc_info:
        connection_module._bigquery_module(import_module=missing_bigquery_import)

    message = str(exc_info.value)
    assert "optional `bigquery` dependency" in message
    assert "retl[bigquery]" in message


def test_bigquery_connection_module_has_no_top_level_driver_import() -> None:
    connection_module = importlib.import_module("retl.backends.bigquery.connection")
    assert connection_module.__file__ is not None

    tree = ast.parse(Path(connection_module.__file__).read_text())
    top_level_driver_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.Import | ast.ImportFrom)
        and (
            any(
                alias.name == "google.cloud.bigquery"
                or alias.name.startswith("google.cloud.bigquery.")
                for alias in getattr(node, "names", ())
            )
            or (
                getattr(node, "module", None) == "google.cloud.bigquery"
                or str(getattr(node, "module", "")).startswith("google.cloud.bigquery.")
            )
        )
    ]

    assert top_level_driver_imports == []


def test_bigquery_connection_uses_query_client_parameters_and_storage_arrow_reads() -> None:
    connection_module = importlib.import_module("retl.backends.bigquery.connection")
    bigquery_module = RecordingBigQueryModule()
    storage_module = RecordingBigQueryStorageModule()

    connection = connection_module.BigQueryConnection(
        project="billing_project",
        location="US",
        bigquery_module=bigquery_module,
        bigquery_storage_module=storage_module,
    )

    result = connection.execute("select @customer_id as id, @active as active", [7, True])

    client = bigquery_module.client
    assert client is not None
    assert client.project == "billing_project"
    assert client.location == "US"
    assert client.queries == [
        ("select @customer_id as id, @active as active", ("customer_id", "active"))
    ]
    assert result.fetchall() == [(7, True)]
    table = result.fetch_arrow_all()
    assert table.column_names == ["id", "active"]
    assert storage_module.read_client.created is True


class RecordingBigQueryModule:
    def __init__(self) -> None:
        self.client: RecordingBigQueryClient | None = None

    def Client(self, **kwargs: Any) -> Any:  # noqa: N802
        self.client = RecordingBigQueryClient(**kwargs)
        return self.client

    class QueryJobConfig:
        def __init__(self, *, query_parameters: list[Any]) -> None:
            self.query_parameters = query_parameters

    class ScalarQueryParameter:
        def __init__(self, name: str, type_: str, value: object) -> None:
            self.name = name
            self.type_ = type_
            self.value = value


class RecordingBigQueryStorageModule:
    def __init__(self) -> None:
        self.read_client = RecordingBigQueryReadClient()

    def BigQueryReadClient(self) -> Any:  # noqa: N802
        self.read_client.created = True
        return self.read_client


class RecordingBigQueryReadClient:
    def __init__(self) -> None:
        self.created = False


class RecordingBigQueryClient:
    def __init__(self, **kwargs: Any) -> None:
        self.project = kwargs["project"]
        self.location = kwargs["location"]
        self.queries: list[tuple[str, tuple[str, ...]]] = []

    def query(self, sql: str, *, job_config: Any) -> Any:
        names = tuple(parameter.name for parameter in job_config.query_parameters)
        self.queries.append((sql, names))
        return RecordingBigQueryJob()


class RecordingBigQueryJob:
    def result(self) -> Any:
        return RecordingBigQueryRows()


class RecordingBigQueryRows:
    def __iter__(self) -> Any:
        return iter([(7, True)])

    def to_arrow(self, *, bqstorage_client: Any) -> Any:
        assert isinstance(bqstorage_client, RecordingBigQueryReadClient)
        return pa.table({"id": [7], "active": [True]})
