from __future__ import annotations

import importlib
import sys
from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from retl.auth import MappingSecretResolver
from retl.config import MappingConfigResolver
from retl.errors import DeclarationValidationError
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace


def _databricks_module_names() -> set[str]:
    return {name for name in sys.modules if name == "databricks" or name.startswith("databricks.")}


def _example_backend() -> Any:
    databricks_module = importlib.import_module("retl.backends.databricks")
    DatabricksSqlBackend = databricks_module.DatabricksSqlBackend
    DatabricksBackendAuth = databricks_module.DatabricksBackendAuth
    return DatabricksSqlBackend(
        server_hostname="dbc-a1b2345c-d6e7.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/a1b234c567d8e9fa",
        source_catalog="source_catalog",
        source_schema="source_schema",
        runtime_catalog="runtime_catalog",
        runtime_schema="runtime_schema",
        auth=DatabricksBackendAuth.from_namespace(
            auth_mode="pat",
            credential_namespace="backends.databricks.pat",
        ),
    )


def test_databricks_public_exports_do_not_import_optional_driver() -> None:
    before = _databricks_module_names()

    databricks_backend = importlib.import_module("retl.backends.databricks")
    expected = {
        "DATABRICKS_DIALECT",
        "DatabricksBackendAuth",
        "DatabricksConnection",
        "DatabricksRuntimeStore",
        "DatabricksSourceAdapter",
        "DatabricksSourceBackend",
        "DatabricksSqlBackend",
        "DatabricksSqlDialect",
    }

    assert expected <= set(databricks_backend.__all__)
    for name in expected:
        assert hasattr(databricks_backend, name)
    assert _databricks_module_names() == before


def test_databricks_backend_constructs_sql_collect_placement_without_driver_import() -> None:
    before = _databricks_module_names()

    backend = _example_backend()

    assert backend.name == "databricks"
    assert backend.source_space == SqlRelationSpace(
        backend_name="databricks",
        database="source_catalog",
        schema="source_schema",
        access="read_only",
    )
    assert backend.runtime_space == SqlRelationSpace(
        backend_name="databricks",
        database="runtime_catalog",
        schema="runtime_schema",
        access="read_write",
    )
    assert backend.placement == SqlCollectPlacement(
        source=backend.source_space,
        runtime=backend.runtime_space,
    )
    assert backend.placement.source != backend.placement.runtime
    assert _databricks_module_names() == before


@pytest.mark.parametrize(
    "field",
    [
        "server_hostname",
        "http_path",
        "source_catalog",
        "source_schema",
        "runtime_catalog",
        "runtime_schema",
    ],
)
def test_databricks_backend_rejects_blank_required_fields(field: str) -> None:
    databricks_module = importlib.import_module("retl.backends.databricks")
    DatabricksSqlBackend = databricks_module.DatabricksSqlBackend
    DatabricksBackendAuth = databricks_module.DatabricksBackendAuth
    config = {
        "server_hostname": "dbc-a1b2345c-d6e7.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/a1b234c567d8e9fa",
        "source_catalog": "source_catalog",
        "source_schema": "source_schema",
        "runtime_catalog": "runtime_catalog",
        "runtime_schema": "runtime_schema",
        "auth": DatabricksBackendAuth.from_namespace(
            auth_mode="pat",
            credential_namespace="backends.databricks.pat",
        ),
    }
    config[field] = " "

    with pytest.raises(DeclarationValidationError, match=field):
        DatabricksSqlBackend(**config)


def test_databricks_backend_rejects_hive_metastore_and_overlapping_spaces() -> None:
    databricks_module = importlib.import_module("retl.backends.databricks")
    DatabricksSqlBackend = databricks_module.DatabricksSqlBackend
    DatabricksBackendAuth = databricks_module.DatabricksBackendAuth
    auth = DatabricksBackendAuth.from_namespace(
        auth_mode="pat",
        credential_namespace="backends.databricks.pat",
    )

    with pytest.raises(DeclarationValidationError, match="hive_metastore"):
        DatabricksSqlBackend(
            server_hostname="dbc.example.com",
            http_path="/sql/1.0/warehouses/abc",
            source_catalog="hive_metastore",
            source_schema="source_schema",
            runtime_catalog="runtime_catalog",
            runtime_schema="runtime_schema",
            auth=auth,
        )

    with pytest.raises(DeclarationValidationError, match="must be distinct"):
        DatabricksSqlBackend(
            server_hostname="dbc.example.com",
            http_path="/sql/1.0/warehouses/abc",
            source_catalog="runtime_catalog",
            source_schema="runtime_schema",
            runtime_catalog="runtime_catalog",
            runtime_schema="runtime_schema",
            auth=auth,
        )


def test_databricks_from_config_expands_auth_namespaces_without_resolving_secrets() -> None:
    databricks_module = importlib.import_module("retl.backends.databricks")
    DatabricksSqlBackend = databricks_module.DatabricksSqlBackend
    resolver = MappingConfigResolver(
        {
            "backends.databricks.server_hostname": "dbc.example.com",
            "backends.databricks.http_path": "/sql/1.0/warehouses/abc",
            "backends.databricks.source_catalog": "src",
            "backends.databricks.source_schema": "app",
            "backends.databricks.runtime_catalog": "run",
            "backends.databricks.runtime_schema": "retl",
        }
    )

    backend = DatabricksSqlBackend.from_config(
        auth_mode="oauth_m2m",
        credential_namespace="backends.databricks.production",
        config_resolver=resolver,
    )

    assert backend.auth.mode.name == "oauth_m2m"
    assert backend.auth.credentials["client_id"].name == (
        "backends.databricks.production.client_id"
    )
    assert backend.auth.credentials["client_secret"].name == (
        "backends.databricks.production.client_secret"
    )
    assert backend.auth.evidence == {
        "mode": "oauth_m2m",
        "required_fields": {"client_id": True, "client_secret": True},
        "resolved": False,
    }


def test_databricks_auth_resolves_pat_only_for_connect_kwargs() -> None:
    auth_module = importlib.import_module("retl.backends.databricks.auth")
    auth = auth_module.DatabricksBackendAuth.from_namespace(
        auth_mode="pat",
        credential_namespace="backends.databricks.pat",
    )

    kwargs = auth_module.databricks_auth_connect_kwargs(
        auth,
        server_hostname="dbc.example.com",
        resolver=MappingSecretResolver({"backends.databricks.pat.token": "secret-token"}),
    )

    assert kwargs == {"access_token": "secret-token"}


def test_databricks_connection_uses_one_session_and_native_qmark_parameters() -> None:
    connection_module = importlib.import_module("retl.backends.databricks.connection")
    connector = RecordingDatabricksConnector()

    connection = connection_module.DatabricksConnection(
        server_hostname="dbc.example.com",
        http_path="/sql/1.0/warehouses/abc",
        catalog="runtime_catalog",
        schema="runtime_schema",
        connect_kwargs={"access_token": "secret-token"},
        connector=connector,
    )

    first = connection.execute("select ? as value", ("abc",))
    second = connection.execute("select ? as value", (123,))
    first.close()
    second.close()
    connection.close()

    assert connector.connect_count == 1
    assert connector.connect_kwargs["access_token"] == "secret-token"
    assert connector.raw_connection.autocommit is False
    assert connector.raw_connection.cursor_count == 2
    assert connector.raw_connection.calls == [
        ("select ? as value", ["abc"]),
        ("select ? as value", [123]),
    ]
    assert connector.raw_connection.close_count == 1
    assert "secret-token" not in repr(connection)


class RecordingDatabricksConnector:
    def __init__(self) -> None:
        self.connect_count = 0
        self.connect_kwargs: dict[str, object] = {}
        self.raw_connection = RecordingDatabricksRawConnection()

    def connect(self, **kwargs: object) -> "RecordingDatabricksRawConnection":
        self.connect_count += 1
        self.connect_kwargs = kwargs
        return self.raw_connection


class RecordingDatabricksRawConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[object] | dict[str, object]]] = []
        self.close_count = 0
        self.cursor_count = 0
        self.autocommit: bool | None = None

    def cursor(self) -> "RecordingDatabricksCursor":
        self.cursor_count += 1
        return RecordingDatabricksCursor(self)

    def close(self) -> None:
        self.close_count += 1


class RecordingDatabricksCursor:
    def __init__(self, connection: RecordingDatabricksRawConnection) -> None:
        self.connection = connection
        self.closed = False

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] | None = None,
    ) -> "RecordingDatabricksCursor":
        normalized: list[object] | dict[str, object]
        if isinstance(parameters, Mapping):
            normalized = dict(parameters)
        else:
            normalized = list(parameters or ())
        self.connection.calls.append((sql, normalized))
        return self

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return ()

    def fetchone(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True
