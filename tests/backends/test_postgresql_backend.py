from __future__ import annotations

import importlib
import sys
from typing import Any

import pytest

from retl.auth import MappingSecretResolver
from retl.config import MappingConfigResolver
from retl.declarations import SecretRef
from retl.errors import DeclarationValidationError
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace


def _postgresql_module_names() -> set[str]:
    return {name for name in sys.modules if name == "psycopg" or name.startswith("psycopg.")}


def _example_backend() -> Any:
    postgresql_module = importlib.import_module("retl.backends.postgresql")
    return postgresql_module.PostgreSqlBackend(
        host="localhost",
        port=5432,
        database="app",
        source_schema="public",
        runtime_schema="retl",
        auth=postgresql_module.PostgreSqlBackendAuth.from_namespace(
            auth_mode="password",
            credential_namespace="backends.postgresql.password",
        ),
    )


def test_postgresql_public_exports_are_backend_package_owned_without_driver_import() -> None:
    before = _postgresql_module_names()
    postgresql_backend = importlib.import_module("retl.backends.postgresql")
    expected = {
        "POSTGRESQL_DIALECT",
        "PostgreSqlBackend",
        "PostgreSqlBackendAuth",
        "PostgreSqlConnection",
        "PostgreSqlConnectionError",
        "PostgreSqlRuntimeStore",
        "PostgreSqlSourceAdapter",
        "PostgreSqlSourceBackend",
    }

    assert expected <= set(postgresql_backend.__all__)
    for name in expected:
        assert hasattr(postgresql_backend, name)
    assert _postgresql_module_names() == before


def test_postgresql_backend_config_constructs_sql_collect_placement_without_driver_import() -> None:
    before = _postgresql_module_names()
    backend = _example_backend()

    assert backend.name == "postgresql"
    assert backend.source_space == SqlRelationSpace(
        backend_name="postgresql",
        database="app",
        schema="public",
        access="read_only",
    )
    assert backend.runtime_space == SqlRelationSpace(
        backend_name="postgresql",
        database="app",
        schema="retl",
        access="read_write",
    )
    assert backend.placement == SqlCollectPlacement(
        source=backend.source_space,
        runtime=backend.runtime_space,
    )
    assert backend.sslmode == "require"
    assert backend.placement.source.backend_name == backend.placement.runtime.backend_name
    assert backend.placement.source != backend.placement.runtime
    assert _postgresql_module_names() == before


@pytest.mark.parametrize("field", ["host", "database", "source_schema", "runtime_schema"])
def test_postgresql_backend_config_rejects_blank_required_fields(field: str) -> None:
    postgresql_module = importlib.import_module("retl.backends.postgresql")
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "app",
        "source_schema": "public",
        "runtime_schema": "retl",
        "auth": postgresql_module.PostgreSqlBackendAuth.from_namespace(
            auth_mode="password",
            credential_namespace="backends.postgresql.password",
        ),
    }
    config[field] = " "

    with pytest.raises(DeclarationValidationError, match=field):
        postgresql_module.PostgreSqlBackend(**config)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("database", "app-db"),
        ("source_schema", "public.data"),
        ("runtime_schema", "retl-runtime"),
    ],
)
def test_postgresql_backend_config_rejects_non_simple_relation_identifiers(
    field: str,
    value: str,
) -> None:
    postgresql_module = importlib.import_module("retl.backends.postgresql")
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "app",
        "source_schema": "public",
        "runtime_schema": "retl",
        "auth": postgresql_module.PostgreSqlBackendAuth.from_namespace(
            auth_mode="password",
            credential_namespace="backends.postgresql.password",
        ),
    }
    config[field] = value

    with pytest.raises(DeclarationValidationError, match=field):
        postgresql_module.PostgreSqlBackend(**config)


def test_postgresql_backend_config_rejects_identical_source_and_runtime_schemas() -> None:
    postgresql_module = importlib.import_module("retl.backends.postgresql")

    with pytest.raises(DeclarationValidationError, match="source and runtime"):
        postgresql_module.PostgreSqlBackend(
            host="localhost",
            port=5432,
            database="app",
            source_schema="retl",
            runtime_schema="RETL",
            auth=postgresql_module.PostgreSqlBackendAuth.from_namespace(
                auth_mode="password",
                credential_namespace="backends.postgresql.password",
            ),
        )


def test_postgresql_backend_auth_expands_password_namespace() -> None:
    postgresql_module = importlib.import_module("retl.backends.postgresql")

    auth = postgresql_module.PostgreSqlBackendAuth.from_namespace(
        auth_mode="password",
        credential_namespace="backends.postgresql.password",
    )

    assert auth.mode.name == "password"
    assert auth.credentials == {
        "user": SecretRef("backends.postgresql.password.user"),
        "password": SecretRef("backends.postgresql.password.password"),
    }
    assert "backends.postgresql.password.password" not in repr(auth)


def test_postgresql_backend_from_config_reads_public_config_and_credential_namespace() -> None:
    postgresql_module = importlib.import_module("retl.backends.postgresql")

    backend = postgresql_module.PostgreSqlBackend.from_config(
        namespace="backends.postgresql",
        credential_namespace="backends.postgresql.password",
        config_resolver=MappingConfigResolver(
            {
                "backends.postgresql.host": "db.example.com",
                "backends.postgresql.port": "5433",
                "backends.postgresql.database": "app",
                "backends.postgresql.source_schema": "analytics",
                "backends.postgresql.runtime_schema": "retl_runtime",
                "backends.postgresql.sslmode": "require",
                "backends.postgresql.connect_timeout": "10",
            }
        ),
    )

    assert backend.host == "db.example.com"
    assert backend.port == 5433
    assert backend.source_schema == "analytics"
    assert backend.runtime_schema == "retl_runtime"
    assert backend.sslmode == "require"
    assert backend.connect_timeout == 10
    assert backend.auth.credentials["password"] == SecretRef(
        "backends.postgresql.password.password"
    )


def test_postgresql_backend_from_config_defaults_optional_values() -> None:
    postgresql_module = importlib.import_module("retl.backends.postgresql")

    backend = postgresql_module.PostgreSqlBackend.from_config(
        config_resolver=MappingConfigResolver(
            {
                "backends.postgresql.host": "localhost",
                "backends.postgresql.database": "app",
            }
        ),
    )

    assert backend.port == 5432
    assert backend.source_schema == "public"
    assert backend.runtime_schema == "retl"
    assert backend.sslmode == "require"


def test_postgresql_backend_from_config_preserves_explicit_sslmode_override() -> None:
    postgresql_module = importlib.import_module("retl.backends.postgresql")

    backend = postgresql_module.PostgreSqlBackend.from_config(
        config_resolver=MappingConfigResolver(
            {
                "backends.postgresql.host": "localhost",
                "backends.postgresql.database": "app",
                "backends.postgresql.sslmode": "verify-full",
            }
        ),
    )

    assert backend.sslmode == "verify-full"


def test_postgresql_password_auth_resolves_to_backend_native_kwargs() -> None:
    auth_module = importlib.import_module("retl.backends.postgresql.auth")
    postgresql_module = importlib.import_module("retl.backends.postgresql")
    auth = postgresql_module.PostgreSqlBackendAuth.from_namespace(
        auth_mode="password",
        credential_namespace="backends.postgresql.password",
    )

    assert auth_module.postgresql_auth_connect_kwargs(
        auth,
        resolver=MappingSecretResolver(
            {
                "backends.postgresql.password.user": "retl_user",
                "backends.postgresql.password.password": "secret-password",
            }
        ),
    ) == {
        "user": "retl_user",
        "password": "secret-password",
    }


def test_postgresql_connection_reports_missing_optional_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection_module = importlib.import_module("retl.backends.postgresql.connection")

    def missing_driver(name: str) -> Any:
        if name == "psycopg":
            raise ImportError("missing")
        return importlib.import_module(name)

    monkeypatch.setattr(connection_module.importlib, "import_module", missing_driver)

    with pytest.raises(connection_module.PostgreSqlConnectionError, match="psycopg"):
        connection_module.PostgreSqlConnection(host="localhost", dbname="app")
