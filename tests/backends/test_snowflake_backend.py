from __future__ import annotations

import ast
import importlib
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from retl.auth import MappingSecretResolver
from retl.config import MappingConfigResolver
from retl.declarations import SecretRef
from retl.errors import DeclarationValidationError
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SNOWFLAKE_PACKAGE = _REPO_ROOT / "src" / "retl" / "backends" / "snowflake"
_BACKEND_MODULE_EXISTS = (_SNOWFLAKE_PACKAGE / "backend.py").exists()
_SOURCE_MODULE_EXISTS = (_SNOWFLAKE_PACKAGE / "source.py").exists()
_STORE_MODULE_EXISTS = (_SNOWFLAKE_PACKAGE / "store.py").exists()
_CONNECTION_MODULE_EXISTS = (_SNOWFLAKE_PACKAGE / "connection.py").exists()
_DIALECT_MODULE_EXISTS = (_SNOWFLAKE_PACKAGE / "dialect.py").exists()
_PACKAGE_EXISTS = (_SNOWFLAKE_PACKAGE / "__init__.py").exists()

requires_step3_backend = pytest.mark.xfail(
    not _BACKEND_MODULE_EXISTS,
    reason="Step 3 adds the Snowflake backend scaffold and placement validation.",
    strict=True,
)
requires_step3_scaffold_exports = pytest.mark.xfail(
    not (
        _PACKAGE_EXISTS
        and _BACKEND_MODULE_EXISTS
        and _SOURCE_MODULE_EXISTS
        and _STORE_MODULE_EXISTS
    ),
    reason="Step 3 adds Snowflake backend, source, and runtime store public exports.",
    strict=True,
)
requires_step4_connection = pytest.mark.xfail(
    not _CONNECTION_MODULE_EXISTS,
    reason="Step 4 adds the Snowflake connection wrapper and optional dependency guard.",
    strict=True,
)
requires_step4_connection_exports = pytest.mark.xfail(
    not (_PACKAGE_EXISTS and _CONNECTION_MODULE_EXISTS),
    reason="Step 4 adds Snowflake connection public exports.",
    strict=True,
)
requires_step5_dialect_exports = pytest.mark.xfail(
    not (_PACKAGE_EXISTS and _DIALECT_MODULE_EXISTS),
    reason="Step 5 adds Snowflake dialect public exports.",
    strict=True,
)


def _snowflake_module_names() -> set[str]:
    return {name for name in sys.modules if name == "snowflake" or name.startswith("snowflake.")}


def _example_backend() -> Any:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeSqlBackend = snowflake_module.SnowflakeSqlBackend  # type: ignore[attr-defined]
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]

    return SnowflakeSqlBackend(
        account="xy12345.us-east-1",
        warehouse="RETL_WH",
        source_database="SOURCE_DB",
        source_schema="APP",
        runtime_database="RETL_DB",
        runtime_schema="RETL_RUNTIME",
        auth=SnowflakeBackendAuth.from_namespace(
            auth_mode="password",
            credential_namespace="backends.snowflake.password",
        ),
    )


@requires_step3_scaffold_exports
def test_snowflake_step3_scaffold_public_exports_are_backend_package_owned() -> None:
    before = _snowflake_module_names()

    snowflake_backend = importlib.import_module("retl.backends.snowflake")
    expected = {
        "SnowflakeRuntimeStore",
        "SnowflakeSourceAdapter",
        "SnowflakeSourceBackend",
        "SnowflakeSqlBackend",
    }

    assert expected <= set(snowflake_backend.__all__)
    for name in expected:
        assert hasattr(snowflake_backend, name)
    assert _snowflake_module_names() == before


@requires_step4_connection_exports
def test_snowflake_step4_connection_public_exports_are_backend_package_owned() -> None:
    before = _snowflake_module_names()

    snowflake_backend = importlib.import_module("retl.backends.snowflake")
    expected = {"SnowflakeConnection", "SnowflakeConnectionError"}

    assert expected <= set(snowflake_backend.__all__)
    for name in expected:
        assert hasattr(snowflake_backend, name)
    assert _snowflake_module_names() == before


@requires_step5_dialect_exports
def test_snowflake_step5_dialect_public_exports_are_backend_package_owned() -> None:
    before = _snowflake_module_names()

    snowflake_backend = importlib.import_module("retl.backends.snowflake")
    expected = {"SNOWFLAKE_DIALECT", "SnowflakeSqlDialect"}

    assert expected <= set(snowflake_backend.__all__)
    for name in expected:
        assert hasattr(snowflake_backend, name)
    assert _snowflake_module_names() == before


@requires_step3_backend
def test_snowflake_backend_config_constructs_sql_collect_placement_without_driver_import() -> None:
    before = _snowflake_module_names()

    backend = _example_backend()

    assert backend.name == "snowflake"
    assert backend.source_space == SqlRelationSpace(
        backend_name="snowflake",
        database="SOURCE_DB",
        schema="APP",
        access="read_only",
    )
    assert backend.runtime_space == SqlRelationSpace(
        backend_name="snowflake",
        database="RETL_DB",
        schema="RETL_RUNTIME",
        access="read_write",
    )
    assert backend.placement == SqlCollectPlacement(
        source=backend.source_space,
        runtime=backend.runtime_space,
    )
    assert backend.placement.source.backend_name == backend.placement.runtime.backend_name
    assert backend.placement.source != backend.placement.runtime
    assert _snowflake_module_names() == before


@requires_step3_backend
@pytest.mark.parametrize(
    "field",
    [
        "account",
        "warehouse",
        "source_database",
        "source_schema",
        "runtime_database",
        "runtime_schema",
    ],
)
def test_snowflake_backend_config_rejects_blank_required_fields(field: str) -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeSqlBackend = snowflake_module.SnowflakeSqlBackend  # type: ignore[attr-defined]
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]

    config = {
        "account": "xy12345.us-east-1",
        "warehouse": "RETL_WH",
        "source_database": "SOURCE_DB",
        "source_schema": "APP",
        "runtime_database": "RETL_DB",
        "runtime_schema": "RETL_RUNTIME",
        "auth": SnowflakeBackendAuth.from_namespace(
            auth_mode="password",
            credential_namespace="backends.snowflake.password",
        ),
    }
    config[field] = " "

    with pytest.raises(DeclarationValidationError, match=field):
        SnowflakeSqlBackend(**config)


@requires_step3_backend
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("warehouse", "bad-warehouse"),
        ("source_database", "SOURCE.DB"),
        ("source_schema", "APP-SCHEMA"),
        ("runtime_database", "RETL DB"),
        ("runtime_schema", "retl-runtime"),
    ],
)
def test_snowflake_backend_config_rejects_non_simple_relation_identifiers(
    field: str,
    value: str,
) -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeSqlBackend = snowflake_module.SnowflakeSqlBackend  # type: ignore[attr-defined]
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]

    config = {
        "account": "xy12345.us-east-1",
        "warehouse": "RETL_WH",
        "source_database": "SOURCE_DB",
        "source_schema": "APP",
        "runtime_database": "RETL_DB",
        "runtime_schema": "RETL_RUNTIME",
        "auth": SnowflakeBackendAuth.from_namespace(
            auth_mode="password",
            credential_namespace="backends.snowflake.password",
        ),
    }
    config[field] = value

    with pytest.raises(DeclarationValidationError, match=field):
        SnowflakeSqlBackend(**config)


@requires_step3_backend
def test_snowflake_backend_config_rejects_identical_source_and_runtime_spaces() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeSqlBackend = snowflake_module.SnowflakeSqlBackend  # type: ignore[attr-defined]

    with pytest.raises(DeclarationValidationError, match="source and runtime"):
        SnowflakeSqlBackend(
            account="xy12345.us-east-1",
            warehouse="RETL_WH",
            source_database="RETL_DB",
            source_schema="RETL_RUNTIME",
            runtime_database="RETL_DB",
            runtime_schema="RETL_RUNTIME",
            auth=snowflake_module.SnowflakeBackendAuth.from_namespace(
                auth_mode="password",
                credential_namespace="backends.snowflake.password",
            ),
        )


@requires_step3_backend
def test_snowflake_backend_auth_expands_password_namespace() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]

    auth = SnowflakeBackendAuth.from_namespace(
        auth_mode="password",
        credential_namespace="backends.snowflake.password",
    )

    assert auth.mode.name == "password"
    assert auth.credentials == {
        "user": SecretRef("backends.snowflake.password.user"),
        "password": SecretRef("backends.snowflake.password.password"),
        "role": SecretRef("backends.snowflake.password.role"),
    }
    assert "backends.snowflake.password.password" not in repr(auth)


@requires_step3_backend
def test_snowflake_backend_auth_expands_key_pair_namespace() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]

    auth = SnowflakeBackendAuth.from_namespace(
        auth_mode="key_pair",
        credential_namespace="backends.snowflake.key_pair",
    )

    assert auth.mode.name == "key_pair"
    assert auth.credentials == {
        "user": SecretRef("backends.snowflake.key_pair.user"),
        "private_key": SecretRef("backends.snowflake.key_pair.private_key"),
        "private_key_path": SecretRef("backends.snowflake.key_pair.private_key_path"),
        "private_key_passphrase": SecretRef("backends.snowflake.key_pair.private_key_passphrase"),
        "role": SecretRef("backends.snowflake.key_pair.role"),
    }


@requires_step3_backend
@pytest.mark.parametrize(
    "credential_namespace",
    [
        "",
        "backends..snowflake",
        "backends.snowflake.",
        "backends.1snowflake",
        "backends.snow-flake",
        "backends.snowflake.*",
    ],
)
def test_snowflake_backend_auth_rejects_invalid_credential_namespace(
    credential_namespace: str,
) -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]

    with pytest.raises(DeclarationValidationError, match="credential_namespace"):
        SnowflakeBackendAuth.from_namespace(
            auth_mode="password",
            credential_namespace=credential_namespace,
        )


@requires_step3_backend
def test_snowflake_backend_from_config_reads_public_config_and_credential_namespace() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeSqlBackend = snowflake_module.SnowflakeSqlBackend  # type: ignore[attr-defined]

    backend = SnowflakeSqlBackend.from_config(
        namespace="backends.snowflake",
        auth_mode="password",
        credential_namespace="backends.snowflake.password",
        config_resolver=MappingConfigResolver(
            {
                "backends.snowflake.account": "xy12345.us-east-1",
                "backends.snowflake.warehouse": "RETL_WH",
                "backends.snowflake.source_database": "SOURCE_DB",
                "backends.snowflake.source_schema": "APP",
                "backends.snowflake.runtime_database": "RETL_DB",
                "backends.snowflake.runtime_schema": "RETL_RUNTIME",
            }
        ),
    )

    assert backend.account == "xy12345.us-east-1"
    assert backend.source_schema == "APP"
    assert backend.runtime_schema == "RETL_RUNTIME"
    assert backend.auth.credentials["password"] == SecretRef("backends.snowflake.password.password")


@requires_step3_backend
def test_snowflake_backend_from_config_defaults_optional_schemas() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeSqlBackend = snowflake_module.SnowflakeSqlBackend  # type: ignore[attr-defined]

    backend = SnowflakeSqlBackend.from_config(
        namespace="backends.snowflake",
        auth_mode="password",
        credential_namespace="backends.snowflake.password",
        config_resolver=MappingConfigResolver(
            {
                "backends.snowflake.account": "xy12345.us-east-1",
                "backends.snowflake.warehouse": "RETL_WH",
                "backends.snowflake.source_database": "SOURCE_DB",
                "backends.snowflake.runtime_database": "RETL_DB",
            }
        ),
    )

    assert backend.source_schema == "PUBLIC"
    assert backend.runtime_schema == "RETL"


@requires_step3_backend
@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("namespace", "backends..snowflake"),
        ("namespace", "backends.1snowflake"),
        ("namespace", "backends.snowflake.*"),
        ("credential_namespace", "backends..snowflake.password"),
        ("credential_namespace", "backends.snowflake.password."),
        ("credential_namespace", "backends.snowflake.password-*"),
    ],
)
def test_snowflake_backend_from_config_rejects_invalid_namespaces(
    field_name: str,
    value: str,
) -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeSqlBackend = snowflake_module.SnowflakeSqlBackend  # type: ignore[attr-defined]
    kwargs = {
        "namespace": "backends.snowflake",
        "auth_mode": "password",
        "credential_namespace": "backends.snowflake.password",
        "config_resolver": MappingConfigResolver({}),
    }
    kwargs[field_name] = value

    with pytest.raises(DeclarationValidationError, match=field_name):
        SnowflakeSqlBackend.from_config(**kwargs)


@requires_step3_backend
def test_snowflake_password_auth_resolves_to_backend_native_kwargs() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]
    snowflake_auth_connect_kwargs = importlib.import_module(
        "retl.backends.snowflake.auth"
    ).snowflake_auth_connect_kwargs

    auth = SnowflakeBackendAuth.from_namespace(
        auth_mode="password",
        credential_namespace="backends.snowflake.password",
    )

    assert snowflake_auth_connect_kwargs(
        auth,
        resolver=MappingSecretResolver(
            {
                "backends.snowflake.password.user": "retl_user",
                "backends.snowflake.password.password": "secret-password",
            }
        ),
    ) == {
        "user": "retl_user",
        "password": "secret-password",
    }


@requires_step3_backend
def test_snowflake_key_pair_auth_resolves_to_backend_native_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]
    auth_module = importlib.import_module("retl.backends.snowflake.auth")

    monkeypatch.setattr(
        auth_module,
        "_private_key_der",
        lambda private_key, *, passphrase: f"parsed:{private_key}:{passphrase}".encode(),
    )
    auth = SnowflakeBackendAuth.from_namespace(
        auth_mode="key_pair",
        credential_namespace="backends.snowflake.key_pair",
    )

    assert auth_module.snowflake_auth_connect_kwargs(
        auth,
        resolver=MappingSecretResolver(
            {
                "backends.snowflake.key_pair.user": "retl_user",
                "backends.snowflake.key_pair.private_key": "pem",
                "backends.snowflake.key_pair.private_key_passphrase": "phrase",
            }
        ),
    ) == {
        "user": "retl_user",
        "private_key": b"parsed:pem:phrase",
    }


@requires_step3_backend
def test_snowflake_key_pair_auth_resolves_private_key_path_to_backend_native_kwargs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]
    auth_module = importlib.import_module("retl.backends.snowflake.auth")
    private_key_path = tmp_path / "snowflake_key.p8"
    private_key_path.write_text("pem-from-file", encoding="utf-8")

    monkeypatch.setattr(
        auth_module,
        "_private_key_der",
        lambda private_key, *, passphrase: f"parsed:{private_key}:{passphrase}".encode(),
    )
    auth = SnowflakeBackendAuth.from_namespace(
        auth_mode="key_pair",
        credential_namespace="backends.snowflake.key_pair",
    )

    assert auth_module.snowflake_auth_connect_kwargs(
        auth,
        resolver=MappingSecretResolver(
            {
                "backends.snowflake.key_pair.user": "retl_user",
                "backends.snowflake.key_pair.private_key_path": str(private_key_path),
                "backends.snowflake.key_pair.private_key_passphrase": "phrase",
            }
        ),
    ) == {
        "user": "retl_user",
        "private_key": b"parsed:pem-from-file:phrase",
    }


@requires_step3_backend
def test_snowflake_key_pair_auth_rejects_multiple_private_key_sources() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]
    auth_module = importlib.import_module("retl.backends.snowflake.auth")
    auth = SnowflakeBackendAuth.from_namespace(
        auth_mode="key_pair",
        credential_namespace="backends.snowflake.key_pair",
    )

    with pytest.raises(DeclarationValidationError, match="exactly one"):
        auth_module.snowflake_auth_connect_kwargs(
            auth,
            resolver=MappingSecretResolver(
                {
                    "backends.snowflake.key_pair.user": "retl_user",
                    "backends.snowflake.key_pair.private_key": "pem",
                    "backends.snowflake.key_pair.private_key_path": "/tmp/snowflake_key.p8",
                }
            ),
        )


@requires_step3_backend
def test_snowflake_key_pair_auth_rejects_missing_private_key_source() -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]
    auth_module = importlib.import_module("retl.backends.snowflake.auth")
    auth = SnowflakeBackendAuth.from_namespace(
        auth_mode="key_pair",
        credential_namespace="backends.snowflake.key_pair",
    )

    with pytest.raises(DeclarationValidationError, match="private_key"):
        auth_module.snowflake_auth_connect_kwargs(
            auth,
            resolver=MappingSecretResolver(
                {
                    "backends.snowflake.key_pair.user": "retl_user",
                }
            ),
        )


@requires_step3_backend
def test_snowflake_key_pair_auth_normalizes_escaped_newlines_before_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snowflake_module = importlib.import_module("retl.backends.snowflake")
    SnowflakeBackendAuth = snowflake_module.SnowflakeBackendAuth  # type: ignore[attr-defined]
    auth_module = importlib.import_module("retl.backends.snowflake.auth")
    seen: dict[str, object] = {}

    def fake_private_key_der(private_key: str, *, passphrase: str | None) -> bytes:
        seen["private_key"] = private_key
        seen["passphrase"] = passphrase
        return b"parsed-key"

    monkeypatch.setattr(auth_module, "_private_key_der", fake_private_key_der)
    auth = SnowflakeBackendAuth.from_namespace(
        auth_mode="key_pair",
        credential_namespace="backends.snowflake.key_pair",
    )

    result = auth_module.snowflake_auth_connect_kwargs(
        auth,
        resolver=MappingSecretResolver(
            {
                "backends.snowflake.key_pair.user": "retl_user",
                "backends.snowflake.key_pair.private_key": "-----BEGIN KEY-----\\nabc\\n-----END KEY-----",
            }
        ),
    )

    assert result["private_key"] == b"parsed-key"
    assert seen == {
        "private_key": "-----BEGIN KEY-----\nabc\n-----END KEY-----",
        "passphrase": None,
    }


@requires_step4_connection
def test_snowflake_connection_adapter_translates_missing_optional_driver() -> None:
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeConnectionError = connection_module.SnowflakeConnectionError  # type: ignore[attr-defined]
    _snowflake_connector = connection_module._snowflake_connector  # type: ignore[attr-defined]

    def missing_snowflake_import(name: str) -> Any:
        if name == "snowflake.connector":
            raise ImportError("No module named snowflake.connector")
        raise AssertionError(name)

    with pytest.raises(SnowflakeConnectionError) as exc_info:
        _snowflake_connector(import_module=missing_snowflake_import)

    message = str(exc_info.value)
    assert "optional `snowflake` dependency" in message
    assert "retl[snowflake]" in message


@requires_step4_connection
def test_snowflake_connection_module_has_no_top_level_driver_import() -> None:
    snowflake_connection_module = importlib.import_module("retl.backends.snowflake.connection")
    assert snowflake_connection_module.__file__ is not None

    tree = ast.parse(Path(snowflake_connection_module.__file__).read_text())
    top_level_driver_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.Import | ast.ImportFrom)
        and (
            any(
                alias.name == "snowflake" or alias.name.startswith("snowflake.")
                for alias in getattr(node, "names", ())
            )
            or (
                getattr(node, "module", None) == "snowflake"
                or str(getattr(node, "module", "")).startswith("snowflake.")
            )
        )
    ]

    assert top_level_driver_imports == []


@requires_step4_connection
def test_snowflake_connection_wraps_cursor_execution_and_close_semantics() -> None:
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeConnection = connection_module.SnowflakeConnection  # type: ignore[attr-defined]

    raw_connection = _RecordingSnowflakeRawConnection()
    connection = SnowflakeConnection(connection=raw_connection, autocommit=False)

    first = connection.execute("select :1", [7])
    second = connection.executemany("insert into rows values (:1)", [[1], [2]])

    assert first.fetchone() == ("ok",)
    assert raw_connection.autocommit_values == [False]
    assert raw_connection.cursors[0].calls == [
        ("execute", "select :1", (7,), None),
    ]
    assert raw_connection.cursors[1].calls == [
        ("executemany", "insert into rows values (:1)", ((1,), (2,)), None),
    ]

    second.close()
    assert raw_connection.cursors[1].closed is True

    connection.close()
    connection.close()
    assert raw_connection.close_count == 1


@requires_step4_connection
def test_snowflake_connection_connects_lazily_with_redacted_repr_and_errors() -> None:
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeConnection = connection_module.SnowflakeConnection  # type: ignore[attr-defined]
    SnowflakeConnectionError = connection_module.SnowflakeConnectionError  # type: ignore[attr-defined]

    connector = _RecordingSnowflakeConnector()
    connection = SnowflakeConnection(
        account="xy12345.us-east-1",
        user="retl_user",
        password="secret-password",
        warehouse="RETL_WH",
        connect_kwargs={"private_key": "private-key"},
        connector=connector,
    )

    assert connector.paramstyle == "numeric"
    assert connector.connect_kwargs["password"] == "secret-password"
    assert connector.connect_kwargs["private_key"] == "private-key"
    assert "secret-password" not in repr(connection)
    assert "private-key" not in repr(connection)

    failing_connector = _RecordingSnowflakeConnector(error=RuntimeError("driver failure"))
    with pytest.raises(SnowflakeConnectionError) as exc_info:
        SnowflakeConnection(
            account="xy12345.us-east-1",
            user="retl_user",
            password="secret-password",
            connector=failing_connector,
        )

    assert "secret-password" not in str(exc_info.value)
    assert "<redacted>" in str(exc_info.value)


@requires_step4_connection
def test_snowflake_connection_result_exposes_arrow_reader() -> None:
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeConnection = connection_module.SnowflakeConnection  # type: ignore[attr-defined]

    raw_connection = _RecordingSnowflakeRawConnection()
    raw_connection.arrow_table = pa.table({"id": [1, 2]})

    reader = (
        SnowflakeConnection(connection=raw_connection)
        .execute("select id from rows")
        .to_arrow_reader(batch_size=1)
    )

    assert reader.schema.names == ["id"]
    assert reader.read_next_batch().num_rows == 1
    assert reader.read_next_batch().num_rows == 1


@requires_step4_connection
def test_snowflake_arrow_reader_coalesces_streamed_batches_without_fetch_arrow_all() -> None:
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeCursorResult = connection_module.SnowflakeCursorResult  # type: ignore[attr-defined]
    schema = pa.schema([pa.field("id", pa.int64())])
    batches = [
        pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], schema=schema),
        pa.RecordBatch.from_arrays([pa.array([3], type=pa.int64())], schema=schema),
    ]
    cursor = _StreamingSnowflakeCursor(chunks=batches)

    reader = SnowflakeCursorResult(cursor).to_arrow_reader(batch_size=10)

    assert reader.schema == schema
    assert reader.read_next_batch().column(0).to_pylist() == [1, 2, 3]
    with pytest.raises(StopIteration):
        reader.read_next_batch()
    assert cursor.fetch_arrow_batches_count == 1
    assert cursor.fetch_arrow_all_count == 0


@requires_step4_connection
def test_snowflake_arrow_reader_splits_streamed_table_chunks_by_batch_size() -> None:
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeCursorResult = connection_module.SnowflakeCursorResult  # type: ignore[attr-defined]
    cursor = _StreamingSnowflakeCursor(
        chunks=[
            pa.table(
                {
                    "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                    "name": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
                }
            )
        ]
    )

    reader = SnowflakeCursorResult(cursor).to_arrow_reader(batch_size=2)

    assert reader.schema.names == ["id", "name"]
    assert reader.read_next_batch().column(0).to_pylist() == [1, 2]
    assert reader.read_next_batch().column(0).to_pylist() == [3, 4]
    assert reader.read_next_batch().column(0).to_pylist() == [5]
    assert cursor.fetch_arrow_batches_count == 1
    assert cursor.fetch_arrow_all_count == 0


@requires_step4_connection
def test_snowflake_arrow_reader_splits_streamed_record_batches_by_batch_size() -> None:
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeCursorResult = connection_module.SnowflakeCursorResult  # type: ignore[attr-defined]
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]
    )
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
        ],
        schema=schema,
    )
    cursor = _StreamingSnowflakeCursor(chunks=[batch])

    reader = SnowflakeCursorResult(cursor).to_arrow_reader(batch_size=2)

    assert reader.schema == schema
    assert reader.read_next_batch().column(0).to_pylist() == [1, 2]
    assert reader.read_next_batch().column(0).to_pylist() == [3, 4]
    assert reader.read_next_batch().column(0).to_pylist() == [5]
    assert cursor.fetch_arrow_batches_count == 1
    assert cursor.fetch_arrow_all_count == 0


@requires_step4_connection
def test_snowflake_arrow_reader_empty_stream_uses_fetch_arrow_all_for_schema_only() -> None:
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeCursorResult = connection_module.SnowflakeCursorResult  # type: ignore[attr-defined]
    empty_table = pa.table({"id": pa.array([], type=pa.int64())})
    cursor = _StreamingSnowflakeCursor(chunks=[], arrow_all=empty_table)

    reader = SnowflakeCursorResult(cursor).to_arrow_reader(batch_size=2)

    with pytest.raises(StopIteration):
        reader.read_next_batch()
    assert reader.schema == empty_table.schema
    assert cursor.fetch_arrow_batches_count == 1
    assert cursor.fetch_arrow_all_count == 1


def test_snowflake_arrow_reader_empty_stream_uses_cursor_description_when_arrow_all_is_none() -> (
    None
):
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeCursorResult = connection_module.SnowflakeCursorResult  # type: ignore[attr-defined]

    class _NoneArrowAllCursor:
        description = (("COLLECT_SEQUENCE",), ("KEY_JSON",))

        def __init__(self) -> None:
            self.fetch_arrow_batches_count = 0
            self.fetch_arrow_all_count = 0

        def fetch_arrow_batches(self) -> object:
            self.fetch_arrow_batches_count += 1
            return
            yield

        def fetch_arrow_all(self) -> None:
            self.fetch_arrow_all_count += 1
            return None

    cursor = _NoneArrowAllCursor()

    reader = SnowflakeCursorResult(cursor).to_arrow_reader(batch_size=2)

    with pytest.raises(StopIteration):
        reader.read_next_batch()
    assert reader.schema.names == ["COLLECT_SEQUENCE", "KEY_JSON"]
    assert cursor.fetch_arrow_batches_count == 1
    assert cursor.fetch_arrow_all_count == 1


def test_snowflake_arrow_reader_empty_stream_uses_cursor_description_when_arrow_all_is_empty() -> (
    None
):
    connection_module = importlib.import_module("retl.backends.snowflake.connection")
    SnowflakeCursorResult = connection_module.SnowflakeCursorResult  # type: ignore[attr-defined]
    cursor = _StreamingSnowflakeCursor(
        chunks=[],
        arrow_all=pa.table({}),
        description=(("COLLECT_SEQUENCE",), ("KEY_JSON",)),
    )

    reader = SnowflakeCursorResult(cursor).to_arrow_reader(batch_size=2)

    with pytest.raises(StopIteration):
        reader.read_next_batch()
    assert reader.schema.names == ["COLLECT_SEQUENCE", "KEY_JSON"]
    assert cursor.fetch_arrow_batches_count == 1
    assert cursor.fetch_arrow_all_count == 1


def test_snowflake_driver_imports_are_confined_to_backend_package_or_live_sandbox() -> None:
    allowed_roots = {
        Path("src/retl/backends/snowflake"),
        Path("tests/backends/sandbox"),
    }
    violations: list[Path] = []

    for root in (_REPO_ROOT / "src", _REPO_ROOT / "tests"):
        for path in root.rglob("*.py"):
            relative = path.relative_to(_REPO_ROOT)
            if any(relative.is_relative_to(allowed) for allowed in allowed_roots):
                continue
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import) and any(
                    alias.name == "snowflake" or alias.name.startswith("snowflake.")
                    for alias in node.names
                ):
                    violations.append(relative)
                if isinstance(node, ast.ImportFrom) and (
                    node.module == "snowflake" or str(node.module).startswith("snowflake.")
                ):
                    violations.append(relative)

    assert violations == []


class _RecordingSnowflakeConnector:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.connect_kwargs: dict[str, object] = {}
        self.paramstyle: str | None = None

    def connect(self, **kwargs: object) -> "_RecordingSnowflakeRawConnection":
        self.connect_kwargs = kwargs
        if self.error is not None:
            raise self.error
        return _RecordingSnowflakeRawConnection()


class _RecordingSnowflakeRawConnection:
    def __init__(self) -> None:
        self.autocommit_values: list[bool] = []
        self.close_count = 0
        self.cursors: list[_RecordingSnowflakeCursor] = []
        self.arrow_table = pa.table({"id": []})

    def cursor(self) -> "_RecordingSnowflakeCursor":
        cursor = _RecordingSnowflakeCursor(arrow_table=self.arrow_table)
        self.cursors.append(cursor)
        return cursor

    def autocommit(self, enabled: bool) -> None:
        self.autocommit_values.append(enabled)

    def close(self) -> None:
        self.close_count += 1


class _RecordingSnowflakeCursor:
    def __init__(self, *, arrow_table: pa.Table) -> None:
        self.arrow_table = arrow_table
        self.calls: list[
            tuple[
                str,
                str,
                tuple[object, ...]
                | dict[str, object]
                | tuple[tuple[object, ...] | dict[str, object], ...],
                Mapping[str, object] | None,
            ]
        ] = []
        self.closed = False

    def execute(
        self,
        sql: str,
        *,
        params: tuple[object, ...] | dict[str, object],
        _statement_params: Mapping[str, object] | None = None,
    ) -> "_RecordingSnowflakeCursor":
        self.calls.append(("execute", sql, params, _statement_params))
        return self

    def executemany(
        self,
        sql: str,
        seqparams: Sequence[tuple[object, ...] | dict[str, object]],
    ) -> "_RecordingSnowflakeCursor":
        self.calls.append(("executemany", sql, tuple(seqparams), None))
        return self

    def fetchone(self) -> tuple[str]:
        return ("ok",)

    def fetch_arrow_all(self) -> pa.Table:
        return self.arrow_table

    def close(self) -> None:
        self.closed = True


class _StreamingSnowflakeCursor:
    def __init__(
        self,
        *,
        chunks: Sequence[pa.Table | pa.RecordBatch],
        arrow_all: pa.Table | None = None,
        description: Sequence[Sequence[object]] | None = None,
    ) -> None:
        self._chunks = chunks
        self._arrow_all = arrow_all
        self.description = tuple(description or ())
        self.fetch_arrow_batches_count = 0
        self.fetch_arrow_all_count = 0

    def fetch_arrow_batches(self) -> object:
        self.fetch_arrow_batches_count += 1
        yield from self._chunks

    def fetch_arrow_all(self) -> pa.Table:
        self.fetch_arrow_all_count += 1
        if self._arrow_all is not None:
            return self._arrow_all
        record_batches: list[pa.RecordBatch] = []
        for chunk in self._chunks:
            if isinstance(chunk, pa.Table):
                record_batches.extend(chunk.to_batches())
            else:
                record_batches.append(chunk)
        return pa.Table.from_batches(record_batches)
