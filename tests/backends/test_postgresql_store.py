from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence

import retl
from retl.auth import MappingSecretResolver
from retl.backends.postgresql import POSTGRESQL_DIALECT, PostgreSqlBackend, PostgreSqlConnection
from retl.backends.postgresql.auth import PostgreSqlBackendAuth
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.store import SqlRuntimeStore


def _postgresql_module_names() -> set[str]:
    return {name for name in sys.modules if name == "psycopg" or name.startswith("psycopg.")}


def _backend() -> PostgreSqlBackend:
    return PostgreSqlBackend(
        host="localhost",
        port=5432,
        database="app",
        source_schema="public",
        runtime_schema="retl",
        auth=PostgreSqlBackendAuth.from_namespace(
            auth_mode="password",
            credential_namespace="backends.postgresql.password",
        ),
    )


def test_postgresql_runtime_store_constructs_shared_context_from_injected_connection() -> None:
    before = _postgresql_module_names()
    backend = _backend()
    connection = RecordingSqlConnection()

    store = backend.runtime_store(connection=connection)
    try:
        assert isinstance(store, SqlRuntimeStore)
        assert type(store).begin_attempt is SqlRuntimeStore.begin_attempt

        context = store._runtime_context  # noqa: SLF001
        assert isinstance(context, SqlRuntimeContext)
        assert context.connection is connection
        assert context.dialect is POSTGRESQL_DIALECT
        assert context.sqlglot_dialect == "postgres"
        assert context.runtime_space == backend.runtime_space
        assert context.collect_placement == backend.placement
        assert store._next_attempt_number == 1  # noqa: SLF001
    finally:
        store.close()

    assert connection.close_count == 1
    assert _postgresql_module_names() == before
    assert connection.calls[0][0] == "create extension if not exists pgcrypto"
    assert connection.calls[1][0] == 'create schema if not exists "retl"'


def test_postgresql_runtime_store_can_open_through_injected_connector_fake() -> None:
    before = _postgresql_module_names()
    connector = RecordingPostgreSqlConnector()
    store = None
    retl.configure(
        secret_resolver=MappingSecretResolver(
            {
                "backends.postgresql.password.user": "retl_user",
                "backends.postgresql.password.password": "secret-password",
            }
        )
    )

    try:
        store = _backend().runtime_store(connector=connector)
        context = store._runtime_context  # noqa: SLF001
        assert isinstance(context, SqlRuntimeContext)
        assert isinstance(context.connection, PostgreSqlConnection)
        assert connector.connect_kwargs["user"] == "retl_user"
        assert connector.connect_kwargs["password"] == "secret-password"
        assert connector.connect_kwargs["dbname"] == "app"
        assert connector.connect_kwargs["sslmode"] == "require"
        assert connector.raw_connection.autocommit is True
        assert any(
            sql == 'create schema if not exists "retl"' for sql, _ in connector.raw_connection.calls
        )
    finally:
        if store is not None:
            store.close()
        retl.configure(secret_resolver=None)

    assert connector.raw_connection.close_count == 1
    assert _postgresql_module_names() == before


class RecordingSqlConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.close_count = 0

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] = (),
    ) -> RecordingSqlResult:
        self.calls.append((sql, tuple(parameters) if not isinstance(parameters, Mapping) else ()))
        return RecordingSqlResult(sql)

    def close(self) -> None:
        self.close_count += 1


class RecordingSqlResult:
    def __init__(self, sql: str) -> None:
        self.sql = sql

    def fetchone(self) -> tuple[str] | None:
        if self.sql == "select current_schema()":
            return ("retl",)
        return None

    def fetchall(self) -> list[tuple[str]]:
        return []


class RecordingPostgreSqlConnector:
    def __init__(self) -> None:
        self.raw_connection = RecordingRawPostgreSqlConnection()
        self.connect_kwargs: dict[str, object] = {}

    def connect(self, **kwargs: object) -> RecordingRawPostgreSqlConnection:
        self.connect_kwargs = kwargs
        return self.raw_connection


class RecordingRawPostgreSqlConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.close_count = 0
        self.autocommit: bool | None = None

    def cursor(self) -> RecordingPostgreSqlCursor:
        return RecordingPostgreSqlCursor(self)

    def commit(self) -> None:
        self.calls.append(("commit", ()))

    def rollback(self) -> None:
        self.calls.append(("rollback", ()))

    def close(self) -> None:
        self.close_count += 1


class RecordingPostgreSqlCursor:
    description: tuple[object, ...] = ()

    def __init__(self, connection: RecordingRawPostgreSqlConnection) -> None:
        self.connection = connection
        self.sql = ""

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] = (),
    ) -> None:
        self.sql = sql
        self.connection.calls.append(
            (sql, tuple(parameters) if not isinstance(parameters, Mapping) else ())
        )

    def fetchone(self) -> tuple[str] | None:
        if self.sql == "select current_schema()":
            return ("retl",)
        return None

    def fetchall(self) -> list[tuple[str]]:
        return []

    def close(self) -> None:
        return None
