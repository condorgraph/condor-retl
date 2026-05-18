from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence

import pytest

import retl
from retl.auth import MappingSecretResolver
from retl.backends.databricks import DATABRICKS_DIALECT, DatabricksConnection, DatabricksSqlBackend
from retl.backends.databricks.auth import DatabricksBackendAuth
from retl.collect_identity import is_uuidv7
from retl.errors import DeclarationValidationError
from retl.stores.contracts import StateSnapshotHandle
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.store import SqlRuntimeStore


def _databricks_module_names() -> set[str]:
    return {name for name in sys.modules if name == "databricks" or name.startswith("databricks.")}


def _backend() -> DatabricksSqlBackend:
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


def test_databricks_runtime_store_constructs_shared_context_from_injected_connection() -> None:
    before = _databricks_module_names()
    backend = _backend()
    connection = RecordingSqlConnection()

    store = backend.runtime_store(connection=connection)
    try:
        assert isinstance(store, SqlRuntimeStore)
        assert type(store).begin_attempt is SqlRuntimeStore.begin_attempt
        assert type(store).produce_state_collect is SqlRuntimeStore.produce_state_collect

        context = store._runtime_context  # noqa: SLF001
        assert isinstance(context, SqlRuntimeContext)
        assert context.connection is connection
        assert context.dialect is DATABRICKS_DIALECT
        assert context.sqlglot_dialect == "databricks"
        assert context.runtime_space == backend.runtime_space
        assert context.collect_placement == backend.placement
        assert store._next_attempt_number == 1  # noqa: SLF001
    finally:
        store.close()

    assert connection.close_count == 1
    assert _databricks_module_names() == before
    assert connection.calls[0][0] == (
        "create schema if not exists `runtime_catalog`.`runtime_schema`"
    )
    assert all("USE " not in sql.upper() for sql, _ in connection.calls)


def test_databricks_runtime_store_can_open_through_injected_connector_fake() -> None:
    before = _databricks_module_names()
    connector = RecordingDatabricksConnector()
    store = None
    retl.configure(
        secret_resolver=MappingSecretResolver(
            {
                "backends.databricks.pat.token": "secret-token",
            }
        )
    )

    try:
        store = _backend().runtime_store(connector=connector)
        context = store._runtime_context  # noqa: SLF001
        assert isinstance(context, SqlRuntimeContext)
        assert isinstance(context.connection, DatabricksConnection)
        assert connector.connect_kwargs["access_token"] == "secret-token"
        assert connector.connect_kwargs["server_hostname"] == (
            "dbc-a1b2345c-d6e7.cloud.databricks.com"
        )
        assert connector.connect_kwargs["http_path"] == "/sql/1.0/warehouses/a1b234c567d8e9fa"
        assert connector.connect_kwargs["catalog"] == "runtime_catalog"
        assert connector.connect_kwargs["schema"] == "runtime_schema"
        assert connector.raw_connection.autocommit is False
    finally:
        if store is not None:
            store.close()
        retl.configure(secret_resolver=None)

    assert connector.raw_connection.close_count == 1
    assert _databricks_module_names() == before


def test_databricks_runtime_store_reports_missing_required_secret_without_value() -> None:
    connector = RecordingDatabricksConnector()
    retl.configure(secret_resolver=MappingSecretResolver({}))

    try:
        with pytest.raises(DeclarationValidationError) as exc_info:
            _backend().runtime_store(connector=connector)
    finally:
        retl.configure(secret_resolver=None)

    message = str(exc_info.value)
    assert "backends.databricks.pat.token" in message
    assert "secret-token" not in message
    assert connector.connect_kwargs == {}


def test_databricks_state_collect_uses_session_temp_table_and_fully_qualified_runtime() -> None:
    connection = CollectRecordingSqlConnection()
    backend = _backend()
    declaration = retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query=(
                "select customer_id, email, plan, audience_key "
                "from `source_catalog`.`source_schema`.`customers`"
            ),
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    snapshot = StateSnapshotHandle(
        backend="databricks",
        source_name="customers",
        source_identity={"backend": "databricks", "catalog": "source_catalog"},
        query=declaration.source.query,
        source_space=backend.source_space,
    )

    store = backend.runtime_store(connection=connection)
    try:
        result = store.produce_state_collect(declaration=declaration, snapshot=snapshot)
    finally:
        store.close()

    assert is_uuidv7(result.collect_id)
    assert result.current_row_count == 2
    assert result.upsert_count == 2
    assert result.remove_count == 0

    calls = connection.calls
    assert ("begin transaction", ()) in calls
    assert ("commit", ()) in calls
    assert all("USE " not in sql.upper() for sql, _ in calls)
    temp_create = next(
        sql for sql, _ in calls if "create temporary table `retl_state_collect_snapshot` as" in sql
    )
    assert "`source_catalog`.`source_schema`.`customers`" in temp_create
    assert "named_struct" in temp_create
    assert "to_json(" in temp_create

    runtime_writes = [
        (sql, params)
        for sql, params in calls
        if sql.lstrip().lower().startswith(("merge into", "insert into", "delete from"))
    ]
    assert runtime_writes
    for sql, _ in runtime_writes:
        assert "`runtime_catalog`.`runtime_schema`" in sql
    assert any("? as declaration_name" in sql.lower() for sql, _ in runtime_writes)


class RecordingSqlConnection:
    def __init__(self, *, fetchall_rows: Sequence[tuple[object, ...]] = ()) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | dict[str, object]]] = []
        self.fetchall_rows = tuple(fetchall_rows)
        self.close_count = 0

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] = (),
    ) -> "RecordingSqlConnection":
        params: tuple[object, ...] | dict[str, object]
        params = dict(parameters) if isinstance(parameters, Mapping) else tuple(parameters)
        self.calls.append((sql, params))
        return self

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self.fetchall_rows

    def fetchone(self) -> tuple[object, ...] | None:
        return None

    def close(self) -> None:
        self.close_count += 1


class CollectRecordingSqlConnection(RecordingSqlConnection):
    def __init__(self) -> None:
        super().__init__()
        self._last_sql = ""
        self._last_params: tuple[object, ...] | dict[str, object] = ()

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] = (),
    ) -> "CollectRecordingSqlConnection":
        super().execute(sql, parameters)
        self._last_sql = sql
        self._last_params = (
            dict(parameters) if isinstance(parameters, Mapping) else tuple(parameters)
        )
        return self

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        if "select attempt_id" in self._last_sql.lower():
            return ()
        return ()

    def fetchone(self) -> tuple[object, ...] | None:
        normalized = " ".join(self._last_sql.lower().split())
        if "group by identity_json having count(*) > 1" in normalized:
            return None
        if "from `retl_state_collect_snapshot`" in normalized and "select count(*)" in normalized:
            return (2,)
        if (
            "select count(*) from `runtime_catalog`.`runtime_schema`.`ordered_work`" in normalized
            and isinstance(self._last_params, tuple)
            and self._last_params[-3:] == ("state", "upsert", "customer_state")
        ):
            return (2,)
        if (
            "select count(*) from `runtime_catalog`.`runtime_schema`.`ordered_work`" in normalized
            and isinstance(self._last_params, tuple)
            and self._last_params[-3:] == ("state", "remove", "customer_state")
        ):
            return (0,)
        if "select count(*) from `runtime_catalog`.`runtime_schema`.`ordered_work`" in normalized:
            return (0,)
        return None


class RecordingDatabricksConnector:
    def __init__(self) -> None:
        self.connect_kwargs: dict[str, object] = {}
        self.raw_connection = RecordingDatabricksRawConnection()

    def connect(self, **kwargs: object) -> "RecordingDatabricksRawConnection":
        self.connect_kwargs = kwargs
        return self.raw_connection


class RecordingDatabricksRawConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[object] | dict[str, object]]] = []
        self.close_count = 0
        self.autocommit: bool | None = None

    def cursor(self) -> "RecordingDatabricksCursor":
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
