from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence

import pytest

import retl
from retl.auth import MappingSecretResolver
from retl.backends.snowflake import SNOWFLAKE_DIALECT, SnowflakeConnection, SnowflakeSqlBackend
from retl.backends.snowflake.auth import SnowflakeBackendAuth
from retl.collect_identity import is_uuidv7
from retl.errors import DeclarationValidationError
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationProgressScope,
    EventKeysetScanPosition,
    EventSourceWindowHandle,
    StateSnapshotHandle,
)
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.destination_batches import compile_destination_batches_by_id_read
from retl.stores.sql_runtime.ordered_work import compile_pending_work_read
from retl.stores.sql_runtime.state_current import (
    compile_state_current_summary_read,
    compile_state_current_upserts_read,
)
from retl.stores.sql_runtime.store import SqlRuntimeStore


def _snowflake_module_names() -> set[str]:
    return {name for name in sys.modules if name == "snowflake" or name.startswith("snowflake.")}


def _backend() -> SnowflakeSqlBackend:
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


def _scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="sync",
        destination_name="warehouse",
        surface="customers",
        family="state",
        declaration_name="customer_state",
    )


def test_snowflake_runtime_store_constructs_shared_context_from_injected_connection() -> None:
    before = _snowflake_module_names()
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
        assert context.dialect is SNOWFLAKE_DIALECT
        assert context.sqlglot_dialect == "snowflake"
        assert context.runtime_space == backend.runtime_space
        assert context.collect_placement == backend.placement
        assert store._next_attempt_number == 1  # noqa: SLF001
    finally:
        store.close()

    assert connection.close_count == 1
    assert _snowflake_module_names() == before
    assert connection.calls[0][0] == 'create schema if not exists "RETL_DB"."RETL_RUNTIME"'


def test_snowflake_runtime_store_can_open_through_injected_connector_fake() -> None:
    before = _snowflake_module_names()
    connector = RecordingSnowflakeConnector()
    store = None
    retl.configure(
        secret_resolver=MappingSecretResolver(
            {
                "backends.snowflake.password.user": "retl_user",
                "backends.snowflake.password.password": "secret-password",
                "backends.snowflake.password.role": "RETL_ROLE",
            }
        )
    )

    try:
        store = _backend().runtime_store(connector=connector)
        context = store._runtime_context  # noqa: SLF001
        assert isinstance(context, SqlRuntimeContext)
        assert isinstance(context.connection, SnowflakeConnection)
        assert connector.paramstyle == "numeric"
        assert connector.connect_kwargs["user"] == "retl_user"
        assert connector.connect_kwargs["password"] == "secret-password"
        assert connector.connect_kwargs["role"] == "RETL_ROLE"
        assert connector.connect_kwargs["database"] == "RETL_DB"
        assert connector.connect_kwargs["schema"] == "RETL_RUNTIME"
        assert connector.connect_kwargs["autocommit"] is True
        assert any(
            sql == 'create schema if not exists "RETL_DB"."RETL_RUNTIME"'
            for sql, _ in connector.raw_connection.calls
        )
    finally:
        if store is not None:
            store.close()
        retl.configure(secret_resolver=None)

    assert connector.raw_connection.close_count == 1
    assert _snowflake_module_names() == before


def test_snowflake_runtime_store_reports_missing_required_secret_without_value() -> None:
    connector = RecordingSnowflakeConnector()
    retl.configure(
        secret_resolver=MappingSecretResolver(
            {
                "backends.snowflake.password.user": "retl_user",
            }
        )
    )

    try:
        with pytest.raises(DeclarationValidationError) as exc_info:
            _backend().runtime_store(connector=connector)
    finally:
        retl.configure(secret_resolver=None)

    message = str(exc_info.value)
    assert "backends.snowflake.password.password" in message
    assert "retl_user" not in message
    assert connector.connect_kwargs == {}


def test_snowflake_state_collect_records_executable_runtime_sql_with_source_schema_context() -> (
    None
):
    connection = CollectRecordingSqlConnection()
    backend = _backend()
    declaration = retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="select customer_id, email, plan, audience_key from customers",
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    snapshot = StateSnapshotHandle(
        backend="snowflake",
        source_name="customers",
        source_identity={"backend": "snowflake", "database": "SOURCE_DB", "schema": "APP"},
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
    assert ("begin", ()) in calls
    assert ("commit", ()) in calls
    assert any(sql == 'use schema "SOURCE_DB"."APP"' for sql, _ in calls)
    assert any(sql == 'use schema "RETL_DB"."RETL_RUNTIME"' for sql, _ in calls)
    temp_create = next(
        sql for sql, _ in calls if 'create temporary table "retl_state_collect_snapshot" as' in sql
    )
    assert "from (select customer_id, email, plan, audience_key from customers) as source_rows" in (
        " ".join(temp_create.split())
    )
    assert '"SOURCE_DB"."APP".' not in temp_create
    assert "object_construct_keep_null" in temp_create
    assert "to_json(" in temp_create

    runtime_writes = [
        (sql, params)
        for sql, params in calls
        if sql.lstrip().startswith(("merge into", "insert into", "delete from"))
    ]
    assert runtime_writes
    for sql, _ in runtime_writes:
        assert '"RETL_DB"."RETL_RUNTIME".' in sql
        assert '"SOURCE_DB"."APP".' not in sql
    assert any(":1 as declaration_name" in sql for sql, _ in runtime_writes)
    assert any(
        isinstance(params, tuple)
        and len(params) >= 4
        and params[0] == "customer_state"
        and params[2] == "customers"
        and str(params[3]).startswith('{"backend":"snowflake"')
        for _, params in runtime_writes
    )


def test_snowflake_event_collect_records_numeric_keyset_progression_across_windows() -> None:
    connection = CollectRecordingSqlConnection()
    backend = _backend()
    declaration = retl.event(
        name="purchase_event",
        source=retl.source(
            name="purchases",
            query="select purchase_id, email, occurred_at, amount from purchases",
            mode="checkpointed",
            checkpoint={
                "cursor": "occurred_at",
                "primary_key": "purchase_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
            backend=backend.source_backend(),
        ),
        key={"purchase": "purchase_id"},
        occurred_at="occurred_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"amount": "amount"},
    )
    scan_after = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-02T03:04:05"),
        primary_key_value=CanonicalKeyScalar.string("purchase_9"),
    )
    window = EventSourceWindowHandle(
        backend="snowflake",
        source_name="purchases",
        source_identity={"backend": "snowflake", "database": "SOURCE_DB", "schema": "APP"},
        query=declaration.source.query,
        cursor_column="occurred_at",
        primary_key_column="purchase_id",
        scan_after=scan_after,
        source_space=backend.source_space,
        limit=25,
    )

    store = backend.runtime_store(connection=connection)
    try:
        result = store.produce_event_collect(declaration=declaration, window=window)
    finally:
        store.close()

    assert is_uuidv7(result.collect_id)
    assert result.scan_after == scan_after
    assert result.scan_upper_bound == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-03T00:00:00"),
        primary_key_value=CanonicalKeyScalar.string("purchase_10"),
    )
    assert result.window_row_count == 2
    assert result.work_row_count == 0

    temp_call = next(
        (sql, params)
        for sql, params in connection.calls
        if 'create temporary table "retl_event_collect_window" as' in sql
    )
    temp_sql, temp_params = temp_call
    assert '"occurred_at" > :1' in temp_sql
    assert '"occurred_at" = :2' in temp_sql
    assert '"purchase_id" > :3' in temp_sql
    assert "LIMIT :4" in temp_sql
    assert temp_params == ("2026-01-02T03:04:05", "2026-01-02T03:04:05", "purchase_9", 25)
    assert 'ORDER BY "occurred_at" ASC, "purchase_id" ASC' in temp_sql

    assert not any(
        'insert into "RETL_DB"."RETL_RUNTIME"."ordered_work"' in sql and "'event'" in sql.lower()
        for sql, _ in connection.calls
    )


def test_snowflake_runtime_store_uses_shared_runtime_sql_validation() -> None:
    store = _backend().runtime_store(connection=RecordingSqlConnection())
    try:
        with pytest.raises(DeclarationValidationError, match="Run registration"):
            store.register_run(object())
    finally:
        store.close()


def test_snowflake_after_collect_read_sql_is_qualified_to_runtime_space_only() -> None:
    store = _backend().runtime_store(connection=RecordingSqlConnection())
    try:
        context = store._runtime_context  # noqa: SLF001
        assert isinstance(context, SqlRuntimeContext)

        compiled = [
            compile_state_current_summary_read(
                context,
                declaration_name="customer_state",
                source_name="customers",
            ),
            compile_state_current_upserts_read(
                context,
                declaration_name="customer_state",
                source_name="customers",
                lower_identity=None,
                limit=101,
            ),
            compile_pending_work_read(
                context,
                scope=_scope(),
                lower_bound=None,
                source_collect_id=None,
                limit=101,
            ),
            compile_destination_batches_by_id_read(context, batch_ids=("batch-1",)),
        ]
    finally:
        store.close()

    for query in compiled:
        assert '"RETL_DB"."RETL_RUNTIME".' in query.sql
        assert '"SOURCE_DB"."APP".' not in query.sql
        assert ":1" in query.sql


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
        if "select attempt_id" in self._last_sql:
            return ()
        return ()

    def fetchone(self) -> tuple[object, ...] | None:
        normalized = " ".join(self._last_sql.lower().split())
        if normalized == "select current_database(), current_schema()":
            return ("RETL_DB", "RETL_RUNTIME")
        if "group by identity_json having count(*) > 1" in normalized:
            return None
        if (
            'from "retl_event_collect_window"' in normalized
            and 'order by "retl_cursor" desc' in normalized
        ):
            return ("2026-01-03T00:00:00", "purchase_10")
        if 'from "retl_state_collect_snapshot"' in normalized and "select count(*)" in normalized:
            return (2,)
        if 'from "retl_event_collect_window"' in normalized and "select count(*)" in normalized:
            return (2,)
        if (
            'select count(*) from "retl_db"."retl_runtime"."ordered_work"' in normalized
            and isinstance(self._last_params, tuple)
            and self._last_params[-3:] == ("state", "upsert", "customer_state")
        ):
            return (2,)
        if (
            'select count(*) from "retl_db"."retl_runtime"."ordered_work"' in normalized
            and isinstance(self._last_params, tuple)
            and self._last_params[-3:] == ("state", "remove", "customer_state")
        ):
            return (0,)
        if (
            'select count(*) from "retl_db"."retl_runtime"."ordered_work"' in normalized
            and isinstance(self._last_params, tuple)
            and self._last_params[-3:] == ("event", "import", "purchase_event")
        ):
            return (2,)
        if "family = 'state'" in normalized and "kind = 'upsert'" in normalized:
            return (2,)
        if "family = 'state'" in normalized and "kind = 'remove'" in normalized:
            return (0,)
        if "family = 'event'" in normalized and "kind = 'import'" in normalized:
            return (2,)
        if 'select count(*) from "retl_db"."retl_runtime"."ordered_work"' in normalized:
            return (0,)
        return None


class RecordingSnowflakeConnector:
    def __init__(self) -> None:
        self.connect_kwargs: dict[str, object] = {}
        self.paramstyle: str | None = None
        self.raw_connection = RecordingSnowflakeRawConnection()

    def connect(self, **kwargs: object) -> "RecordingSnowflakeRawConnection":
        self.connect_kwargs = kwargs
        return self.raw_connection


class RecordingSnowflakeRawConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | dict[str, object]]] = []
        self.close_count = 0

    def cursor(self) -> "RecordingSnowflakeCursor":
        return RecordingSnowflakeCursor(self)

    def autocommit(self, enabled: bool) -> None:
        self.calls.append(("autocommit", (enabled,)))

    def close(self) -> None:
        self.close_count += 1


class RecordingSnowflakeCursor:
    def __init__(self, connection: RecordingSnowflakeRawConnection) -> None:
        self.connection = connection
        self.closed = False

    def execute(
        self,
        sql: str,
        *,
        params: Sequence[object] | Mapping[str, object],
        _statement_params: Mapping[str, object] | None = None,
    ) -> "RecordingSnowflakeCursor":
        _ = _statement_params
        normalized = dict(params) if isinstance(params, Mapping) else tuple(params)
        self.connection.calls.append((sql, normalized))
        return self

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return ()

    def fetchone(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True
