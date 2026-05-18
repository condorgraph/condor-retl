from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

import pytest

import retl
from retl.auth import AuthResolutionError, EnvironmentSecretResolver
from retl.backends.snowflake import (
    SnowflakeBackendAuth,
    SnowflakeConnection,
    SnowflakeConnectionError,
    SnowflakeSqlBackend,
)
from retl.backends.snowflake.auth import snowflake_auth_connect_kwargs
from retl.destinations.targets import RemoteTarget, TargetRegistryKey, TargetRegistryRecord
from retl.runtime.provenance import RunProvenance
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    EventKeysetScanPosition,
    EventSourceWindowHandle,
    StateOrderedWorkScanPosition,
    StateSnapshotHandle,
    WorkFamily,
    destination_batch_id,
)
from retl.stores.sql_runtime.schema import RUNTIME_TABLE_CATALOG
from tests.backends.sandbox.runtime_cleanup_helpers import (
    assert_event_keyset_skip_operation,
    assert_runtime_cleanup_operations,
    assert_runtime_inspect_reset_operations,
)

pytestmark = pytest.mark.live_sandbox

_SIMPLE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_OPTIONAL_DEPENDENCY_MESSAGE = "optional `snowflake` dependency"


@dataclass(frozen=True)
class _LiveSnowflakeConfig:
    account: str
    warehouse: str
    source_database: str
    runtime_database: str
    auth: SnowflakeBackendAuth
    source_schema: str
    runtime_schema: str


@dataclass(frozen=True)
class _LiveSnowflakeSandbox:
    config: _LiveSnowflakeConfig
    backend: SnowflakeSqlBackend
    admin_connection: SnowflakeConnection


@dataclass(frozen=True)
class _ReportRef:
    ref: str


@dataclass(frozen=True)
class _ReportPhase:
    status: str = "succeeded"


@dataclass(frozen=True)
class _ReportDestination:
    attempted_count: int = 2
    confirmed_count: int = 2
    accepted_count: int = 2
    retryable_failure_count: int = 0
    terminal_failure_count: int = 0
    pre_acceptance_failure_count: int = 0
    failure_category: str | None = None
    http_status: int | None = None
    last_error_summary: str | None = None
    last_error_detail: str | None = None


@dataclass(frozen=True)
class _ReportCommit:
    progress_advanced: bool = True


@dataclass(frozen=True)
class _SyncReportRecord:
    run_id: str
    attempt_id: str
    sync_name: str
    ref: _ReportRef = _ReportRef("sync-report:snowflake")
    runner_name: str = "runner"
    declaration_name: str = "customer_state"
    declaration_version_id: str = "decl:customer_state"
    declaration_kind: str = "state"
    destination_binding_name: str = "warehouse"
    surface: str = "customers"
    dry_run: bool = False
    destination: _ReportDestination = _ReportDestination()
    commit: _ReportCommit = _ReportCommit()
    phases: tuple[_ReportPhase, ...] = (_ReportPhase(),)
    status: str = "succeeded"

    @property
    def report_id(self) -> str:
        return f"{self.run_id}:{self.attempt_id}:{self.sync_name}"

    def to_dict(self) -> dict[str, object]:
        return {
            "report_id": self.report_id,
            "attempt_id": self.attempt_id,
            "run_id": self.run_id,
            "sync_name": self.sync_name,
            "surface": self.surface,
        }


@pytest.fixture(scope="module")
def live_snowflake_sandbox() -> Iterator[_LiveSnowflakeSandbox]:
    config = _live_snowflake_config()
    admin_connection = _open_admin_connection(config)
    try:
        _execute(
            admin_connection,
            f"create schema if not exists {_schema(config.source_database, config.source_schema)}",
        )
        _execute(
            admin_connection,
            f"create schema if not exists {_schema(config.runtime_database, config.runtime_schema)}",
        )
        backend = SnowflakeSqlBackend(
            account=config.account,
            warehouse=config.warehouse,
            source_database=config.source_database,
            source_schema=config.source_schema,
            runtime_database=config.runtime_database,
            runtime_schema=config.runtime_schema,
            auth=config.auth,
        )
        _create_source_tables(admin_connection, config)
        yield _LiveSnowflakeSandbox(
            config=config,
            backend=backend,
            admin_connection=admin_connection,
        )
    finally:
        _drop_schema(admin_connection, config.source_database, config.source_schema)
        _drop_schema(admin_connection, config.runtime_database, config.runtime_schema)
        admin_connection.close()


def test_snowflake_live_sandbox_schema_collect_and_current_limitations(
    live_snowflake_sandbox: _LiveSnowflakeSandbox,
) -> None:
    sandbox = live_snowflake_sandbox
    store = sandbox.backend.runtime_store()
    try:
        _assert_runtime_schema_initialized(sandbox.admin_connection, sandbox.config)

        state_result = store.produce_state_collect(
            declaration=_state_declaration(sandbox.backend),
            snapshot=StateSnapshotHandle(
                backend="snowflake",
                source_name="customers",
                source_identity=_source_identity(sandbox.config),
                query='select "customer_id", "email", "plan", "audience_key" from "customers"',
                source_space=sandbox.backend.source_space,
            ),
        )
        assert state_result.current_row_count == 2
        assert state_result.upsert_count == 2
        assert state_result.remove_count == 0
        assert state_result.work_row_count == 2

        first_event_result = store.produce_event_collect(
            declaration=_event_declaration(sandbox.backend),
            window=EventSourceWindowHandle(
                backend="snowflake",
                source_name="purchases",
                source_identity=_source_identity(sandbox.config),
                query='select "purchase_id", "email", "occurred_at", "amount" from "purchases"',
                cursor_column="occurred_at",
                primary_key_column="purchase_id",
                source_space=sandbox.backend.source_space,
                limit=2,
            ),
        )
        assert first_event_result.window_row_count == 2
        assert first_event_result.work_row_count == 0
        assert first_event_result.scan_upper_bound == EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-02T00:00:00"),
            primary_key_value=CanonicalKeyScalar.string("purchase_2"),
        )

        second_event_result = store.produce_event_collect(
            declaration=_event_declaration(sandbox.backend),
            window=EventSourceWindowHandle(
                backend="snowflake",
                source_name="purchases",
                source_identity=_source_identity(sandbox.config),
                query='select "purchase_id", "email", "occurred_at", "amount" from "purchases"',
                cursor_column="occurred_at",
                primary_key_column="purchase_id",
                scan_after=first_event_result.scan_upper_bound,
                source_space=sandbox.backend.source_space,
                limit=2,
            ),
        )
        assert second_event_result.window_row_count == 1
        assert second_event_result.work_row_count == 0
        assert second_event_result.scan_after == first_event_result.scan_upper_bound
        assert second_event_result.scan_upper_bound == EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-03T00:00:00"),
            primary_key_value=CanonicalKeyScalar.string("purchase_3"),
        )

        _assert_collect_rows_written(sandbox.admin_connection, sandbox.config)
        _assert_declaration_provenance_written(sandbox.admin_connection, sandbox.config)
        _assert_state_current_summary_readable(store, state_result.collect_id)
        _assert_state_current_upserts_readable(store, state_result.collect_id)
        _assert_pending_work_readable(store, state_result.collect_id)
        _assert_progress_retention_and_cleanup(store, state_result.collect_id)
        _assert_reporting_and_target_registry(sandbox.admin_connection, sandbox.config, store)
        _assert_destination_batch_current_state_rows(
            sandbox.admin_connection,
            sandbox.config,
            store,
        )
        _assert_destination_batch_ledger(store)
        assert_event_keyset_skip_operation(store)
        assert_runtime_cleanup_operations(store)
        assert_runtime_inspect_reset_operations(store)
    finally:
        store.close()


def _live_snowflake_config() -> _LiveSnowflakeConfig:
    required = {
        "BACKENDS__SNOWFLAKE__ACCOUNT": os.environ.get("BACKENDS__SNOWFLAKE__ACCOUNT"),
        "BACKENDS__SNOWFLAKE__WAREHOUSE": os.environ.get("BACKENDS__SNOWFLAKE__WAREHOUSE"),
        "BACKENDS__SNOWFLAKE__SOURCE_DATABASE": os.environ.get(
            "BACKENDS__SNOWFLAKE__SOURCE_DATABASE"
        ),
        "BACKENDS__SNOWFLAKE__RUNTIME_DATABASE": os.environ.get(
            "BACKENDS__SNOWFLAKE__RUNTIME_DATABASE"
        ),
    }
    missing = sorted(name for name, value in required.items() if not value)
    if missing:
        pytest.skip("Snowflake live sandbox config is absent; missing " + ", ".join(missing))

    auth_mode = os.environ.get("BACKENDS__SNOWFLAKE__AUTH_MODE", "password").strip() or "password"
    credential_namespace = f"backends.snowflake.{auth_mode}"
    auth = SnowflakeBackendAuth.from_namespace(
        auth_mode=auth_mode,
        credential_namespace=credential_namespace,
    )
    try:
        snowflake_auth_connect_kwargs(auth, resolver=EnvironmentSecretResolver())
    except AuthResolutionError as exc:
        pytest.skip(f"Snowflake live sandbox authentication is absent; {exc}")

    source_database = required["BACKENDS__SNOWFLAKE__SOURCE_DATABASE"]
    runtime_database = required["BACKENDS__SNOWFLAKE__RUNTIME_DATABASE"]
    assert source_database is not None
    assert runtime_database is not None

    prefix = os.environ.get("RETL_SNOWFLAKE_SANDBOX_SCHEMA_PREFIX", "RETL_LIVE")
    suffix = uuid.uuid4().hex[:10].upper()
    worker = re.sub(r"[^A-Za-z0-9_]", "_", os.environ.get("PYTEST_XDIST_WORKER", "gw0")).upper()
    source_schema = _test_identifier(prefix, worker, suffix, "SRC")
    runtime_schema = _test_identifier(prefix, worker, suffix, "RT")

    return _LiveSnowflakeConfig(
        account=str(required["BACKENDS__SNOWFLAKE__ACCOUNT"]).strip(),
        warehouse=_required_identifier(
            str(required["BACKENDS__SNOWFLAKE__WAREHOUSE"]),
            "warehouse",
        ),
        source_database=_required_identifier(source_database, "source database"),
        runtime_database=_required_identifier(runtime_database, "runtime database"),
        auth=auth,
        source_schema=source_schema,
        runtime_schema=runtime_schema,
    )


def _open_admin_connection(config: _LiveSnowflakeConfig) -> SnowflakeConnection:
    try:
        return SnowflakeConnection(
            account=config.account,
            warehouse=config.warehouse,
            database=config.source_database,
            connect_kwargs=snowflake_auth_connect_kwargs(
                config.auth,
                resolver=EnvironmentSecretResolver(),
            ),
        )
    except SnowflakeConnectionError as exc:
        if _OPTIONAL_DEPENDENCY_MESSAGE in str(exc):
            pytest.skip("Snowflake optional dependency is not installed.")
        raise


def _create_source_tables(
    connection: SnowflakeConnection,
    config: _LiveSnowflakeConfig,
) -> None:
    customers = _relation(config.source_database, config.source_schema, "customers")
    purchases = _relation(config.source_database, config.source_schema, "purchases")
    _execute(
        connection,
        f"""
        create table {customers} (
            "customer_id" string,
            "email" string,
            "plan" string,
            "audience_key" string
        )
        """,
    )
    _executemany(
        connection,
        f"insert into {customers} values (:1, :2, :3, :4)",
        (
            ("cust_1", "alpha@example.com", "free", "audience_1"),
            ("cust_2", "bravo@example.com", "pro", "audience_2"),
        ),
    )
    _execute(
        connection,
        f"""
        create table {purchases} (
            "purchase_id" string,
            "email" string,
            "occurred_at" string,
            "amount" number(10, 2)
        )
        """,
    )
    _executemany(
        connection,
        f"insert into {purchases} values (:1, :2, :3, :4)",
        (
            ("purchase_1", "alpha@example.com", "2026-01-01T00:00:00", 10.25),
            ("purchase_2", "bravo@example.com", "2026-01-02T00:00:00", 20.50),
            ("purchase_3", "alpha@example.com", "2026-01-03T00:00:00", 30.75),
        ),
    )


def _state_declaration(backend: SnowflakeSqlBackend) -> object:
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query='select "customer_id", "email", "plan", "audience_key" from "customers"',
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=({"type": "email", "value": "email"},),
        payload={"plan": "plan"},
    )


def _event_declaration(backend: SnowflakeSqlBackend) -> object:
    return retl.event(
        name="purchase_event",
        source=retl.source(
            name="purchases",
            query='select "purchase_id", "email", "occurred_at", "amount" from "purchases"',
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
        identifiers=({"type": "email", "value": "email"},),
        payload={"amount": "amount"},
    )


def _assert_runtime_schema_initialized(
    connection: SnowflakeConnection,
    config: _LiveSnowflakeConfig,
) -> None:
    rows = _execute(
        connection,
        """
        select table_name
        from information_schema.tables
        where table_catalog = :1
          and table_schema = :2
        """,
        (config.runtime_database.upper(), config.runtime_schema.upper()),
    ).fetchall()
    created_tables = {str(row[0]).casefold() for row in rows}
    expected_tables = {name.casefold() for name in RUNTIME_TABLE_CATALOG}
    assert expected_tables <= created_tables
    assert {
        "run_indexes",
        "destination_submission_attempts",
        "destination_batch_attempts",
    }.isdisjoint(created_tables)


def _assert_collect_rows_written(
    connection: SnowflakeConnection,
    config: _LiveSnowflakeConfig,
) -> None:
    ordered_work_count = _execute(
        connection,
        f"""
        select count(*)
        from {_relation(config.runtime_database, config.runtime_schema, "ordered_work")}
        where declaration_name in (:1, :2)
        """,
        ("customer_state", "purchase_event"),
    ).fetchone()
    assert ordered_work_count is not None
    assert int(ordered_work_count[0]) == 2

    state_current_count = _execute(
        connection,
        f"""
        select count(*)
        from {_relation(config.runtime_database, config.runtime_schema, "state_current")}
        where declaration_name = :1
          and source_name = :2
        """,
        ("customer_state", "customers"),
    ).fetchone()
    assert state_current_count is not None
    assert int(state_current_count[0]) == 2


def _assert_declaration_provenance_written(
    connection: SnowflakeConnection,
    config: _LiveSnowflakeConfig,
) -> None:
    declarations = _execute(
        connection,
        f"""
        select declaration_name, source_backend, source_location_json
        from {_relation(config.runtime_database, config.runtime_schema, "declarations")}
        where declaration_name in (:1, :2)
        order by declaration_name
        """,
        ("customer_state", "purchase_event"),
    ).fetchall()
    assert [row[0] for row in declarations] == ["customer_state", "purchase_event"]
    for _, source_backend, source_location_json in declarations:
        assert source_backend == "snowflake"
        location = json.loads(source_location_json)
        assert location["backend_name"] == "snowflake"
        assert location["database"] == config.source_database
        assert location["schema"] == config.source_schema


def _assert_reporting_and_target_registry(
    connection: SnowflakeConnection,
    config: _LiveSnowflakeConfig,
    store: object,
) -> None:
    run = RunProvenance(
        run_id="run-snowflake-live",
        runner_name="runner",
        dry_run=False,
        script_path=None,
        script_content_hash=None,
        started_at="2026-05-09T00:00:00+00:00",
    )
    store.register_run(run)  # type: ignore[attr-defined]
    store.complete_run(run_id=run.run_id, status="succeeded")  # type: ignore[attr-defined]

    key = TargetRegistryKey(
        binding_name="warehouse",
        destination_ref="retl/snowflake-live",
        surface="customers",
        logical_target="audience_1",
    )
    record = TargetRegistryRecord(
        key=key,
        remote=RemoteTarget(
            remote_id="remote-audience-1",
            display_name="Audience 1",
            metadata={"account_id": "act_123", "retention_days": 90},
        ),
        source="managed_created",
    )
    store.put(record)  # type: ignore[attr-defined]
    assert store.get(key) == record  # type: ignore[attr-defined]

    report = _SyncReportRecord(
        run_id=run.run_id,
        attempt_id="attempt-snowflake-live",
        sync_name="customer_sync",
    )
    store.record_sync_report(report)  # type: ignore[attr-defined]
    _commit_store(store)

    run_row = _execute(
        connection,
        f"""
        select run_id, runner_name, status, dry_run, completed_at is not null
        from {_relation(config.runtime_database, config.runtime_schema, "runs")}
        where run_id = :1
        """,
        (run.run_id,),
    ).fetchone()
    assert run_row == (run.run_id, run.runner_name, "succeeded", False, True)

    sync_report = _execute(
        connection,
        f"""
        select report_id, run_id, attempt_id, sync_name, status, submitted_record_count,
               succeeded_record_count, accepted_record_count, failed_record_count,
               progress_advanced
        from {_relation(config.runtime_database, config.runtime_schema, "sync_reports")}
        where run_id = :1
        """,
        (run.run_id,),
    ).fetchone()
    assert sync_report == (
        report.report_id,
        run.run_id,
        report.attempt_id,
        report.sync_name,
        "succeeded",
        2,
        2,
        2,
        0,
        True,
    )


def _assert_state_current_summary_readable(store: object, collect_id: str) -> None:
    summary = store.state_current_summary(  # type: ignore[attr-defined]
        declaration_name="customer_state",
        source_name="customers",
    )

    assert summary.declaration_name == "customer_state"
    assert summary.source_name == "customers"
    assert summary.collect_id == collect_id
    assert summary.row_count == 2


def _assert_state_current_upserts_readable(store: object, collect_id: str) -> None:
    first_page = store.read_state_current_upserts(  # type: ignore[attr-defined]
        declaration_name="customer_state",
        source_name="customers",
        max_rows=1,
    )
    assert first_page.row_count == 1
    assert first_page.next_cursor is not None
    assert first_page.collect_id == collect_id
    assert _json_column_values(first_page.payload, "key_json") == [{"customer": "cust_1"}]

    second_page = store.read_state_current_upserts(  # type: ignore[attr-defined]
        declaration_name="customer_state",
        source_name="customers",
        max_rows=10,
        cursor=first_page.next_cursor,
    )
    assert second_page.row_count == 1
    assert second_page.next_cursor is None
    assert second_page.collect_id == collect_id
    assert _json_column_values(second_page.payload, "key_json") == [{"customer": "cust_2"}]


def _assert_pending_work_readable(store: object, collect_id: str) -> None:
    state_page = store.read_pending_work(  # type: ignore[attr-defined]
        scope=_progress_scope(family="state", declaration_name="customer_state"),
        max_rows=10,
    )
    assert state_page.row_count == 2
    assert _json_column_values(state_page.payload, "key_json") == [
        {"customer": "cust_1"},
        {"customer": "cust_2"},
    ]
    assert state_page.complete_through_collect_id == collect_id


def _assert_progress_retention_and_cleanup(store: object, collect_id: str) -> None:
    scope = _progress_scope(family="state", declaration_name="customer_state")

    before = store.get_destination_progress(scope)  # type: ignore[attr-defined]
    assert before.position is None

    registered = store.register_destination_progress(scope)  # type: ignore[attr-defined]
    assert registered.position is None

    update = store.update_destination_progress(  # type: ignore[attr-defined]
        scope=scope,
        position=StateOrderedWorkScanPosition(collect_id=collect_id, sequence_order=1),
    )
    assert update.before is None
    assert update.after == StateOrderedWorkScanPosition(collect_id=collect_id, sequence_order=1)
    assert update.advanced is True

    after = store.get_destination_progress(scope)  # type: ignore[attr-defined]
    assert after.position == StateOrderedWorkScanPosition(collect_id=collect_id, sequence_order=1)

    watermark = store.retention_watermark(  # type: ignore[attr-defined]
        family="state",
        declaration_name="customer_state",
    )
    assert watermark == collect_id

    cleanup = store.cleanup_ordered_work(  # type: ignore[attr-defined]
        family="state",
        declaration_name="customer_state",
        through_collect_id=collect_id,
    )
    assert cleanup.safe_through_collect_id == collect_id
    assert cleanup.deleted_ordered_work_count == 2
    assert cleanup.retained_pending_count == 0

    assert store.read_pending_work(scope=scope, max_rows=10).row_count == 0  # type: ignore[attr-defined]


def _assert_destination_batch_ledger(store: object) -> None:
    scope = _progress_scope(family="event", declaration_name="purchase_event")
    pending = _destination_batch(scope=scope, label="ledger-pending", index=0)
    failed = DestinationBatchRecord(
        batch_id=destination_batch_id(
            _destination_batch_identity(scope=scope, label="ledger-failed", index=1)
        ),
        identity=_destination_batch_identity(scope=scope, label="ledger-failed", index=1),
        status="failed",
        completion_state="unresolved",
        attempt_count=1,
        attempt_id="attempt-ledger-failed",
        run_id="run-ledger",
        http_status=503,
        retry_eligible=True,
    )

    stored = store.upsert_destination_batch(pending)  # type: ignore[attr-defined]
    stored_many = store.upsert_destination_batches((stored, failed))  # type: ignore[attr-defined]

    assert stored == pending
    assert tuple(batch.batch_id for batch in stored_many) == (pending.batch_id, failed.batch_id)
    assert store.get_destination_batch(batch_id=pending.batch_id) == pending  # type: ignore[attr-defined]
    assert store.get_destination_batches(batch_ids=(pending.batch_id, failed.batch_id)) == (  # type: ignore[attr-defined]
        pending,
        failed,
    )
    assert store.list_destination_batches(scope=scope) == (pending, failed)  # type: ignore[attr-defined]
    assert store.list_destination_batches(scope=scope, statuses=("failed",)) == (failed,)  # type: ignore[attr-defined]
    assert store.list_destination_batch_retry_candidates(scope=scope, retry_limit=3) == (  # type: ignore[attr-defined]
        pending,
        failed,
    )

    dismissed = store.dismiss_unresolved_destination_batches(scope=scope)  # type: ignore[attr-defined]
    assert tuple(batch.batch_id for batch in dismissed) == (pending.batch_id, failed.batch_id)
    assert {batch.status for batch in dismissed} == {"skipped"}
    assert {batch.completion_state for batch in dismissed} == {"resolved"}
    assert {batch.retry_eligible for batch in dismissed} == {False}
    assert all(batch.completed_at is not None for batch in dismissed)


def _assert_destination_batch_current_state_rows(
    connection: SnowflakeConnection,
    config: _LiveSnowflakeConfig,
    store: object,
) -> None:
    scope = DestinationProgressScope(
        sync_name="sync-current-state",
        destination_name="warehouse",
        surface="customers",
        family="event",
        declaration_name="purchase_event",
    )
    first = store.upsert_destination_batch(  # type: ignore[attr-defined]
        _destination_batch(scope=scope, label="current-failed", index=10)
    )
    second = store.upsert_destination_batch(  # type: ignore[attr-defined]
        _destination_batch(scope=scope, label="current-succeeded", index=11)
    )
    attempted_at = datetime(2026, 5, 9, 10, 0, 0)
    completed_at = datetime(2026, 5, 9, 10, 5, 0)

    failed = replace(
        first,
        run_id="run-current-state",
        attempt_id="attempt-current-failed",
        status="failed",
        completion_state="unresolved",
        attempt_count=1,
        record_count=2,
        retry_eligible=True,
        http_status=503,
        last_error_summary="retry later",
        last_error_detail="retry later detail",
        last_failure_category="retryable",
        first_submitted_at=attempted_at,
        last_attempted_at=attempted_at,
    )
    succeeded = replace(
        second,
        run_id="run-current-state",
        attempt_id="attempt-current-succeeded",
        status="succeeded",
        completion_state="resolved",
        attempt_count=1,
        record_count=1,
        retry_eligible=False,
        http_status=200,
        first_submitted_at=completed_at,
        last_attempted_at=completed_at,
        completed_at=completed_at,
    )
    stored = store.upsert_destination_batches(  # type: ignore[attr-defined]
        (failed, succeeded),
        existing_batches=(first, second),
    )
    assert tuple(batch.status for batch in stored) == ("failed", "succeeded")
    _commit_store(store)

    rows = _execute(
        connection,
        f"""
        select run_id, attempt_id, status, completion_state, attempt_count,
               record_count, http_status, retry_eligible, last_error_summary,
               last_error_detail, completed_at is not null
        from {_relation(config.runtime_database, config.runtime_schema, "destination_batches")}
        where run_id = :1
        order by destination_batch_index
        """,
        ("run-current-state",),
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (
            "run-current-state",
            "attempt-current-failed",
            "failed",
            "unresolved",
            1,
            2,
            503,
            True,
            "retry later",
            "retry later detail",
            False,
        ),
        (
            "run-current-state",
            "attempt-current-succeeded",
            "succeeded",
            "resolved",
            1,
            1,
            200,
            False,
            None,
            None,
            True,
        ),
    ]


def _destination_batch(
    *,
    scope: DestinationProgressScope,
    label: str,
    index: int,
) -> DestinationBatchRecord:
    identity = _destination_batch_identity(scope=scope, label=label, index=index)
    return DestinationBatchRecord(batch_id=destination_batch_id(identity), identity=identity)


def _destination_batch_identity(
    *,
    scope: DestinationProgressScope,
    label: str,
    index: int,
) -> DestinationBatchIdentity:
    return DestinationBatchIdentity(
        scope=scope,
        declaration_version_id="decl:purchase_event",
        reconcile_page_index=0,
        first_collect_id="00000000-0002-7000-8000-000000000000",
        last_collect_id="00000000-0003-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=1,
        destination_batch_index=index,
        payload_fingerprint=f"payload:{label}",
        target_request_fingerprint=f"request:{label}",
    )


def _commit_store(store: object) -> None:
    context = store._runtime_context  # type: ignore[attr-defined]  # noqa: SLF001
    context.connection.commit()


def _progress_scope(*, family: WorkFamily, declaration_name: str) -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="sync",
        destination_name="warehouse",
        surface="customers",
        family=family,
        declaration_name=declaration_name,
    )


def _json_column_values(batch: Any, column: str) -> list[Any]:
    values = batch.column(column).to_pylist()
    return [json.loads(value) if value else None for value in values]


def _source_identity(config: _LiveSnowflakeConfig) -> Mapping[str, str]:
    return {
        "backend": "snowflake",
        "database": config.source_database,
        "schema": config.source_schema,
    }


def _execute(
    connection: SnowflakeConnection,
    sql: str,
    parameters: Sequence[object] = (),
) -> Any:
    return connection.execute(" ".join(sql.split()), parameters)


def _executemany(
    connection: SnowflakeConnection,
    sql: str,
    parameters: Sequence[Sequence[object]],
) -> None:
    connection.executemany(" ".join(sql.split()), parameters).close()


def _drop_schema(connection: SnowflakeConnection, database: str, schema: str) -> None:
    try:
        _execute(connection, f"drop schema if exists {_schema(database, schema)} cascade")
    except SnowflakeConnectionError:
        pass


def _relation(database: str, schema: str, relation: str) -> str:
    return f"{_schema(database, schema)}.{_quote_identifier(relation)}"


def _schema(database: str, schema: str) -> str:
    return f"{_quote_identifier(database)}.{_quote_identifier(schema)}"


def _quote_identifier(identifier: str) -> str:
    return '"' + _required_identifier(identifier, "SQL identifier").replace('"', '""') + '"'


def _test_identifier(prefix: str, worker: str, suffix: str, purpose: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_]", "_", prefix).strip("_").upper()
    if not normalized or not re.match(r"^[A-Z_]", normalized):
        normalized = f"RETL_{normalized}"
    return _required_identifier(f"{normalized}_{worker}_{suffix}_{purpose}"[:120], "schema")


def _required_identifier(value: str, label: str) -> str:
    normalized = value.strip()
    if _SIMPLE_IDENTIFIER.fullmatch(normalized) is None:
        raise ValueError(f"Snowflake live sandbox {label} must be a simple SQL identifier.")
    return normalized
