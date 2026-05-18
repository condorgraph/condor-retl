from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pytest
from retl_reference_http.definitions import STATE_SURFACE as REFERENCE_HTTP_STATE_SURFACE

import retl
import retl.sync_runtime.submission as submission_runtime
from retl.auth import none
from retl.backends.duckdb import DuckDBRuntimeStore, DuckDBSqlBackend
from retl.collect_identity import is_uuidv7
from retl.declarations import JSONValue
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.compatibility import DestinationCompatibilityError
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.registry import declarative_connector
from retl.destinations.request_batch import (
    DestinationWorkRecord,
    RequestBatchingPolicy,
    RequestBatchPlan,
    plan_request_batches,
)
from retl.destinations.surfaces import DestinationSurface
from retl.destinations.targets import RemoteTarget, TargetRegistryRecord, registry_key
from retl.destinations.terminal_failures import DestinationSyncEvidence
from retl.runtime import destination_progress_scope
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationBatchCompletionState,
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationBatchStatus,
    DestinationProgressScope,
    DestinationScanRange,
    EventKeysetScanPosition,
    StateCurrentSnapshotScanPosition,
    destination_batch_id,
)
from retl.sync_runtime.submission import sync_destination


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return _backend(tmp_path).runtime_store()


def _backend(tmp_path: Path) -> DuckDBSqlBackend:
    return DuckDBSqlBackend(
        database=_warehouse_database(tmp_path),
        source_schema="main",
        runtime_schema="retl",
    )


def _warehouse_database(tmp_path: Path) -> Path:
    return tmp_path / "warehouse.duckdb"


class _CountingDuckDBConnection:
    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self.execute_sql: list[str] = []

    def execute(self, sql: object, *args: Any, **kwargs: Any) -> Any:
        self.execute_sql.append(str(sql))
        return self._connection.execute(sql, *args, **kwargs)

    def executemany(self, sql: object, *args: Any, **kwargs: Any) -> Any:
        return self._connection.executemany(sql, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


def _state_declaration(tmp_path: Path) -> retl.State:
    source_database = _warehouse_database(tmp_path)
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table customers (
            customer_id varchar,
            email varchar,
            plan varchar
        )
        """
    )
    connection.executemany(
        "insert into customers values (?, ?, ?)",
        [
            ("cust_1", "one@example.com", "pro"),
            ("cust_2", "two@example.com", "free"),
        ],
    )
    connection.close()
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="select customer_id, email, plan from customers",
            backend=_backend(tmp_path).source_backend(),
        ),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _bulk_state_declaration(tmp_path: Path, *, row_count: int) -> retl.State:
    source_database = _warehouse_database(tmp_path)
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table customers (
            customer_id varchar,
            email varchar,
            plan varchar
        )
        """
    )
    connection.executemany(
        "insert into customers values (?, ?, ?)",
        [
            (f"cust_{index:04d}", f"customer-{index:04d}@example.com", "pro")
            for index in range(row_count)
        ],
    )
    connection.close()
    return retl.state(
        name="bulk_customer_state",
        source=retl.source(
            name="bulk_customers",
            query="select customer_id, email, plan from customers order by customer_id",
            backend=_backend(tmp_path).source_backend(),
        ),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _targeted_state_declaration(tmp_path: Path) -> retl.State:
    source_database = _warehouse_database(tmp_path)
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table customers (
            customer_id varchar,
            email varchar,
            plan varchar,
            audience varchar
        )
        """
    )
    connection.executemany(
        "insert into customers values (?, ?, ?, ?)",
        [
            ("cust_1", "one@example.com", "pro", "vip"),
            ("cust_2", "two@example.com", "free", "vip"),
        ],
    )
    connection.close()
    return retl.state(
        name="targeted_customer_state",
        source=retl.source(
            name="targeted_customers",
            query="select customer_id, email, plan, audience from customers",
            backend=_backend(tmp_path).source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _reference_http_state_declaration(tmp_path: Path) -> retl.State:
    source_database = _warehouse_database(tmp_path)
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table customers (
            customer_id varchar,
            email varchar,
            status varchar
        )
        """
    )
    connection.executemany(
        "insert into customers values (?, ?, ?)",
        [
            ("cust_1", "one@example.com", "active"),
            ("cust_2", "two@example.com", "inactive"),
        ],
    )
    connection.close()
    return retl.state(
        name="reference_http_customer_state",
        source=retl.source(
            name="reference_http_customers",
            query="select customer_id, email, status from customers",
            backend=_backend(tmp_path).source_backend(),
        ),
        key={"customer_id": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"status": "status"},
    )


def _three_row_reference_http_state_declaration(tmp_path: Path) -> retl.State:
    source_database = _warehouse_database(tmp_path)
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table customers (
            customer_id varchar,
            email varchar,
            status varchar
        )
        """
    )
    connection.executemany(
        "insert into customers values (?, ?, ?)",
        [
            ("cust_1", "one@example.com", "active"),
            ("cust_2", "two@example.com", "inactive"),
            ("cust_3", "three@example.com", "active"),
        ],
    )
    connection.close()
    return retl.state(
        name="three_row_reference_http_customer_state",
        source=retl.source(
            name="three_row_reference_http_customers",
            query="select customer_id, email, status from customers",
            backend=_backend(tmp_path).source_backend(),
        ),
        key={"customer_id": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"status": "status"},
    )


def _bulk_reference_http_state_declaration(tmp_path: Path, *, row_count: int) -> retl.State:
    source_database = _warehouse_database(tmp_path)
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table customers (
            customer_id varchar,
            email varchar,
            status varchar
        )
        """
    )
    connection.executemany(
        "insert into customers values (?, ?, ?)",
        [
            (f"cust_{index:04d}", f"customer-{index:04d}@example.com", "active")
            for index in range(row_count)
        ],
    )
    connection.close()
    return retl.state(
        name="bulk_reference_http_customer_state",
        source=retl.source(
            name="bulk_reference_http_customers",
            query="select customer_id, email, status from customers order by customer_id",
            backend=_backend(tmp_path).source_backend(),
        ),
        key={"customer_id": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"status": "status"},
    )


def _replace_state_source_rows(tmp_path: Path, rows: list[tuple[str, str, str]]) -> None:
    connection = duckdb.connect(str(_warehouse_database(tmp_path)))
    connection.execute("delete from customers")
    connection.executemany("insert into customers values (?, ?, ?)", rows)
    connection.close()


def _event_declaration(tmp_path: Path) -> retl.Event:
    source_database = _warehouse_database(tmp_path)
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table purchases (
            purchase_id varchar,
            email varchar,
            purchased_at varchar,
            order_total integer
        )
        """
    )
    connection.executemany(
        "insert into purchases values (?, ?, ?, ?)",
        [
            ("purchase_1", "one@example.com", "2026-01-01T00:00:00Z", 100),
            ("purchase_2", "two@example.com", "2026-01-02T00:00:00Z", 200),
        ],
    )
    connection.close()
    return retl.event(
        name="purchase",
        source=retl.source(
            name="purchases",
            mode="checkpointed",
            query="select purchase_id, email, purchased_at, order_total from purchases",
            checkpoint={
                "cursor": "purchased_at",
                "primary_key": "purchase_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
            backend=_backend(tmp_path).source_backend(),
        ),
        key={"purchase": "purchase_id"},
        occurred_at="purchased_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"order_total": "order_total"},
    )


def _append_event_source_rows(tmp_path: Path, rows: list[tuple[str, str, str, int]]) -> None:
    connection = duckdb.connect(str(_warehouse_database(tmp_path)))
    connection.executemany("insert into purchases values (?, ?, ?, ?)", rows)
    connection.close()


def _replace_event_source_rows(tmp_path: Path, rows: list[tuple[str, str, str, int]]) -> None:
    connection = duckdb.connect(str(_warehouse_database(tmp_path)))
    connection.execute("delete from purchases")
    connection.executemany("insert into purchases values (?, ?, ?, ?)", rows)
    connection.close()


def _state_sync(
    declaration: retl.State,
    *,
    name: str,
    binding_name: str,
    config: dict[str, JSONValue] | None = None,
    on_failure: retl.FailureHandlingMode = "continue_on_any",
) -> retl.Sync:
    return retl.sync(
        name=name,
        declaration=declaration,
        destination=retl.destinations.load(
            "retl/mock",
            binding_name=binding_name,
            config=config,
        ),
        surface="profile_properties",
        on_failure=on_failure,
    )


def _recording_state_sync(
    declaration: retl.State,
    *,
    name: str,
    binding_name: str,
    submissions: list[tuple[int, tuple[str, ...]]],
    events: list[str] | None = None,
    plan_events: list[str] | None = None,
    config: dict[str, JSONValue] | None = None,
    request_batch_max_rows: int = 1,
    submission_hook: Any | None = None,
    on_failure: retl.FailureHandlingMode = "continue_on_any",
) -> retl.Sync:
    def submit_recording_destination(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        request_plans = selected_request_plans or ()
        if events is not None:
            events.append("submit")
        submissions.append((attempted_count, tuple(plan.batch_id for plan in request_plans)))
        return DestinationSubmissionEvidence(
            status="confirmed",
            attempted_count=attempted_count,
            confirmed_count=attempted_count,
            request_batch_count=len(request_plans),
            summary="Recording destination confirmed submitted work.",
        )

    def plan_recording_requests(
        *,
        surface: DestinationSurface,
        reconciled: object,
        **_: Any,
    ) -> object:
        if plan_events is not None:
            plan_events.append("plan")
        work = getattr(reconciled, "operation_pages", None) or getattr(
            reconciled, "import_pages", None
        )
        return plan_request_batches(
            sync_name=str(getattr(reconciled, "sync_name", "sync")),
            surface_name=surface.name,
            work=work,
            request_template={
                "method": "POST",
                "path": f"/recording/{surface.name}/batches",
                "json_body": {
                    "sync": "{{sync}}",
                    "surface": "{{surface}}",
                    "index": "{{index}}",
                    "row_count": "{{row_count}}",
                },
            },
            batching_policy=RequestBatchingPolicy(max_rows=request_batch_max_rows),
            family="event_imports" if getattr(reconciled, "import_pages", None) else None,
        )

    connector = declarative_connector(
        ref=f"retl/recording-{name}",
        display_name="Recording Destination",
        surfaces=(
            DestinationSurface(
                name="profile_properties",
                declaration_family="state",
                supported_operations=("upsert", "remove"),
                target_mode="unsupported",
                accepted_identifier_types=("email",),
            ),
        ),
        auth_modes=(none(),),
        batch_planning_hook=plan_recording_requests,
        submission_hook=submission_hook or submit_recording_destination,
    )
    return retl.sync(
        name=name,
        declaration=declaration,
        destination=retl.DestinationBinding(
            binding_name=binding_name,
            destination_ref=connector.connector_ref,
            connector=connector,
            config=config or {},
        ),
        surface="profile_properties",
        on_failure=on_failure,
    )


def _single_row_reconciled(sync: retl.Sync) -> SimpleNamespace:
    payload = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "cust_1",
                "identifiers": ({"type": "email", "value": "one@example.com"},),
                "payload": {"plan": "pro"},
                "key": {"customer": "cust_1"},
                "collect_id": "00000000-0001-7000-8000-000000000000",
                "sequence_order": 0,
            }
        ]
    ).to_batches()[0]
    return SimpleNamespace(
        phase="reconcile",
        status="succeeded",
        sync_name=sync.name,
        operation_pages=(payload,),
        operation_count=1,
        upsert_count=1,
        remove_count=0,
        scope=destination_progress_scope(sync),
        dry_run=False,
    )


@dataclass
class _RecordingTransport:
    responses: list[HttpResponse]
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return self.responses.pop(0)


@dataclass
class _RecordingManagedTargetClient:
    created_prefix: str = "remote"
    find_calls: list[str] | None = None
    create_calls: list[tuple[str, str]] | None = None

    def __post_init__(self) -> None:
        if self.find_calls is None:
            self.find_calls = []
        if self.create_calls is None:
            self.create_calls = []

    def find_target(self, logical_target: str) -> RemoteTarget | None:
        assert self.find_calls is not None
        self.find_calls.append(logical_target)
        return None

    def create_target(self, logical_target: str, *, display_name: str) -> RemoteTarget:
        assert self.create_calls is not None
        self.create_calls.append((logical_target, display_name))
        return RemoteTarget(remote_id=f"{self.created_prefix}_{logical_target}")


def _reference_http_state_sync(
    declaration: retl.State,
    *,
    transport: _RecordingTransport,
    request_batch_max_rows: int = 10,
    on_failure: retl.FailureHandlingMode = "continue_on_any",
    config: dict[str, JSONValue] | None = None,
) -> retl.Sync:
    destination_config: dict[str, JSONValue] = {
        "request_batch_max_rows": request_batch_max_rows,
        "transport": transport,  # type: ignore[dict-item]
    }
    if config is not None:
        destination_config.update(config)
    return retl.sync(
        name="reference_http_customer_profiles",
        declaration=declaration,
        destination=retl.destinations.load(
            "retl/reference-http",
            binding_name="reference_http",
            config=destination_config,
        ),
        surface=REFERENCE_HTTP_STATE_SURFACE,
        on_failure=on_failure,
    )


def _event_sync(
    declaration: retl.Event,
    *,
    name: str = "purchase_imports",
    binding_name: str = "mock_events",
    config: dict[str, JSONValue] | None = None,
) -> retl.Sync:
    return retl.sync(
        name=name,
        declaration=declaration,
        destination=retl.destinations.load(
            "retl/mock",
            binding_name=binding_name,
            config=config,
        ),
        surface="purchase_event",
    )


def _recording_event_sync(
    declaration: retl.Event,
    *,
    name: str,
    binding_name: str,
    submissions: list[tuple[int, tuple[str, ...]]],
    events: list[str] | None = None,
    submission_hook: Any | None = None,
) -> retl.Sync:
    def submit_recording_destination(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        request_plans = selected_request_plans or ()
        if events is not None:
            events.append("submit")
        submissions.append((attempted_count, tuple(plan.batch_id for plan in request_plans)))
        return DestinationSubmissionEvidence(
            status="confirmed",
            attempted_count=attempted_count,
            confirmed_count=attempted_count,
            request_batch_count=len(request_plans),
            summary="Recording destination confirmed submitted event work.",
        )

    def plan_recording_requests(
        *,
        surface: DestinationSurface,
        reconciled: object,
        **_: Any,
    ) -> object:
        work = getattr(reconciled, "import_pages", None)
        return plan_request_batches(
            sync_name=str(getattr(reconciled, "sync_name", "sync")),
            surface_name=surface.name,
            work=work,
            request_template={
                "method": "POST",
                "path": f"/recording/{surface.name}/batches",
                "json_body": {
                    "sync": "{{sync}}",
                    "surface": "{{surface}}",
                    "index": "{{index}}",
                    "row_count": "{{row_count}}",
                },
            },
            batching_policy=RequestBatchingPolicy(max_rows=1),
            family="event_imports",
        )

    connector = declarative_connector(
        ref=f"retl/recording-event-{name}",
        display_name="Recording Event Destination",
        surfaces=(
            DestinationSurface(
                name="purchase_event",
                declaration_family="event",
                supported_operations=("import",),
                target_mode="unsupported",
                accepted_identifier_types=("email",),
            ),
        ),
        auth_modes=(none(),),
        batch_planning_hook=plan_recording_requests,
        submission_hook=submission_hook or submit_recording_destination,
    )
    return retl.sync(
        name=name,
        declaration=declaration,
        destination=retl.DestinationBinding(
            binding_name=binding_name,
            destination_ref=connector.connector_ref,
            connector=connector,
        ),
        surface="purchase_event",
    )


def _targeted_state_sync(
    declaration: retl.State,
    *,
    name: str = "targeted_customer_profiles",
    binding_name: str = "targeted_profiles",
    request_paths: list[str] | None = None,
    managed_target_client: _RecordingManagedTargetClient | None = None,
) -> retl.Sync:
    def remote_target_record(
        record: DestinationWorkRecord,
        *,
        binding: retl.DestinationBinding,
        surface: DestinationSurface,
    ) -> DestinationWorkRecord:
        if record.target is None:
            return record
        mapped = next(
            (
                mapping.remote.remote_id
                for mapping in binding.target_mappings
                if mapping.logical_target == record.target
                and (mapping.surface is None or mapping.surface == surface.name)
            ),
            None,
        )
        if mapped is not None:
            remote_target = mapped
        else:
            key = registry_key(binding=binding, surface=surface.name, logical_target=record.target)
            registered = (
                binding.target_registry.get(key) if binding.target_registry is not None else None
            )
            remote_target = registered.remote.remote_id if registered is not None else record.target
        return DestinationWorkRecord(
            operation=record.operation,
            record_identity=record.record_identity,
            identifiers=record.identifiers,
            payload=record.payload,
            key=record.key,
            collect_id=record.collect_id,
            sequence_order=record.sequence_order,
            target=remote_target,
            occurred_at=record.occurred_at,
            payload_fingerprint=record.payload_fingerprint,
            source_position=record.source_position,
            raw=record.raw,
        )

    def plan_targeted_requests(
        *,
        binding: retl.DestinationBinding,
        surface: DestinationSurface,
        reconciled: object,
        **_: Any,
    ) -> object:
        work = getattr(reconciled, "operation_pages", None)
        return plan_request_batches(
            sync_name=str(getattr(reconciled, "sync_name", name)),
            surface_name=surface.name,
            work=work,
            request_template={
                "method": "POST",
                "path": "/targets/{{ target }}/members",
                "json_body": {"row_count": "{{ row_count }}"},
            },
            batching_policy=RequestBatchingPolicy(max_rows=100),
            family="state_operations",
            partition_key=lambda record: (record.target, record.operation),
            record_hook=lambda record: remote_target_record(
                record,
                binding=binding,
                surface=surface,
            ),
        )

    def submit_targeted_destination(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        if request_paths is not None:
            request_paths.extend(plan.request.path for plan in selected_request_plans or ())
        return DestinationSubmissionEvidence(
            status="confirmed",
            attempted_count=attempted_count,
            confirmed_count=attempted_count,
            request_batch_count=len(selected_request_plans or ()),
            summary="Targeted destination confirmed submitted work.",
        )

    connector = declarative_connector(
        ref=f"retl/targeted-{name}",
        display_name="Targeted Destination",
        surfaces=(
            DestinationSurface(
                name="audience_membership",
                declaration_family="state",
                supported_operations=("upsert", "remove"),
                target_mode="required",
                supports_managed_targets=managed_target_client is not None,
                accepted_identifier_types=("email",),
            ),
        ),
        auth_modes=(none(),),
        batch_planning_hook=plan_targeted_requests,
        submission_hook=submit_targeted_destination,
    )
    return retl.sync(
        name=name,
        declaration=declaration,
        destination=retl.DestinationBinding(
            binding_name=binding_name,
            destination_ref=connector.connector_ref,
            connector=connector,
            managed_target_client=managed_target_client,
        ),
        surface="audience_membership",
    )


def _destination_batch(
    scope: DestinationProgressScope,
    *,
    index: int = 0,
    label: str = "batch",
    status: DestinationBatchStatus = "pending",
    completion_state: DestinationBatchCompletionState = "unresolved",
    retry_eligible: bool | None = None,
) -> DestinationBatchRecord:
    collect_id = _collect_id(index + 1)
    identity = DestinationBatchIdentity(
        scope=scope,
        declaration_version_id=f"decl:{label}",
        source_range=None,
        source_page_index=None,
        reconcile_page_index=0,
        first_collect_id=collect_id,
        last_collect_id=collect_id,
        first_sequence_order=index * 10,
        last_sequence_order=index * 10 + 4,
        destination_batch_index=index,
        payload_fingerprint=f"payload:{label}",
        target_request_fingerprint=f"request:{label}",
    )
    return DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        status=status,
        completion_state=completion_state,
        retry_eligible=retry_eligible,
    )


def _collect_id(index: int) -> str:
    return f"00000000-{index:04x}-7000-8000-000000000000"


def test_runner_run_executes_ordered_work_runtime_instead_of_unsupported_stub(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.runner_name == "crm_to_lifecycle"
    assert [sync_result.sync_name for sync_result in result.syncs] == ["customer_profiles"]
    assert result.syncs[0].operation_count == 2
    assert result.syncs[0].destination_confirmed_count == 2
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert len(batches) == 1
    assert batches[0].status == "succeeded"
    assert batches[0].completion_state == "resolved"
    assert batches[0].attempt_count == 1
    assert batches[0].run_id == result.run_id
    assert batches[0].identity.reconcile_page_index == 1
    assert is_uuidv7(batches[0].identity.first_collect_id)
    assert batches[0].identity.last_collect_id == batches[0].identity.first_collect_id
    assert batches[0].identity.first_sequence_order == 0
    assert batches[0].identity.last_sequence_order == 1
    assert batches[0].attempt_id is not None


def test_runner_carries_destination_progress_after_attempt_setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
    )

    def get_destination_progress_after_setup(*args: object, **kwargs: object) -> object:
        _ = (args, kwargs)
        raise AssertionError("destination progress should be carried after attempt setup")

    monkeypatch.setattr(store, "get_destination_progress", get_destination_progress_after_setup)

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.syncs[0].progress_advanced is True


def test_runner_runtime_store_is_default_target_registry(tmp_path: Path) -> None:
    store = _store(tmp_path)
    paths: list[str] = []
    sync = _targeted_state_sync(_targeted_state_declaration(tmp_path), request_paths=paths)
    binding = cast(retl.DestinationBinding, sync.destination)
    store.put(
        TargetRegistryRecord(
            key=registry_key(
                binding=binding,
                surface="audience_membership",
                logical_target="vip",
            ),
            remote=RemoteTarget(remote_id="aud_123"),
        )
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.syncs[0].target_resolution_status == "resolved"
    assert result.syncs[0].target_registry_count == 1
    assert paths == ["/targets/aud_123/members"]


def test_runner_reuses_resolved_target_mapping_across_stage_pages(tmp_path: Path) -> None:
    store = _store(tmp_path)
    paths: list[str] = []
    sync = _targeted_state_sync(_targeted_state_declaration(tmp_path), request_paths=paths)
    binding = cast(retl.DestinationBinding, sync.destination)
    store.put(
        TargetRegistryRecord(
            key=registry_key(
                binding=binding,
                surface="audience_membership",
                logical_target="vip",
            ),
            remote=RemoteTarget(remote_id="aud_123"),
        )
    )

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        stage_batch_max_rows=1,
    ).run(sync)

    assert result.syncs[0].target_resolution_status == "resolved"
    assert result.syncs[0].target_count == 1
    assert result.syncs[0].target_registry_count == 1
    assert result.syncs[0].target_mapped_count == 1
    assert paths == ["/targets/aud_123/members", "/targets/aud_123/members"]


def test_runner_reuses_created_target_record_across_store_instances(tmp_path: Path) -> None:
    database = _warehouse_database(tmp_path)
    client = _RecordingManagedTargetClient(created_prefix="aud")
    first_paths: list[str] = []
    declaration = _targeted_state_declaration(tmp_path)
    first_sync = _targeted_state_sync(
        declaration,
        request_paths=first_paths,
        managed_target_client=client,
    )
    first_store = DuckDBSqlBackend(
        database=database,
        source_schema="main",
        runtime_schema="retl",
    ).runtime_store()

    first = retl.runner(name="crm_to_lifecycle", runtime_store=first_store).run(first_sync)
    first_store.close()

    second_paths: list[str] = []
    second_sync = _targeted_state_sync(
        declaration,
        name="targeted_customer_profiles",
        request_paths=second_paths,
        managed_target_client=client,
    )
    second_store = DuckDBSqlBackend(
        database=database,
        source_schema="main",
        runtime_schema="retl",
    ).runtime_store()
    second = retl.runner(name="crm_to_lifecycle", runtime_store=second_store).run(
        second_sync,
        resend_all=True,
    )

    assert first.syncs[0].target_resolution_status == "resolved"
    assert first.syncs[0].target_managed_created_count == 1
    assert first_paths == ["/targets/aud_vip/members"]
    assert second.syncs[0].target_registry_count == 1
    assert second.syncs[0].target_managed_created_count == 0
    assert second_paths == ["/targets/aud_vip/members"]
    assert client.find_calls == ["vip"]
    assert client.create_calls == [("vip", "vip")]


def test_runner_dry_run_managed_target_plan_does_not_persist_registry(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    client = _RecordingManagedTargetClient(created_prefix="aud")
    sync = _targeted_state_sync(
        _targeted_state_declaration(tmp_path),
        managed_target_client=client,
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync, dry_run=True)

    assert result.syncs[0].target_resolution_status == "planned"
    assert result.syncs[0].target_planned_create_count == 1
    binding = cast(retl.DestinationBinding, sync.destination)
    assert (
        store.get(
            registry_key(
                binding=binding,
                surface="audience_membership",
                logical_target="vip",
            )
        )
        is None
    )
    assert client.find_calls == ["vip"]
    assert client.create_calls == []


def test_runner_run_executes_event_runtime(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _event_declaration(tmp_path)
    sync = _event_sync(declaration)

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.syncs[0].event_import_count == 2
    progress = store.get_destination_progress(destination_progress_scope(sync)).position
    assert progress == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-01-02T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("purchase_2"),
    )
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert len(batches) == 1
    assert batches[0].identity.source_range is not None
    assert batches[0].identity.source_range.upper_bound_inclusive == progress


def test_runner_run_event_does_not_call_unbounded_event_collect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    sync = _event_sync(_event_declaration(tmp_path))

    def reject_event_collect_scan(**_: object) -> object:
        raise AssertionError("Event runner collect must not scan the source before staging.")

    monkeypatch.setattr(store, "produce_event_collect", reject_event_collect_scan)

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.syncs[0].event_import_count == 2


def test_accepted_surface_records_accepted_ledger_without_sync_policy(tmp_path: Path) -> None:
    store = _store(tmp_path)
    declaration = _event_declaration(tmp_path)
    sync = retl.sync(
        name="accepted_purchase_imports",
        declaration=declaration,
        destination=retl.destinations.load("retl/mock", binding_name="mock_accepted_events"),
        surface="accepted_event_import",
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.status == "succeeded"
    assert result.syncs[0].destination_accepted_count == 2
    assert result.syncs[0].destination_confirmed_count == 0
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert [batch.status for batch in batches] == ["accepted"]


def test_runner_drains_paginated_state_stage_before_reporting_success(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
    )

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        stage_batch_max_rows=1,
    ).run(sync)

    assert result.status == "succeeded"
    assert result.syncs[0].operation_count == 2
    assert result.syncs[0].destination_confirmed_count == 2
    assert result.sync_reports[0].progress.page_count == 2
    assert result.sync_reports[0].destination.last_error_summary == ""
    assert "Last Error Summary:" not in result.to_text()


def test_runner_drains_paginated_event_stage_before_reporting_success(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _event_declaration(tmp_path)
    sync = _event_sync(declaration)

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        stage_batch_max_rows=1,
    ).run(sync)

    assert result.status == "succeeded"
    assert result.syncs[0].event_import_count == 2
    assert result.syncs[0].destination_confirmed_count == 2
    assert result.sync_reports[0].progress.page_count == 2


def test_dry_run_state_runner_execution_does_not_persist_destination_batches(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync, dry_run=True)

    assert result.dry_run is True
    assert result.syncs[0].operation_count == 2
    assert result.sync_reports[0].destination.attempted_count == 2
    assert result.sync_reports[0].destination.request_batch_count == 0
    assert store.list_destination_batches(scope=destination_progress_scope(sync)) == ()
    assert "destination_batch_attempts" not in store.inspect_runtime_store()["tables"]


def test_submitting_destination_without_request_batch_planning_fails_before_submission(
    tmp_path: Path,
) -> None:
    connector = declarative_connector(
        ref="retl/no-request-plan",
        display_name="No Request Plan",
        surfaces=(
            DestinationSurface(
                name="profile_properties",
                declaration_family="state",
                supported_operations=("upsert", "remove"),
                accepted_identifier_types=("email",),
            ),
        ),
        auth_modes=(none(),),
    )
    sync = retl.sync(
        name="customer_profiles",
        declaration=_state_declaration(tmp_path),
        destination=retl.DestinationBinding(
            binding_name="no_request_plan",
            destination_ref=connector.connector_ref,
            connector=connector,
        ),
        surface="profile_properties",
    )

    with pytest.raises(
        DestinationCompatibilityError,
        match="must produce request-batch plans.*Reconcile-batch ledger fallback",
    ):
        retl.runner(name="crm_to_lifecycle", runtime_store=_store(tmp_path)).run(sync)


def test_dry_run_event_runner_execution_does_not_submit_destination(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _event_declaration(tmp_path)
    sync = _event_sync(declaration)

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync, dry_run=True)

    assert result.syncs[0].event_import_count == 2


def test_reference_http_request_batches_create_distinct_ledger_records(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _reference_http_state_declaration(tmp_path)
    transport = _RecordingTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"request_id": "first"}),
            HttpResponse(status_code=200, json_body={"request_id": "second"}),
        ],
        requests=[],
    )
    sync = _reference_http_state_sync(
        declaration,
        transport=transport,
        request_batch_max_rows=1,
        on_failure="stop_on_any",
        config={"destination_in_run_retry_attempt_limit": 1},
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.status == "succeeded"
    assert len(transport.requests) == 2
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert len(batches) == 2
    assert [batch.identity.destination_batch_index for batch in batches] == [0, 1]
    assert [batch.identity.first_sequence_order for batch in batches] == [0, 1]
    assert [batch.identity.last_sequence_order for batch in batches] == [0, 1]
    assert len({batch.batch_id for batch in batches}) == 2
    assert len({batch.identity.payload_fingerprint for batch in batches}) == 2
    assert sorted((batch.status, batch.attempt_count) for batch in batches) == [
        ("succeeded", 1),
        ("succeeded", 1),
    ]


def test_large_state_sync_uses_attempt_local_destination_batch_working_set(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    counting_connection = _CountingDuckDBConnection(store._connection)
    store._connection = counting_connection
    submissions: list[tuple[int, tuple[str, ...]]] = []
    sync = _recording_state_sync(
        _bulk_state_declaration(tmp_path, row_count=3_000),
        name="bulk_customer_profiles",
        binding_name="bulk_recording_profiles",
        submissions=submissions,
        request_batch_max_rows=200,
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    execution_sql = tuple(counting_connection.execute_sql)
    normalized_sql = tuple(" ".join(sql.lower().split()) for sql in execution_sql)
    batch_id_reads = [
        sql
        for sql in normalized_sql
        if (
            ("from retl.destination_batches" in sql or 'from "retl"."destination_batches"' in sql)
            and "where" in sql
            and "batch_id" in sql
            and " in " in sql
        )
    ]
    assert result.status == "succeeded"
    assert len(submissions) == 15
    assert {submission[0] for submission in submissions} == {200}
    assert all(len(submission[1]) == 1 for submission in submissions)
    assert result.syncs[0].destination_batch_count == 15
    assert result.sync_reports[0].destination.destination_batch_count == 15
    assert len(batch_id_reads) == 1
    assert batch_id_reads[0].count("?") == 15

    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert len(batches) == 15
    assert {batch.status for batch in batches} == {"succeeded"}
    assert {batch.attempt_count for batch in batches} == {1}


def test_reference_http_mixed_terminal_evidence_updates_matching_batch_statuses(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _three_row_reference_http_state_declaration(tmp_path)
    transport = _RecordingTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"request_id": "first"}),
            HttpResponse(
                status_code=409,
                json_body={"error": {"message": "terminal batch failure"}},
            ),
        ],
        requests=[],
    )
    sync = _reference_http_state_sync(
        declaration,
        transport=transport,
        request_batch_max_rows=1,
        on_failure="stop_on_any",
        config={"destination_in_run_retry_attempt_limit": 1},
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.status == "failed"
    assert len(transport.requests) == 2
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert [(batch.identity.destination_batch_index, batch.status) for batch in batches] == [
        (0, "succeeded"),
        (1, "failed"),
        (2, "pending"),
    ]
    assert result.syncs[0].destination_batch_count == 3
    assert result.sync_reports[0].destination.destination_batch_count == 3
    assert batches[0].completion_state == "resolved"
    assert batches[1].completion_state == "unresolved"
    assert batches[2].completion_state == "unresolved"


def test_continue_on_any_attempts_remaining_request_batches_after_terminal_failure(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _three_row_reference_http_state_declaration(tmp_path)
    transport = _RecordingTransport(
        responses=[
            HttpResponse(
                status_code=409,
                json_body={"error": {"message": "terminal batch failure"}},
            ),
            HttpResponse(status_code=200, json_body={"request_id": "second"}),
            HttpResponse(status_code=200, json_body={"request_id": "third"}),
        ],
        requests=[],
    )
    sync = _reference_http_state_sync(
        declaration,
        transport=transport,
        request_batch_max_rows=1,
        on_failure="continue_on_any",
        config={"destination_in_run_retry_attempt_limit": 1},
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.status == "succeeded"
    assert len(transport.requests) == 3
    assert result.syncs[0].destination_terminal_failure_count == 1
    assert result.syncs[0].destination_confirmed_count == 2
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert [(batch.identity.destination_batch_index, batch.status) for batch in batches] == [
        (0, "failed"),
        (1, "succeeded"),
        (2, "succeeded"),
    ]


def test_continue_on_any_persists_destination_batch_page_with_bounded_ledger_write(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _bulk_reference_http_state_declaration(tmp_path, row_count=12)
    transport = _RecordingTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"request_id": f"request-{index}"})
            for index in range(12)
        ],
        requests=[],
    )
    sync = _reference_http_state_sync(
        declaration,
        transport=transport,
        request_batch_max_rows=1,
        on_failure="continue_on_any",
        config={"destination_in_run_retry_attempt_limit": 1},
    )
    write_sizes: list[int] = []
    original = store.upsert_destination_batches

    def upsert_destination_batches(
        records: tuple[Any, ...],
        **kwargs: Any,
    ) -> tuple[DestinationBatchRecord, ...]:
        if kwargs.get("existing_batches") is not None:
            write_sizes.append(len(records))
        return original(records, **kwargs)

    store.upsert_destination_batches = upsert_destination_batches  # type: ignore[method-assign]

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.status == "succeeded"
    assert len(transport.requests) == 12
    assert write_sizes == [12, 10, 2]


def test_continue_on_any_continues_to_next_stage_page_after_progress_allowed_failure(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _three_row_reference_http_state_declaration(tmp_path)
    transport = _RecordingTransport(
        responses=[
            HttpResponse(
                status_code=409,
                json_body={"error": {"message": "terminal first page failure"}},
            ),
            HttpResponse(status_code=200, json_body={"request_id": "second-page"}),
            HttpResponse(status_code=200, json_body={"request_id": "third-page"}),
        ],
        requests=[],
    )
    sync = _reference_http_state_sync(
        declaration,
        transport=transport,
        request_batch_max_rows=1,
        on_failure="continue_on_any",
        config={"destination_in_run_retry_attempt_limit": 1},
    )

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        stage_batch_max_rows=1,
    ).run(sync)

    assert result.status == "succeeded"
    assert len(transport.requests) == 3
    assert result.sync_reports[0].progress.page_count == 3
    assert result.syncs[0].destination_terminal_failure_count == 1
    assert result.syncs[0].destination_confirmed_count == 2
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert [(batch.identity.reconcile_page_index, batch.status) for batch in batches] == [
        (1, "failed"),
        (2, "succeeded"),
        (3, "succeeded"),
    ]


def test_reference_http_mixed_retryable_evidence_updates_matching_batch_statuses(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _reference_http_state_declaration(tmp_path)
    transport = _RecordingTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"request_id": "first"}),
            HttpResponse(
                status_code=503,
                json_body={"error": {"message": "temporary batch failure"}},
            ),
        ],
        requests=[],
    )
    sync = _reference_http_state_sync(
        declaration,
        transport=transport,
        request_batch_max_rows=1,
        on_failure="stop_on_any",
        config={"destination_in_run_retry_attempt_limit": 1},
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.status == "failed"
    assert len(transport.requests) == 2
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert [(batch.identity.destination_batch_index, batch.status) for batch in batches] == [
        (0, "succeeded"),
        (1, "failed"),
    ]
    assert batches[0].completion_state == "resolved"
    assert batches[1].completion_state == "unresolved"
    assert batches[1].retry_eligible is True


def test_runner_in_run_retry_resubmits_only_retryable_failed_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    declaration = _reference_http_state_declaration(tmp_path)
    sleeps: list[float] = []
    monkeypatch.setattr(submission_runtime, "_sleep", sleeps.append)
    transport = _RecordingTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"request_id": "first"}),
            HttpResponse(
                status_code=503,
                json_body={"error": {"message": "temporary batch failure"}},
            ),
            HttpResponse(status_code=200, json_body={"request_id": "retry"}),
        ],
        requests=[],
    )
    sync = _reference_http_state_sync(
        declaration,
        transport=transport,
        request_batch_max_rows=1,
        on_failure="stop_on_any",
        config={
            "destination_in_run_retry_attempt_limit": 2,
            "destination_in_run_retry_base_backoff_seconds": 0,
            "destination_in_run_retry_jitter_ratio": 0,
        },
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.status == "succeeded"
    assert len(transport.requests) == 3
    first_body = cast(dict[str, object], transport.requests[0].json_body)
    second_body = cast(dict[str, object], transport.requests[1].json_body)
    retry_body = cast(dict[str, object], transport.requests[2].json_body)
    assert first_body != retry_body
    assert second_body == retry_body
    assert sleeps == []
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert [(batch.status, batch.attempt_count) for batch in batches] == [
        ("succeeded", 1),
        ("succeeded", 2),
    ]


def test_runner_retries_same_planned_batch_in_run_without_replanning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    submissions: list[tuple[int, tuple[str, ...], tuple[object, ...]]] = []
    plan_events: list[str] = []
    sleeps: list[float] = []
    outcomes = ["retryable", "confirmed"]

    monkeypatch.setattr(submission_runtime, "_sleep", sleeps.append)
    monkeypatch.setattr(submission_runtime, "_random", lambda: 0.5)

    def submit_then_succeed(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        request_plans = selected_request_plans or ()
        submissions.append(
            (
                attempted_count,
                tuple(plan.batch_id for plan in request_plans),
                tuple(plan.request.json_body for plan in request_plans),
            )
        )
        outcome = outcomes.pop(0)
        if outcome == "retryable":
            return DestinationSubmissionEvidence(
                status="retryable_failure",
                attempted_count=attempted_count,
                retryable_failure_count=attempted_count,
                request_batch_count=len(request_plans),
                http_status=429,
                retry_after_seconds=1,
                summary="Short rate limit.",
            )
        return DestinationSubmissionEvidence(
            status="confirmed",
            attempted_count=attempted_count,
            confirmed_count=attempted_count,
            request_batch_count=len(request_plans),
            summary="Retry confirmed.",
        )

    def counting_submit(*args: Any, **kwargs: Any) -> DestinationSubmissionEvidence:
        return submit_then_succeed(*args, **kwargs)

    sync = _recording_state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=[],
        config={"destination_in_run_retry_attempt_limit": 2},
        plan_events=plan_events,
        submission_hook=counting_submit,
        request_batch_max_rows=2,
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.status == "succeeded"
    assert plan_events == ["plan"]
    assert len(submissions) == 2
    assert submissions[0] == submissions[1]
    assert sleeps == [1.0]
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert len(batches) == 1
    assert {batch.status for batch in batches} == {"succeeded"}
    assert {batch.attempt_count for batch in batches} == {2}
    assert [(batch.attempt_count, batch.status, batch.http_status) for batch in batches] == [
        (2, "succeeded", None)
    ]


def test_sync_destination_records_injected_destination_evidence_without_in_run_retry(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    calls = 0

    def submit_should_not_run(**_: Any) -> DestinationSubmissionEvidence:
        nonlocal calls
        calls += 1
        raise AssertionError("Injected destination evidence must not call submission_hook.")

    sync = _recording_state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=[],
        config={"destination_in_run_retry_attempt_limit": 3},
        submission_hook=submit_should_not_run,
    )

    result = sync_destination(
        sync=sync,
        reconciled=cast(Any, _single_row_reconciled(sync)),
        dry_run=False,
        destination_evidence=DestinationSyncEvidence(
            attempted_count=1,
            retryable_failure_count=1,
        ),
        runtime_store=store,
        run_id="run-injected-evidence",
        attempt_id="attempt-injected-evidence",
        page_index=1,
    )

    assert calls == 0
    assert result.submission.status == "retryable_failure"
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert [(batch.status, batch.retry_eligible, batch.attempt_count) for batch in batches] == [
        ("failed", True, 1)
    ]
    assert [(batch.run_id, batch.attempt_id, batch.attempt_count) for batch in batches] == [
        ("run-injected-evidence", "attempt-injected-evidence:destination-batch-1", 1)
    ]


@pytest.mark.parametrize(("category", "http_status"), [("auth", 429), ("schema", 503)])
def test_runner_does_not_in_run_retry_pre_acceptance_auth_or_schema_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    category: str,
    http_status: int,
) -> None:
    store = _store(tmp_path)
    calls = 0
    sleeps: list[float] = []
    monkeypatch.setattr(submission_runtime, "_sleep", sleeps.append)

    def submit_pre_acceptance(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        nonlocal calls
        calls += 1
        return DestinationSubmissionEvidence(
            status="pre_acceptance_failure",
            attempted_count=attempted_count,
            pre_acceptance_failure_count=attempted_count,
            pre_acceptance_failure_category=cast(Any, category),
            request_batch_count=len(selected_request_plans or ()),
            http_status=http_status,
            retry_after_seconds=0,
            summary=f"{category} failure must not retry in-run.",
        )

    sync = _recording_state_sync(
        _state_declaration(tmp_path),
        name=f"customer_profiles_{category}",
        binding_name=f"recording_profiles_{category}",
        submissions=[],
        config={"destination_in_run_retry_attempt_limit": 3},
        submission_hook=submit_pre_acceptance,
        request_batch_max_rows=2,
    )

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert calls == 1
    assert sleeps == []
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert len(batches) == 1
    assert {(batch.status, batch.retry_eligible, batch.attempt_count) for batch in batches} == {
        ("failed", False, 1)
    }
    assert [batch.attempt_count for batch in batches] == [1]


def test_runner_skips_in_run_retry_for_long_retry_after_and_leaves_retryable_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    submissions: list[tuple[int, tuple[str, ...]]] = []
    sleeps: list[float] = []
    monkeypatch.setattr(submission_runtime, "_sleep", sleeps.append)

    def submit_long_retry_after(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        request_plans = selected_request_plans or ()
        submissions.append((attempted_count, tuple(plan.batch_id for plan in request_plans)))
        return DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=attempted_count,
            retryable_failure_count=attempted_count,
            request_batch_count=len(request_plans),
            http_status=503,
            retry_after_seconds=30,
            summary="Long retry window.",
        )

    sync = _recording_state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=[],
        config={
            "destination_in_run_retry_attempt_limit": 3,
            "destination_in_run_retry_max_retry_after_seconds": 5,
        },
        submission_hook=submit_long_retry_after,
        request_batch_max_rows=2,
    )

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert len(submissions) == 1
    assert sleeps == []
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert len(batches) == 1
    assert {batch.status for batch in batches} == {"failed"}
    assert {batch.retry_eligible for batch in batches} == {True}
    assert {batch.attempt_count for batch in batches} == {1}


def test_runner_in_run_retry_budget_caps_attempts_and_nonretryable_is_not_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    sleeps: list[float] = []
    monkeypatch.setattr(submission_runtime, "_sleep", sleeps.append)
    monkeypatch.setattr(submission_runtime, "_random", lambda: 0.5)
    calls = 0

    def submit_retryable_until_budget(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        nonlocal calls
        calls += 1
        request_plans = selected_request_plans or ()
        return DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=attempted_count,
            retryable_failure_count=attempted_count,
            request_batch_count=len(request_plans),
            http_status=503,
            summary="Still retryable.",
        )

    retryable_sync = _recording_state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=[],
        config={
            "destination_in_run_retry_attempt_limit": 4,
            "destination_in_run_retry_base_backoff_seconds": 1,
            "destination_in_run_retry_sleep_budget_seconds": 1,
            "destination_in_run_retry_jitter_ratio": 0,
        },
        submission_hook=submit_retryable_until_budget,
        request_batch_max_rows=2,
    )

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(retryable_sync)

    assert calls == 2
    assert sleeps == [1.0]
    retryable_batches = store.list_destination_batches(
        scope=destination_progress_scope(retryable_sync)
    )
    assert {batch.attempt_count for batch in retryable_batches} == {2}
    assert {batch.retry_eligible for batch in retryable_batches} == {True}

    nonretry_path = tmp_path / "nonretry"
    nonretry_path.mkdir()
    nonretry_store = _store(nonretry_path)
    nonretry_calls = 0

    def submit_terminal(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        nonlocal nonretry_calls
        nonretry_calls += 1
        request_plans = selected_request_plans or ()
        return DestinationSubmissionEvidence(
            status="terminal_record_failure",
            attempted_count=attempted_count,
            terminal_record_failure_count=attempted_count,
            request_batch_count=len(request_plans),
            http_status=422,
            summary="Validation failed.",
        )

    nonretry_sync = _recording_state_sync(
        _state_declaration(nonretry_path),
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=[],
        config={"destination_in_run_retry_attempt_limit": 4},
        submission_hook=submit_terminal,
        request_batch_max_rows=2,
    )

    retl.runner(name="crm_to_lifecycle", runtime_store=nonretry_store).run(nonretry_sync)

    assert nonretry_calls == 1
    nonretry_batches = nonretry_store.list_destination_batches(
        scope=destination_progress_scope(nonretry_sync)
    )
    assert {batch.retry_eligible for batch in nonretry_batches} == {False}


def test_runner_retries_old_pending_and_retryable_failed_batches_before_new_scan_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    submissions: list[tuple[int, tuple[str, ...], tuple[str, ...], tuple[object, ...]]] = []
    events: list[str] = []
    outcomes = ["seed", "retry"]

    def submit_retry_recording_destination(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        request_plans = selected_request_plans or ()
        events.append("submit")
        submissions.append(
            (
                attempted_count,
                tuple(plan.batch_id for plan in request_plans),
                tuple(plan.request.path for plan in request_plans),
                tuple(plan.request.json_body for plan in request_plans),
            )
        )
        if not outcomes:
            return DestinationSubmissionEvidence.planned(
                attempted_count=attempted_count,
                dry_run=False,
                request_batch_count=len(request_plans),
                summary="No recording destination request plans selected.",
            )
        outcome = outcomes.pop(0)
        if outcome == "seed":
            return DestinationSubmissionEvidence(
                status="retryable_failure",
                attempted_count=attempted_count,
                retryable_failure_count=1,
                request_batch_count=len(request_plans),
                summary="Seeded one retryable failure and one pending batch.",
            )
        return DestinationSubmissionEvidence(
            status="confirmed",
            attempted_count=attempted_count,
            confirmed_count=attempted_count,
            request_batch_count=len(request_plans),
            summary="Retry sweep confirmed submitted work.",
        )

    sync = _recording_state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=[],
        config={"destination_in_run_retry_attempt_limit": 1},
        submission_hook=submit_retry_recording_destination,
        on_failure="stop_on_any",
    )
    scope = destination_progress_scope(sync)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    seeded = store.list_destination_batches(scope=scope)
    old_failed = seeded[0]
    old_pending = seeded[1]
    assert [(batch.status, batch.retry_eligible) for batch in seeded] == [
        ("failed", True),
        ("pending", None),
    ]
    events.clear()
    submissions.clear()
    original_read_pending_work = store.read_pending_work

    def read_pending_work_spy(*args: Any, **kwargs: Any) -> object:
        events.append("scan")
        return original_read_pending_work(*args, **kwargs)

    monkeypatch.setattr(store, "read_pending_work", read_pending_work_spy)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert events[:2] == ["submit", "scan"]
    assert len(submissions[0][1]) == 2
    assert submissions[0][2] == (
        "/recording/profile_properties/batches",
        "/recording/profile_properties/batches",
    )
    bodies = [cast(dict[str, object], body) for body in submissions[0][3]]
    assert [body["sync"] for body in bodies] == [
        "customer_profiles",
        "customer_profiles",
    ]
    assert all("records" in body for body in bodies)
    assert [len(cast(tuple[object, ...], body["records"])) for body in bodies] == [1, 1]
    batches = {batch.batch_id: batch for batch in store.list_destination_batches(scope=scope)}
    assert batches[old_pending.batch_id].status == "succeeded"
    assert batches[old_pending.batch_id].attempt_count == 1
    assert batches[old_failed.batch_id].status == "succeeded"
    assert batches[old_failed.batch_id].attempt_count == 2


def test_retry_reconstruction_reads_reconcile_siblings_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    submissions: list[tuple[int, tuple[str, ...]]] = []
    sync = _recording_state_sync(
        _bulk_state_declaration(tmp_path, row_count=3),
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=submissions,
        request_batch_max_rows=1,
    )
    scope = destination_progress_scope(sync)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    seeded = store.list_destination_batches(scope=scope)
    failed_sibling = seeded[0]
    retry_candidates = seeded[1:]
    store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=failed_sibling.batch_id,
            identity=failed_sibling.identity,
            status="failed",
            completion_state="unresolved",
            attempt_count=1,
            retry_eligible=False,
        )
    )
    for batch in retry_candidates:
        store.upsert_destination_batch(
            DestinationBatchRecord(
                batch_id=batch.batch_id,
                identity=batch.identity,
                status="pending",
                completion_state="unresolved",
                attempt_count=batch.attempt_count,
            )
        )

    read_batch_ids: list[str] = []
    original_read_destination_batch_work = store.read_destination_batch_work

    def read_destination_batch_work_spy(*args: Any, **kwargs: Any) -> object:
        batch = cast(DestinationBatchRecord, kwargs["batch"])
        read_batch_ids.append(batch.batch_id)
        return original_read_destination_batch_work(*args, **kwargs)

    monkeypatch.setattr(store, "read_destination_batch_work", read_destination_batch_work_spy)
    submissions.clear()

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert read_batch_ids == [batch.batch_id for batch in seeded]
    assert submissions[0][0] == 2
    assert len(submissions[0][1]) == len(retry_candidates)
    batches = {batch.batch_id: batch for batch in store.list_destination_batches(scope=scope)}
    assert batches[failed_sibling.batch_id].status == "failed"
    assert batches[failed_sibling.batch_id].retry_eligible is False
    assert [batches[batch.batch_id].status for batch in retry_candidates] == [
        "succeeded",
        "succeeded",
    ]


def test_event_runner_retries_old_batches_before_destination_scoped_source_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    submissions: list[tuple[int, tuple[str, ...]]] = []
    events: list[str] = []
    sync = _recording_event_sync(
        _event_declaration(tmp_path),
        name="purchase_imports",
        binding_name="recording_events",
        submissions=submissions,
        events=events,
    )
    scope = destination_progress_scope(sync)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    seeded = store.list_destination_batches(scope=scope)
    old_pending = store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=seeded[0].batch_id,
            identity=seeded[0].identity,
            record_count=seeded[0].record_count,
            attempt_count=seeded[0].attempt_count,
        )
    )
    submissions.clear()
    events.clear()
    _append_event_source_rows(
        tmp_path,
        [("purchase_3", "three@example.com", "2026-01-03T00:00:00Z", 300)],
    )
    original_produce_event_collect = store.produce_event_collect

    def produce_event_collect_spy(*args: Any, **kwargs: Any) -> object:
        events.append("collect")
        return original_produce_event_collect(*args, **kwargs)

    monkeypatch.setattr(store, "produce_event_collect", produce_event_collect_spy)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert events[0] == "submit"
    assert "collect" not in events
    assert len(submissions[0][1]) == 1
    retried = store.get_destination_batch(batch_id=old_pending.batch_id)
    assert retried is not None
    assert retried.status == "succeeded"


def test_event_retry_replays_stored_source_range_without_ordered_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    selected: list[tuple[tuple[str, ...], DestinationScanRange | None]] = []

    def submit_recording_destination(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        request_plans = selected_request_plans or ()
        assert attempted_count == len(request_plans)
        selected.extend((plan.record_identities, plan.source_range) for plan in request_plans)
        return DestinationSubmissionEvidence(
            status="confirmed",
            attempted_count=attempted_count,
            confirmed_count=attempted_count,
            request_batch_count=len(request_plans),
            summary="Recording destination confirmed submitted event work.",
        )

    sync = _recording_event_sync(
        _event_declaration(tmp_path),
        name="purchase_imports",
        binding_name="recording_events",
        submissions=[],
        submission_hook=submit_recording_destination,
    )
    scope = destination_progress_scope(sync)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    seeded = store.list_destination_batches(scope=scope)
    retry_source_range = seeded[0].identity.source_range
    assert retry_source_range is not None
    old_pending = store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=seeded[0].batch_id,
            identity=seeded[0].identity,
            record_count=seeded[0].record_count,
            attempt_count=seeded[0].attempt_count,
        )
    )
    _append_event_source_rows(
        tmp_path,
        [("purchase_3", "three@example.com", "2026-01-03T00:00:00Z", 300)],
    )

    replay_windows: list[tuple[EventKeysetScanPosition | None, EventKeysetScanPosition | None]] = []
    original_read_event_source_window = store.read_event_source_window

    def read_event_source_window_spy(*args: Any, **kwargs: Any) -> object:
        window = kwargs["window"]
        if getattr(window, "scan_through", None) is not None:
            replay_windows.append((window.scan_after, window.scan_through))
        return original_read_event_source_window(*args, **kwargs)

    monkeypatch.setattr(store, "read_event_source_window", read_event_source_window_spy)

    def read_destination_batch_work_spy(*_: Any, **__: Any) -> object:
        raise AssertionError("Event retry must replay Source SQL, not Event ordered_work.")

    monkeypatch.setattr(store, "read_destination_batch_work", read_destination_batch_work_spy)
    selected.clear()

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert replay_windows[0] == (
        retry_source_range.lower_bound_exclusive,
        retry_source_range.upper_bound_inclusive,
    )
    assert selected
    assert selected[0][1] == retry_source_range
    assert all("purchase_3" not in record for record in selected[0][0])
    retried = store.get_destination_batch(batch_id=old_pending.batch_id)
    assert retried is not None
    assert retried.status == "succeeded"


def test_event_retry_reports_missing_source_range_rows(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _recording_event_sync(
        _event_declaration(tmp_path),
        name="purchase_imports",
        binding_name="recording_events",
        submissions=[],
    )
    scope = destination_progress_scope(sync)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    seeded = store.list_destination_batches(scope=scope)
    store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=seeded[0].batch_id,
            identity=seeded[0].identity,
            record_count=seeded[0].record_count,
            attempt_count=seeded[0].attempt_count,
        )
    )
    _replace_event_source_rows(
        tmp_path,
        [("purchase_2", "two@example.com", "2026-01-02T00:00:00Z", 200)],
    )

    with pytest.raises(
        retl.DeclarationValidationError,
        match="source may no longer retain the rows required for retry",
    ):
        retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)


def test_event_retry_reports_extra_source_range_rows(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _recording_event_sync(
        _event_declaration(tmp_path),
        name="purchase_imports",
        binding_name="recording_events",
        submissions=[],
    )
    scope = destination_progress_scope(sync)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    seeded = store.list_destination_batches(scope=scope)
    store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=seeded[0].batch_id,
            identity=seeded[0].identity,
            record_count=seeded[0].record_count,
            attempt_count=seeded[0].attempt_count,
        )
    )
    _append_event_source_rows(
        tmp_path,
        [("purchase_1b", "one-b@example.com", "2026-01-01T12:00:00Z", 150)],
    )

    with pytest.raises(
        retl.DeclarationValidationError,
        match="source may no longer retain the rows required for retry",
    ):
        retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)


def test_runner_retry_sweep_runs_once_and_does_not_retry_current_run_failures(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    submissions: list[tuple[int, tuple[str, ...]]] = []
    old_batch_ids: set[str] = set()
    old_payload_fingerprints: set[str] = set()
    seed_run = True

    def submit_once_then_fail(
        *,
        attempted_count: int,
        selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
        **_: Any,
    ) -> DestinationSubmissionEvidence:
        request_plans = selected_request_plans or ()
        batch_ids = tuple(plan.batch_id for plan in request_plans)
        submissions.append((attempted_count, batch_ids))
        nonlocal seed_run
        if seed_run:
            seed_run = False
            return DestinationSubmissionEvidence(
                status="confirmed",
                attempted_count=attempted_count,
                confirmed_count=attempted_count,
                request_batch_count=len(request_plans),
                summary="Seed run confirmed.",
            )
        if (
            request_plans
            and {plan.payload_fingerprint for plan in request_plans} <= old_payload_fingerprints
        ):
            return DestinationSubmissionEvidence(
                status="confirmed",
                attempted_count=attempted_count,
                confirmed_count=attempted_count,
                request_batch_count=len(request_plans),
                summary="Old retry sweep confirmed.",
            )
        return DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=attempted_count,
            retryable_failure_count=attempted_count,
            request_batch_count=len(request_plans),
            summary="Current run failed retryably.",
        )

    sync = _recording_state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="retry_sweep_once",
        submissions=[],
        config={"destination_in_run_retry_attempt_limit": 1},
        submission_hook=submit_once_then_fail,
        request_batch_max_rows=2,
    )
    scope = destination_progress_scope(sync)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    seeded = store.list_destination_batches(scope=scope)
    old_pending = store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=seeded[0].batch_id,
            identity=seeded[0].identity,
            attempt_count=seeded[0].attempt_count,
        )
    )
    old_batch_ids.add(old_pending.batch_id)
    old_payload_fingerprints.add(old_pending.identity.payload_fingerprint)
    submissions.clear()
    _replace_state_source_rows(
        tmp_path,
        [
            ("cust_1", "one@example.com", "pro"),
            ("cust_2", "two@example.com", "free"),
            ("cust_3", "three@example.com", "enterprise"),
        ],
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert len(submissions[0][1]) == 1
    assert len(submissions) == 2
    current_batches = [
        batch
        for batch in store.list_destination_batches(scope=scope)
        if batch.batch_id not in old_batch_ids and batch.run_id == result.run_id
    ]
    assert len(current_batches) == 1
    assert current_batches[0].status == "failed"
    assert current_batches[0].retry_eligible is True
    assert current_batches[0].attempt_count == 1


def test_runner_retry_sweep_excludes_resolved_nonretryable_and_exhausted_batches(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    submissions: list[tuple[int, tuple[str, ...]]] = []
    sync = _recording_state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=submissions,
    )
    scope = destination_progress_scope(sync)
    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    seeded = store.list_destination_batches(scope=scope)
    submissions.clear()
    retryable_seed = seeded[0]
    retryable = DestinationBatchRecord(
        batch_id=retryable_seed.batch_id,
        identity=retryable_seed.identity,
        status="failed",
        completion_state="unresolved",
        attempt_count=1,
        retry_eligible=True,
    )
    excluded = (
        _destination_batch(
            scope, index=11, label="accepted", status="accepted", completion_state="resolved"
        ),
        _destination_batch(
            scope, index=12, label="succeeded", status="succeeded", completion_state="resolved"
        ),
        _destination_batch(
            scope,
            index=13,
            label="skipped",
            status="skipped",
            completion_state="resolved",
            retry_eligible=False,
        ),
        _destination_batch(
            scope, index=14, label="nonretryable", status="failed", retry_eligible=False
        ),
        _destination_batch(
            scope,
            index=15,
            label="resolved-failed",
            status="failed",
            completion_state="resolved",
            retry_eligible=True,
        ),
        DestinationBatchRecord(
            batch_id=(
                exhausted := _destination_batch(
                    scope,
                    index=16,
                    label="exhausted",
                    status="failed",
                    retry_eligible=True,
                )
            ).batch_id,
            identity=exhausted.identity,
            status=exhausted.status,
            completion_state=exhausted.completion_state,
            attempt_count=3,
            retry_eligible=exhausted.retry_eligible,
        ),
    )
    store.upsert_destination_batches((retryable, *excluded))

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert len(submissions[0][1]) == 1
    batches = {batch.batch_id: batch for batch in store.list_destination_batches(scope=scope)}
    for batch in excluded:
        assert batches[batch.batch_id].attempt_count == batch.attempt_count


def test_runner_retry_sweep_is_limited_to_exact_destination_scope_and_retry_limit(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    submissions: list[tuple[int, tuple[str, ...]]] = []
    sync = _recording_state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=submissions,
        config={"destination_batch_retry_limit": 1},
    )
    scope = destination_progress_scope(sync)
    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    seeded = store.list_destination_batches(scope=scope)
    submissions.clear()
    matching_pending = store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=seeded[0].batch_id,
            identity=seeded[0].identity,
            attempt_count=seeded[0].attempt_count,
        )
    )
    exhausted_seed = seeded[1]
    exhausted_failed = DestinationBatchRecord(
        batch_id=exhausted_seed.batch_id,
        identity=exhausted_seed.identity,
        status="failed",
        completion_state="unresolved",
        attempt_count=1,
        retry_eligible=True,
    )
    other_scopes = (
        DestinationProgressScope(
            sync_name="customer_profiles_other",
            destination_name=scope.destination_name,
            surface=scope.surface,
            family=scope.family,
            declaration_name=scope.declaration_name,
        ),
        DestinationProgressScope(
            sync_name=scope.sync_name,
            destination_name="other_destination",
            surface=scope.surface,
            family=scope.family,
            declaration_name=scope.declaration_name,
        ),
        DestinationProgressScope(
            sync_name=scope.sync_name,
            destination_name=scope.destination_name,
            surface="other_surface",
            family=scope.family,
            declaration_name=scope.declaration_name,
        ),
        DestinationProgressScope(
            sync_name=scope.sync_name,
            destination_name=scope.destination_name,
            surface=scope.surface,
            family="event",
            declaration_name=scope.declaration_name,
        ),
        DestinationProgressScope(
            sync_name=scope.sync_name,
            destination_name=scope.destination_name,
            surface=scope.surface,
            family=scope.family,
            declaration_name="other_declaration",
        ),
    )
    other_pending = tuple(
        _destination_batch(other_scope, index=index + 10, label=f"other-pending-{index}")
        for index, other_scope in enumerate(other_scopes)
    )
    store.upsert_destination_batches((matching_pending, exhausted_failed, *other_pending))

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert len(submissions[0][1]) == 1
    matching = {batch.batch_id: batch for batch in store.list_destination_batches(scope=scope)}
    assert matching[matching_pending.batch_id].attempt_count == 2
    assert matching[exhausted_failed.batch_id].attempt_count == 1
    for other_scope, batch in zip(other_scopes, other_pending, strict=True):
        assert store.list_destination_batches(scope=other_scope) == (batch,)


def test_runner_run_many_retries_once_per_sync_destination_scope(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _state_declaration(tmp_path)
    first_submissions: list[tuple[int, tuple[str, ...]]] = []
    second_submissions: list[tuple[int, tuple[str, ...]]] = []
    first_sync = _recording_state_sync(
        declaration,
        name="customer_profiles",
        binding_name="recording_profiles",
        submissions=first_submissions,
    )
    second_sync = _recording_state_sync(
        declaration,
        name="customer_profiles_archive",
        binding_name="recording_profiles_archive",
        submissions=second_submissions,
    )
    retl.runner(name="crm_to_lifecycle", runtime_store=store).run_many([first_sync, second_sync])
    first_seeded = store.list_destination_batches(scope=destination_progress_scope(first_sync))[0]
    second_seeded = store.list_destination_batches(scope=destination_progress_scope(second_sync))[0]
    first_submissions.clear()
    second_submissions.clear()
    first_pending = store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=first_seeded.batch_id,
            identity=first_seeded.identity,
            attempt_count=first_seeded.attempt_count,
        )
    )
    second_pending = store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=second_seeded.batch_id,
            identity=second_seeded.identity,
            attempt_count=second_seeded.attempt_count,
        )
    )

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run_many([first_sync, second_sync])

    assert len(first_submissions[0][1]) == 1
    assert len(second_submissions[0][1]) == 1
    first_stored = store.get_destination_batch(batch_id=first_pending.batch_id)
    second_stored = store.get_destination_batch(batch_id=second_pending.batch_id)
    assert first_stored is not None
    assert second_stored is not None
    assert first_stored.attempt_count == 2
    assert second_stored.attempt_count == 2


def test_runner_report_preserves_pre_acceptance_submission_summary(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    failing_sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
        config={"submission_outcome": "auth_failure"},
        on_failure="stop_on_any",
    )

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        stage_batch_max_rows=1,
    ).run(failing_sync)

    assert result.status == "failed"
    assert result.syncs[0].destination_pre_acceptance_failure_category == "auth"
    assert result.sync_reports[0].destination.submission_status == "pre_acceptance_failure"
    assert result.sync_reports[0].destination.failure_category == "auth"
    assert (
        result.sync_reports[0].destination.last_error_summary
        == "Mock destination produced a pre-acceptance auth failure."
    )
    assert "Last Error Summary: Mock destination produced a pre-acceptance auth failure." in (
        result.to_text()
    )
    assert "Destination: request_batches=1, destination_batches=1" in result.to_text()
    assert "terminal_record_failure" not in result.to_text()
    batches = store.list_destination_batches(scope=destination_progress_scope(failing_sync))
    assert len(batches) == 1
    assert batches[0].status == "failed"
    assert batches[0].completion_state == "unresolved"
    assert batches[0].last_error_summary == (
        "Mock destination produced a pre-acceptance auth failure."
    )


def test_duckdb_persists_failed_run_report_and_submission_attempt_across_reopen(
    tmp_path: Path,
) -> None:
    database = _warehouse_database(tmp_path)
    store = DuckDBSqlBackend(
        database=database,
        source_schema="main",
        runtime_schema="retl",
    ).runtime_store()
    failing_sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
        config={"submission_outcome": "auth_failure"},
        on_failure="stop_on_any",
    )

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        stage_batch_max_rows=1,
    ).run(failing_sync)
    assert result.run_index is not None
    run_id = result.run_index.run_id
    attempt_id = result.sync_reports[0].attempt_id
    store.close()

    reopened = DuckDBRuntimeStore(database=database)
    run_row = reopened._connection.execute(
        """
        select run_id, runner_name, status, dry_run
        from retl.runs
        where run_id = ?
        """,
        [run_id],
    ).fetchone()
    sync_report = reopened._connection.execute(
        """
        select
            run_id,
            attempt_id,
            sync_name,
            status,
            failure_category,
            pre_acceptance_failure_count
        from retl.sync_reports
        where run_id = ?
        """,
        [run_id],
    ).fetchone()

    assert attempt_id is not None
    assert run_row == (run_id, "crm_to_lifecycle", "failed", False)
    assert sync_report == (
        run_id,
        attempt_id,
        "customer_profiles",
        "failed",
        "auth",
        1,
    )


def test_duckdb_persists_sanitized_last_error_detail_in_sync_reports(tmp_path: Path) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")
    detail = (
        "error_data.blame_field_specs[0].fields[1]=custom_data.value "
        "Authorization: Bearer secret-token"
    )
    store.record_sync_report(
        SimpleNamespace(
            run_id="run-1",
            attempt_id="attempt",
            sync_name="sync",
            report_id="run-1:attempt:sync",
            ref=SimpleNamespace(ref="sync-report:1"),
            runner_name="runner",
            declaration_name="purchase_events",
            declaration_version_id="decl:1",
            declaration_kind="event",
            destination_binding_name="destination",
            surface="events",
            status="failed",
            dry_run=False,
            phases=(SimpleNamespace(status="failed"),),
            destination=SimpleNamespace(
                status="failed",
                attempted_count=1,
                confirmed_count=0,
                accepted_count=0,
                retryable_failure_count=0,
                terminal_failure_count=0,
                pre_acceptance_failure_count=1,
                failure_category="schema",
                last_error_summary="failed",
                last_error_detail=detail,
            ),
            commit=SimpleNamespace(progress_advanced=False),
            to_dict=lambda: {"run_id": "run-1"},
        )
    )

    persisted = store._connection.execute(
        """
        select last_error_detail
        from retl.sync_reports
        where report_id = 'run-1:attempt:sync'
        """
    ).fetchone()[0]

    assert "error_data.blame_field_specs[0].fields[1]" in persisted
    assert "Authorization=[redacted]" in persisted
    assert "secret-token" not in persisted


def test_repeated_same_sync_runs_persist_distinct_sync_report_rows(tmp_path: Path) -> None:
    store = _store(tmp_path)
    declaration = _state_declaration(tmp_path)
    sync = _state_sync(
        declaration,
        name="customer_profiles",
        binding_name="mock_profiles",
    )

    first = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    assert first.run_index is not None
    _replace_state_source_rows(
        tmp_path,
        [
            ("cust_1", "one@example.com", "pro"),
            ("cust_2", "two@example.com", "free"),
            ("cust_3", "three@example.com", "growth"),
        ],
    )
    second = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    assert second.run_index is not None

    report_rows = store._connection.execute(
        """
        select run_id, report_ref
        from retl.sync_reports
        where sync_name = 'customer_profiles'
        order by created_at
        """
    ).fetchall()

    assert [row[0] for row in report_rows] == [
        first.run_index.run_id,
        second.run_index.run_id,
    ]
    assert len({row[1] for row in report_rows}) == 1


def test_run_many_collects_shared_state_declaration_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import retl.runtime.executor as executor
    import retl.state_runtime as state_runtime
    import retl.state_runtime.producer as state_producer

    declaration = _state_declaration(tmp_path)
    sync_a = _state_sync(declaration, name="customer_profiles", binding_name="mock_profiles")
    sync_b = _state_sync(declaration, name="customer_backup", binding_name="mock_backup")
    calls: list[tuple[str, str]] = []
    real_produce_state_collect = state_producer.produce_state_collect

    def spy_produce_state_collect(**kwargs: object) -> object:
        produced_declaration = cast(retl.State, kwargs["declaration"])
        calls.append((produced_declaration.name, produced_declaration.source.name))
        if len(calls) > 1:
            raise AssertionError("run_many must collect a shared declaration/source group once")
        return real_produce_state_collect(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(state_producer, "produce_state_collect", spy_produce_state_collect)
    monkeypatch.setattr(state_runtime, "produce_state_collect", spy_produce_state_collect)
    monkeypatch.setattr(executor, "produce_state_collect", spy_produce_state_collect, raising=False)

    result = retl.runner(name="crm_to_lifecycle", runtime_store=_store(tmp_path)).run_many(
        [sync_a, sync_b],
    )

    assert calls == [("customer_state", "customers")]
    assert [sync_result.sync_name for sync_result in result.syncs] == [
        "customer_profiles",
        "customer_backup",
    ]


def test_run_many_keeps_destination_scopes_independent_per_sync(tmp_path: Path) -> None:
    store = _store(tmp_path)
    declaration = _state_declaration(tmp_path)
    sync_a = _state_sync(declaration, name="customer_profiles", binding_name="mock_profiles")
    sync_b = _state_sync(declaration, name="customer_backup", binding_name="mock_backup")

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run_many([sync_a, sync_b])

    scope_a = destination_progress_scope(sync_a)
    scope_b = destination_progress_scope(sync_b)

    assert scope_a.sync_name == "customer_profiles"
    assert scope_b.sync_name == "customer_backup"
    assert scope_a != scope_b


def test_runner_dismiss_unresolved_calls_scoped_store_and_returns_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
    )
    scope = destination_progress_scope(sync)
    pending = store.upsert_destination_batch(
        _destination_batch(scope, index=0, label="pending-dismiss")
    )
    failed = store.upsert_destination_batch(
        _destination_batch(
            scope,
            index=1,
            label="failed-dismiss",
            status="failed",
            retry_eligible=True,
        )
    )
    calls: list[DestinationProgressScope] = []
    original_dismiss = store.dismiss_unresolved_destination_batches

    def spy_dismiss(
        *,
        scope: DestinationProgressScope,
    ) -> tuple[DestinationBatchRecord, ...]:
        calls.append(scope)
        return original_dismiss(scope=scope)

    monkeypatch.setattr(store, "dismiss_unresolved_destination_batches", spy_dismiss)

    dismissed = retl.runner(name="crm_to_lifecycle", runtime_store=store).dismiss_unresolved(sync)

    assert calls == [scope]
    assert {batch.batch_id for batch in dismissed} == {pending.batch_id, failed.batch_id}
    assert {batch.status for batch in dismissed} == {"skipped"}
    assert {batch.completion_state for batch in dismissed} == {"resolved"}
    assert {batch.retry_eligible for batch in dismissed} == {False}
    assert (
        tuple(store.get_destination_batch(batch_id=batch.batch_id) for batch in (pending, failed))
        == dismissed
    )
    tables = store.inspect_runtime_store()["tables"]
    assert "destination_batch_attempts" not in tables
    assert "destination_submission_attempts" not in tables
    assert "run_indexes" not in tables


def test_runner_dismiss_unresolved_only_affects_exact_destination_scope(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
    )
    scope = destination_progress_scope(sync)

    matching = store.upsert_destination_batch(_destination_batch(scope, index=0, label="matching"))
    unaffected = tuple(
        store.upsert_destination_batch(record)
        for record in (
            _destination_batch(
                DestinationProgressScope(
                    sync_name="other_customer_profiles",
                    destination_name=scope.destination_name,
                    surface=scope.surface,
                    family=scope.family,
                    declaration_name=scope.declaration_name,
                ),
                index=1,
                label="other-sync",
            ),
            _destination_batch(
                DestinationProgressScope(
                    sync_name=scope.sync_name,
                    destination_name="other_destination",
                    surface=scope.surface,
                    family=scope.family,
                    declaration_name=scope.declaration_name,
                ),
                index=2,
                label="other-destination",
            ),
            _destination_batch(
                DestinationProgressScope(
                    sync_name=scope.sync_name,
                    destination_name=scope.destination_name,
                    surface="other_surface",
                    family=scope.family,
                    declaration_name=scope.declaration_name,
                ),
                index=3,
                label="other-surface",
            ),
            _destination_batch(
                DestinationProgressScope(
                    sync_name=scope.sync_name,
                    destination_name=scope.destination_name,
                    surface=scope.surface,
                    family="event",
                    declaration_name=scope.declaration_name,
                ),
                index=4,
                label="other-family",
            ),
            _destination_batch(
                DestinationProgressScope(
                    sync_name=scope.sync_name,
                    destination_name=scope.destination_name,
                    surface=scope.surface,
                    family=scope.family,
                    declaration_name="other_declaration",
                ),
                index=5,
                label="other-declaration",
            ),
        )
    )

    dismissed = retl.runner(name="crm_to_lifecycle", runtime_store=store).dismiss_unresolved(sync)

    assert len(dismissed) == 1
    assert dismissed[0] == DestinationBatchRecord(
        batch_id=matching.batch_id,
        identity=matching.identity,
        status="skipped",
        completion_state="resolved",
        retry_eligible=False,
        completed_at=dismissed[0].completed_at,
    )
    assert dismissed[0].completed_at is not None
    assert store.get_destination_batch(batch_id=matching.batch_id) == dismissed[0]
    for batch in unaffected:
        assert store.get_destination_batch(batch_id=batch.batch_id) == batch


def test_run_many_plans_event_declaration_per_destination_scope_without_event_collect_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    declaration = _event_declaration(tmp_path)
    sync_a = _event_sync(declaration, name="purchase_imports", binding_name="mock_events")
    sync_b = _event_sync(declaration, name="purchase_backup", binding_name="mock_backup")
    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync_a)

    def reject_event_collect_scan(**_: object) -> object:
        raise AssertionError("Event runner collect must not scan the source before staging.")

    monkeypatch.setattr(store, "produce_event_collect", reject_event_collect_scan)

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run_many(
        [sync_a, sync_b],
    )

    assert [sync_result.sync_name for sync_result in result.syncs] == [
        "purchase_imports",
        "purchase_backup",
    ]
    assert [sync_result.event_import_count for sync_result in result.syncs] == [0, 2]
    assert destination_progress_scope(sync_a) != destination_progress_scope(sync_b)


def test_event_runtime_uses_destination_cursor_without_source_checkpoint_store(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _event_declaration(tmp_path)
    sync = _event_sync(declaration)

    first = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    second = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert first.syncs[0].event_import_count == 2
    assert second.syncs[0].event_import_count == 0
    assert not hasattr(store, "get_source_checkpoint")
    assert not hasattr(store, "set")


def test_run_result_run_index_and_sync_reports_expose_shared_collect_and_outcomes(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _state_declaration(tmp_path)
    sync_a = _state_sync(declaration, name="customer_profiles", binding_name="mock_profiles")
    sync_b = _state_sync(declaration, name="customer_backup", binding_name="mock_backup")

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run_many(
        [sync_a, sync_b],
    )

    assert result.run_index is not None
    assert result.run_index_reference == result.run_index.ref.ref
    assert len(result.source_groups) == 1
    assert result.source_groups[0].sync_names == ("customer_profiles", "customer_backup")
    assert len(result.sync_reports) == 2
    assert result.report_references == tuple(report.ref.ref for report in result.sync_reports)
    assert result.run_index.source_groups[0]["sync_names"] == (
        "customer_profiles",
        "customer_backup",
    )
    assert [entry.sync_name for entry in result.run_index.syncs] == [
        "customer_profiles",
        "customer_backup",
    ]


def test_state_resend_all_runner_execution_is_public_and_state_only(tmp_path: Path) -> None:
    store = _store(tmp_path)
    sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(
        sync,
        resend_all=True,
    )

    assert [sync_result.sync_name for sync_result in result.syncs] == ["customer_profiles"]
    assert result.syncs[0].operation_count == 2
    assert result.syncs[0].upsert_count == 2
    assert result.syncs[0].destination_confirmed_count == 2
    assert result.sync_reports[0].progress.stage_mode == "resend_all"


def test_state_resend_all_runner_collects_fresh_current_state_first(tmp_path: Path) -> None:
    store = _store(tmp_path)
    declaration = _state_declaration(tmp_path)
    sync = _state_sync(declaration, name="customer_profiles", binding_name="mock_profiles")

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)
    _replace_state_source_rows(
        tmp_path,
        [
            ("cust_1", "one@example.com", "enterprise"),
            ("cust_2", "two@example.com", "free"),
            ("cust_3", "three@example.com", "pro"),
        ],
    )

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(
        sync,
        resend_all=True,
    )

    assert result.syncs[0].operation_count == 3
    assert result.syncs[0].upsert_count == 3
    assert result.syncs[0].destination_confirmed_count == 3


def test_run_many_resend_all_collects_shared_state_once_and_sends_each_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import retl.runtime.executor as executor
    import retl.state_runtime as state_runtime
    import retl.state_runtime.producer as state_producer

    store = _store(tmp_path)
    declaration = _state_declaration(tmp_path)
    sync_a = _state_sync(declaration, name="customer_profiles", binding_name="mock_profiles")
    sync_b = _state_sync(declaration, name="customer_backup", binding_name="mock_backup")
    calls: list[tuple[str, str]] = []
    real_produce_state_collect = state_producer.produce_state_collect

    def spy_produce_state_collect(**kwargs: object) -> object:
        produced_declaration = cast(retl.State, kwargs["declaration"])
        calls.append((produced_declaration.name, produced_declaration.source.name))
        if len(calls) > 1:
            raise AssertionError("resend-all run_many must collect a shared State group once")
        return real_produce_state_collect(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(state_producer, "produce_state_collect", spy_produce_state_collect)
    monkeypatch.setattr(state_runtime, "produce_state_collect", spy_produce_state_collect)
    monkeypatch.setattr(executor, "produce_state_collect", spy_produce_state_collect, raising=False)

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run_many(
        [sync_a, sync_b],
        resend_all=True,
    )

    assert calls == [("customer_state", "customers")]
    assert [sync_result.sync_name for sync_result in result.syncs] == [
        "customer_profiles",
        "customer_backup",
    ]
    assert [sync_result.destination_confirmed_count for sync_result in result.syncs] == [2, 2]


def test_state_resend_all_runner_drains_paginated_current_state_pages(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
    )

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        stage_batch_max_rows=1,
    ).run(sync, resend_all=True)

    assert result.status == "succeeded"
    assert result.syncs[0].operation_count == 2
    assert result.syncs[0].destination_confirmed_count == 2
    assert result.sync_reports[0].progress.page_count == 2
    assert result.sync_reports[0].progress.stage_mode == "resend_all"
    progress = store.get_destination_progress(destination_progress_scope(sync)).position
    assert isinstance(progress, StateCurrentSnapshotScanPosition)
    assert progress.key.parts[0].value == '{"key":{"customer":"cust_2"},"target":null}'
    batches = store.list_destination_batches(scope=destination_progress_scope(sync))
    assert len(batches) == 2
    assert all(batch.identity.source_range is not None for batch in batches)


def test_state_resend_all_leaves_pending_ordered_work_for_normal_runner(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    declaration = _state_declaration(tmp_path)
    sync = _state_sync(declaration, name="customer_profiles", binding_name="mock_profiles")

    resend = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(
        sync,
        resend_all=True,
    )
    normal = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert resend.syncs[0].operation_count == 2
    assert normal.syncs[0].operation_count == 2
    assert normal.sync_reports[0].progress.stage_mode == "pending"


def test_event_resend_all_runner_execution_rejects_clearly(tmp_path: Path) -> None:
    sync = _event_sync(_event_declaration(tmp_path))

    with pytest.raises(
        retl.DeclarationValidationError,
        match=r"resend_all=True.*State.*Event Syncs.*purchase_imports",
    ):
        retl.runner(name="crm_to_lifecycle", runtime_store=_store(tmp_path)).run(
            sync,
            resend_all=True,
        )


def test_mixed_state_event_resend_all_run_many_rejects_before_mutation(tmp_path: Path) -> None:
    store = _store(tmp_path)
    state_sync = _state_sync(
        _state_declaration(tmp_path),
        name="customer_profiles",
        binding_name="mock_profiles",
    )
    event_sync = _event_sync(_event_declaration(tmp_path))

    with pytest.raises(
        retl.DeclarationValidationError,
        match=r"resend_all=True.*State.*Event Syncs.*purchase_imports",
    ):
        retl.runner(name="crm_to_lifecycle", runtime_store=store).run_many(
            [state_sync, event_sync],
            resend_all=True,
        )
