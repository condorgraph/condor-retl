from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest
import retl_reference_http.hooks as reference_hooks
from retl_reference_http.common import reference_http_config
from retl_reference_http.definitions import (
    EVENT_SURFACE,
    STATE_SURFACE,
    reference_http_connector,
)
from retl_reference_http.hooks import (
    classify_reference_http_response,
    plan_reference_http_requests,
    reference_http_fixture_response,
)

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.declarations import DestinationBinding
from retl.destinations.acknowledgements import (
    DestinationSubmissionEvidence,
)
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.request_batch import DryRunSubmissionPlan, RequestBatchPlan
from retl.destinations.surfaces import DestinationSurface
from retl.errors import DeclarationValidationError
from retl.events.reconcile import EventReconcileEvidence
from retl.runtime import destination_progress_scope
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl.sync_runtime.submission import sync_destination


@dataclass
class RecordingTransport:
    responses: list[HttpResponse]
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return self.responses.pop(0)


@dataclass
class FailingTransport:
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        raise RuntimeError("transport unavailable")


def _coordinated_state_page() -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "collect_id": "00000000-000b-7000-8000-000000000000",
                "sequence_order": 0,
                "state_key": {"customer_id": "1"},
                "identifiers": [{"type": "email", "value": "one@example.test"}],
                "payload": {"status": "active"},
            },
            {
                "operation": "upsert",
                "record_identity": "customer-2",
                "collect_id": "00000000-000b-7000-8000-000000000000",
                "sequence_order": 1,
                "state_key": {"customer_id": "2"},
                "identifiers": [{"type": "email", "value": "two@example.test"}],
                "payload": {"status": "active"},
            },
        ]
    ).to_batches()[0]


def _state_sync(*, connector: object, transport: RecordingTransport) -> retl.Sync:
    declaration = retl.state(
        name="reference_http_customers",
        source=retl.source(name="customers", query="select * from customers"),
        key={"customer_id": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"status": "status"},
    )
    return retl.sync(
        name="reference_http_customer_profiles",
        declaration=declaration,
        destination=DestinationBinding(
            binding_name="reference_http",
            destination_ref="retl/reference-http",
            connector=connector,
            config={
                "request_batch_max_rows": 1,
                "transport": transport,  # type: ignore[dict-item]
            },
        ),
        surface=STATE_SURFACE,
    )


def test_reference_http_connector_declares_state_event_surfaces_and_auth() -> None:
    connector = reference_http_connector()

    assert connector.connector_ref == "retl/reference-http"
    assert connector.auth_modes[0].name == "none"
    assert set(connector.surface_names) == {STATE_SURFACE, EVENT_SURFACE}
    state_surface = connector.surface(STATE_SURFACE)
    event_surface = connector.surface(EVENT_SURFACE)
    assert state_surface.declaration_family == "state"
    assert state_surface.accepted_identifier_types == ("email",)
    assert state_surface.identifier_requirements[0].match == "all_of"
    assert event_surface.declaration_family == "event"
    assert event_surface.accepted_identifier_types == ("email",)
    assert event_surface.identifier_requirements[0].match == "all_of"
    assert callable(connector.submission_hook)


def test_reference_http_loads_namespace_config_and_keeps_transport_explicit() -> None:
    retl.configure(
        config_resolver=retl.MappingConfigResolver(
            {
                "destinations.reference_http.base_url": "https://reference.example.test",
                "destinations.reference_http.request_batch_max_rows": "2",
            }
        )
    )
    try:
        connector = reference_http_connector()
        registry = retl.destinations.DestinationRegistry()
        registry.register(connector)

        destination = retl.destinations.load(
            "retl/reference-http",
            binding_name="reference_http",
            config_namespace="destinations.reference_http",
            config={"transport": object()},  # type: ignore[dict-item]
            registry=registry,
        )

        config = reference_http_config(destination)
        assert config.base_url == "https://reference.example.test"
        assert config.request_batch_max_rows == 2
        assert "transport" in destination.config
        assert "destinations.reference_http.transport" not in destination.config
    finally:
        retl.configure(config_resolver=None)


def test_reference_http_receipt_classification_is_bounded() -> None:
    confirmed = classify_reference_http_response(reference_http_fixture_response("confirmed"))
    retryable = classify_reference_http_response(
        reference_http_fixture_response("retryable_failure")
    )
    terminal = classify_reference_http_response(
        reference_http_fixture_response("terminal_record_failure")
    )
    pre_acceptance = classify_reference_http_response(
        reference_http_fixture_response("pre_acceptance_failure")
    )

    assert confirmed.outcome == "confirmed"
    assert retryable.outcome == "retryable_failure"
    assert retryable.retry_after_seconds == 30
    assert terminal.outcome == "terminal_record_failure"
    assert pre_acceptance.outcome == "pre_acceptance_failure"
    assert pre_acceptance.partner_message is not None
    assert "secret-token" not in pre_acceptance.partner_message


def test_planned_submission_evidence_carries_request_batch_count() -> None:
    evidence = DestinationSubmissionEvidence.planned(
        attempted_count=3,
        dry_run=True,
        request_batch_count=2,
        summary="planned",
    )

    assert evidence.status == "planned"
    assert evidence.request_batch_count == 2


def test_reference_http_plans_page_wise_request_batches() -> None:
    connector = reference_http_connector()
    binding = DestinationBinding(
        binding_name="reference_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={"request_batch_max_rows": 2},
    )
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "state_key": {"customer_id": "1"},
                "identifiers": [{"type": "email", "value": "one@example.test"}],
                "payload": {"status": "active"},
            },
            {
                "operation": "upsert",
                "record_identity": "customer-2",
                "state_key": {"customer_id": "2"},
                "identifiers": [{"type": "email", "value": "two@example.test"}],
                "payload": {"status": "active"},
            },
            {
                "operation": "upsert",
                "record_identity": "customer-3",
                "state_key": {"customer_id": "3"},
                "identifiers": [{"type": "email", "value": "three@example.test"}],
                "payload": {"status": "active"},
            },
        ]
    ).to_batches()[0]

    plan = plan_reference_http_requests(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="reference_sync", operation_pages=(page,)),
        ),
    )

    assert plan.request_count == 2
    assert [request.row_count for request in plan.plans] == [2, 1]
    assert all(request.request.path == "/reference-http/state-records" for request in plan.plans)


def test_reference_http_non_dry_run_sends_bounded_payload_batches() -> None:
    connector = reference_http_connector()
    transport = RecordingTransport(
        responses=[
            reference_http_fixture_response("confirmed"),
            reference_http_fixture_response("confirmed"),
        ],
        requests=[],
    )
    binding = DestinationBinding(
        binding_name="reference_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={"request_batch_max_rows": 1, "transport": transport},  # type: ignore[dict-item]
    )
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "state_key": {"customer_id": "1"},
                "identifiers": [{"type": "email", "value": "one@example.test"}],
                "payload": {"status": "active"},
            },
            {
                "operation": "upsert",
                "record_identity": "customer-2",
                "state_key": {"customer_id": "2"},
                "identifiers": [{"type": "email", "value": "two@example.test"}],
                "payload": {"status": "active"},
            },
        ]
    ).to_batches()[0]

    hook = connector.submission_hook
    assert hook is not None
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="reference_sync", operation_pages=(page,)),
    )
    selected_plan = plan_reference_http_requests(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        reconciled=reconciled,
    ).plans[:1]
    assert len(selected_plan) == 1
    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(STATE_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=selected_plan[0].row_count,
            dry_run=False,
            resolved_auth=object(),
            reconciled=reconciled,
            selected_request_plans=selected_plan,
        ),
    )

    assert evidence.status == "confirmed"
    assert evidence.confirmed_count == selected_plan[0].row_count
    assert evidence.request_batch_count == 1
    sent_identities = [
        cast(dict[str, Any], request.json_body)["records"][0]["record_identity"]
        for request in transport.requests
    ]
    assert sent_identities == ["customer-1"]


def test_reference_http_non_dry_run_joins_base_url_and_applies_resolved_auth() -> None:
    connector = reference_http_connector()
    transport = RecordingTransport(
        responses=[reference_http_fixture_response("confirmed")],
        requests=[],
    )
    binding = DestinationBinding(
        binding_name="reference_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "base_url": "https://collector.example.test/root/",
            "request_batch_max_rows": 10,
            "transport": transport,  # type: ignore[dict-item]
        },
    )
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "state_key": {"customer_id": "1"},
                "identifiers": [{"type": "email", "value": "one@example.test"}],
                "payload": {"status": "active"},
            },
        ]
    ).to_batches()[0]
    hook = connector.submission_hook
    assert hook is not None
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(
            sync_name="reference_sync",
            operation_pages=(_coordinated_state_page(),),
        ),
    )
    selected_plan = plan_reference_http_requests(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        reconciled=reconciled,
    ).plans[:1]
    assert len(selected_plan) == 1

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(STATE_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=1,
            dry_run=False,
            resolved_auth=SimpleNamespace(headers={"Authorization": "Bearer token"}),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(sync_name="reference_sync", operation_pages=(page,)),
            ),
        ),
    )

    assert evidence.status == "confirmed"
    assert transport.requests[0].url == (
        "https://collector.example.test/root/reference-http/state-records"
    )
    assert transport.requests[0].headers["Authorization"] == "Bearer token"


def test_reference_http_invalid_config_uses_declaration_validation_errors() -> None:
    connector = reference_http_connector()

    with pytest.raises(DeclarationValidationError, match="base_url"):
        plan_reference_http_requests(
            binding=DestinationBinding(
                binding_name="reference_primary",
                destination_ref=connector.connector_ref,
                connector=connector,
                config={"base_url": "https://example.test/path?access_token=secret"},
            ),
            surface=connector.surface(STATE_SURFACE),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    sync_name="reference_sync",
                    operation_pages=(_coordinated_state_page(),),
                ),
            ),
        )

    with pytest.raises(DeclarationValidationError, match="request_batch_max_rows"):
        plan_reference_http_requests(
            binding=DestinationBinding(
                binding_name="reference_primary",
                destination_ref=connector.connector_ref,
                connector=connector,
                config={"request_batch_max_rows": 0},
            ),
            surface=connector.surface(STATE_SURFACE),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    sync_name="reference_sync",
                    operation_pages=(_coordinated_state_page(),),
                ),
            ),
        )


def test_reference_http_invalid_transport_config_raises_validation_error() -> None:
    connector = reference_http_connector()
    hook = connector.submission_hook
    assert hook is not None

    with pytest.raises(DeclarationValidationError, match="transport"):
        hook(
            binding=DestinationBinding(
                binding_name="reference_primary",
                destination_ref=connector.connector_ref,
                connector=connector,
                config={"transport": object()},  # type: ignore[dict-item]
            ),
            surface=connector.surface(STATE_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=1,
            dry_run=False,
            resolved_auth=object(),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    sync_name="reference_sync",
                    operation_pages=(_coordinated_state_page(),),
                ),
            ),
        )


def test_reference_http_transport_exception_returns_pre_acceptance_evidence() -> None:
    connector = reference_http_connector()
    transport = FailingTransport(requests=[])
    binding = DestinationBinding(
        binding_name="reference_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={"request_batch_max_rows": 1, "transport": transport},  # type: ignore[dict-item]
    )
    hook = connector.submission_hook
    assert hook is not None
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(
            sync_name="reference_sync",
            operation_pages=(_coordinated_state_page(),),
        ),
    )
    selected_plan = plan_reference_http_requests(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        reconciled=reconciled,
    ).plans[:1]
    assert len(selected_plan) == 1

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(STATE_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=selected_plan[0].row_count,
            dry_run=False,
            resolved_auth=object(),
            reconciled=reconciled,
            selected_request_plans=selected_plan,
        ),
    )

    assert evidence.status == "pre_acceptance_failure"
    assert evidence.pre_acceptance_failure_category == "transport"
    assert evidence.pre_acceptance_failure_count == 1
    assert evidence.request_batch_count == 1
    assert "RuntimeError" in evidence.summary
    assert len(transport.requests) == 1


def test_reference_http_empty_selected_request_plans_sends_no_requests() -> None:
    connector = reference_http_connector()
    transport = RecordingTransport(
        responses=[reference_http_fixture_response("confirmed")],
        requests=[],
    )
    binding = DestinationBinding(
        binding_name="reference_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={"request_batch_max_rows": 1, "transport": transport},  # type: ignore[dict-item]
    )
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "state_key": {"customer_id": "1"},
                "identifiers": [{"type": "email", "value": "one@example.test"}],
                "payload": {"status": "active"},
            },
        ]
    ).to_batches()[0]

    hook = connector.submission_hook
    assert hook is not None
    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(STATE_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=0,
            dry_run=False,
            resolved_auth=object(),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(sync_name="reference_sync", operation_pages=(page,)),
            ),
            selected_request_plans=(),
        ),
    )

    assert evidence.status == "planned"
    assert evidence.request_batch_count == 0
    assert transport.requests == []


def test_reference_http_selected_plans_drive_dry_run_and_transport_evidence_without_replanning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = reference_http_connector()
    binding = DestinationBinding(
        binding_name="reference_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={"request_batch_max_rows": 1},
    )
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="reference_sync", operation_pages=(_coordinated_state_page(),)),
    )
    selected_plan = plan_reference_http_requests(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        reconciled=reconciled,
    ).plans[:1]

    def fail_replanning(**_: object) -> object:
        raise AssertionError("selected request plans must not be planned again")

    monkeypatch.setattr(reference_hooks, "plan_reference_http_requests", fail_replanning)
    hook = connector.submission_hook
    assert hook is not None

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(STATE_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=1,
            dry_run=True,
            resolved_auth=object(),
            reconciled=reconciled,
            selected_request_plans=selected_plan,
        ),
    )

    assert evidence.status == "planned"
    assert evidence.dry_run is True
    assert evidence.request_batch_count == 1
    assert "1 request batch(es) for 1 record(s)" in evidence.summary

    missing_transport = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(STATE_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=1,
            dry_run=False,
            resolved_auth=object(),
            reconciled=reconciled,
            selected_request_plans=selected_plan,
        ),
    )

    assert missing_transport.status == "pre_acceptance_failure"
    assert missing_transport.pre_acceptance_failure_category == "transport"
    assert missing_transport.request_batch_count == 1


def test_reference_http_sync_dry_run_reuses_ledger_request_plans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_plan = reference_hooks.plan_reference_http_requests
    planned: list[tuple[RequestBatchPlan, ...]] = []

    def counting_plan(
        *,
        binding: DestinationBinding,
        surface: DestinationSurface,
        reconciled: StateReconcileEvidence | EventReconcileEvidence,
    ) -> DryRunSubmissionPlan:
        plan = original_plan(binding=binding, surface=surface, reconciled=reconciled)
        planned.append(plan.plans)
        return plan

    def fail_replanning(**_: object) -> object:
        raise AssertionError("dry-run submission must reuse the selected ledger request plans")

    connector = replace(reference_http_connector(), batch_planning_hook=counting_plan)
    monkeypatch.setattr(reference_hooks, "plan_reference_http_requests", fail_replanning)
    transport = RecordingTransport(responses=[], requests=[])
    sync = _state_sync(connector=connector, transport=transport)
    reconciled = SimpleNamespace(
        phase="reconcile",
        status="succeeded",
        sync_name=sync.name,
        scope=destination_progress_scope(sync),
        operation_pages=(_coordinated_state_page(),),
        operation_count=2,
    )

    evidence = sync_destination(
        sync=sync,
        reconciled=cast(Any, reconciled),
        dry_run=True,
        runtime_store=DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb"),
        run_id="run-1",
        attempt_id="attempt-1",
        page_index=1,
    )

    assert len(planned) == 1
    assert evidence.submission.dry_run is True
    assert evidence.submission.request_batch_count == len(planned[0])
    assert evidence.destination_batch_count == len(planned[0])
    assert transport.requests == []


def test_reference_http_sync_dry_run_reuses_empty_connector_request_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    planned_count = 0

    def empty_plan(
        *,
        binding: DestinationBinding,
        surface: DestinationSurface,
        reconciled: StateReconcileEvidence | EventReconcileEvidence,
    ) -> DryRunSubmissionPlan:
        nonlocal planned_count
        _ = binding, surface, reconciled
        planned_count += 1
        return DryRunSubmissionPlan(
            dry_run=True,
            plans=(),
            record_count=2,
            request_count=0,
        )

    def fail_replanning(**_: object) -> object:
        raise AssertionError("empty dry-run request plans must not be planned again")

    connector = replace(reference_http_connector(), batch_planning_hook=empty_plan)
    monkeypatch.setattr(reference_hooks, "plan_reference_http_requests", fail_replanning)
    transport = RecordingTransport(responses=[], requests=[])
    sync = _state_sync(connector=connector, transport=transport)
    reconciled = SimpleNamespace(
        phase="reconcile",
        status="succeeded",
        sync_name=sync.name,
        scope=destination_progress_scope(sync),
        operation_pages=(_coordinated_state_page(),),
        operation_count=2,
    )

    evidence = sync_destination(
        sync=sync,
        reconciled=cast(Any, reconciled),
        dry_run=True,
        runtime_store=DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb"),
        run_id="run-1",
        attempt_id="attempt-1",
        page_index=1,
    )

    assert planned_count == 1
    assert evidence.submission.dry_run is True
    assert evidence.submission.request_batch_count == 0
    assert transport.requests == []


def test_reference_http_submission_uses_selected_plans_that_seeded_batch_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_plan = reference_hooks.plan_reference_http_requests
    planned: list[tuple[RequestBatchPlan, ...]] = []

    def counting_plan(
        *,
        binding: DestinationBinding,
        surface: DestinationSurface,
        reconciled: StateReconcileEvidence | EventReconcileEvidence,
    ) -> DryRunSubmissionPlan:
        plan = original_plan(binding=binding, surface=surface, reconciled=reconciled)
        planned.append(plan.plans)
        return plan

    def fail_replanning(**_: object) -> object:
        raise AssertionError("submission hook must reuse the selected ledger request plans")

    connector = replace(reference_http_connector(), batch_planning_hook=counting_plan)
    monkeypatch.setattr(reference_hooks, "plan_reference_http_requests", fail_replanning)
    transport = RecordingTransport(
        responses=[
            reference_http_fixture_response("confirmed"),
            reference_http_fixture_response("confirmed"),
        ],
        requests=[],
    )
    sync = _state_sync(connector=connector, transport=transport)
    reconciled = SimpleNamespace(
        phase="reconcile",
        status="succeeded",
        sync_name=sync.name,
        scope=destination_progress_scope(sync),
        operation_pages=(_coordinated_state_page(),),
        operation_count=2,
    )

    evidence = sync_destination(
        sync=sync,
        reconciled=cast(Any, reconciled),
        dry_run=False,
        runtime_store=DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb"),
        run_id="run-1",
        attempt_id="attempt-1",
        page_index=1,
    )

    assert len(planned) == 1
    selected_plans = planned[0]
    assert evidence.submission.status == "confirmed"
    assert evidence.submission.request_batch_count == len(selected_plans)
    assert len(transport.requests) == len(selected_plans)
    assert [
        cast(dict[str, Any], request.json_body)["records"][0]["record_identity"]
        for request in transport.requests
    ] == [plan.record_identities[0] for plan in selected_plans]
    assert [request.json_body for request in transport.requests] == [
        plan.request.json_body for plan in selected_plans
    ]

    batches = evidence.destination_batches
    assert len(batches) == len(selected_plans)
    assert all(plan.source_range is not None for plan in selected_plans)
    assert [batch.identity.payload_fingerprint for batch in batches] == [
        plan.payload_fingerprint for plan in selected_plans
    ]
    assert [batch.identity.target_request_fingerprint for batch in batches] == [
        plan.target_request_fingerprint for plan in selected_plans
    ]
    assert [batch.identity.source_range for batch in batches] == [
        plan.source_range for plan in selected_plans
    ]
