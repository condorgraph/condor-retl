from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pytest
from retl_klaviyo.definitions import (
    LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE,
    LIST_MEMBERSHIPS_SURFACE,
    PROFILES_SURFACE,
    klaviyo_connector,
)
from retl_klaviyo.hooks import plan_klaviyo_requests, submit_klaviyo_destination

from retl.auth import apply_auth
from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.surfaces import DestinationSurface
from retl.destinations.targets import ManagedTargetClient, RemoteTarget, TargetMapping
from retl.events.reconcile import EventReconcileEvidence
from retl.state_runtime.reconcile import StateReconcileEvidence


@dataclass
class RecordingTransport:
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return HttpResponse(
            status_code=202,
            json_body={"data": {"type": "profile-bulk-import-job", "id": "job_123"}},
        )


@dataclass
class StaticTransport:
    response: HttpResponse
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return self.response


@dataclass
class FailingTransport:
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        raise RuntimeError("transport unavailable")


@dataclass
class QueueTransport:
    responses: list[HttpResponse]
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError(f"Unexpected Klaviyo request: {request.method} {request.url}")
        return self.responses.pop(0)


def _profiles_page() -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "profile-1",
                "collect_id": "00000000-0001-7000-8000-000000000000",
                "sequence_order": 0,
                "key_json": {"profile_key": "profile-1"},
                "identifiers_json": [
                    {"type": "email", "value": "Ada@Example.Test"},
                    {"type": "external_id", "value": "customer-1"},
                ],
                "payload_json": {
                    "first_name": "Ada",
                    "favorite_color": "blue",
                    "unused": None,
                    "properties": {"loyalty_tier": "gold", "clear_me": None},
                },
            },
            {
                "operation": "upsert",
                "record_identity": "profile-2",
                "collect_id": "00000000-0001-7000-8000-000000000000",
                "sequence_order": 1,
                "key_json": {"profile_key": "profile-2"},
                "identifiers_json": [{"type": "phone_e164", "value": "+15551234567"}],
                "payload_json": {"last_name": "Lovelace"},
            },
        ]
    ).to_batches()[0]


def _reconciled(page: pa.RecordBatch | None = None) -> StateReconcileEvidence:
    return cast(
        StateReconcileEvidence,
        SimpleNamespace(
            sync_name="klaviyo_profile_sync", operation_pages=(page or _profiles_page(),)
        ),
    )


def _binding(
    *,
    transport: object | None = None,
    target_mappings: tuple[TargetMapping, ...] = (),
) -> DestinationBinding:
    connector = klaviyo_connector()
    config: dict[str, object] = {"api_revision": "2026-04-15"}
    if transport is not None:
        config["transport"] = transport
    return DestinationBinding(
        binding_name="klaviyo_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config=cast(dict[str, JSONValue], config),
        target_mappings=target_mappings,
    )


def _surface(name: str = PROFILES_SURFACE) -> DestinationSurface:
    return cast(Mapping[str, DestinationSurface], klaviyo_connector().surfaces)[name]


def _list_target_mapping() -> tuple[TargetMapping, ...]:
    return (
        TargetMapping(
            logical_target="retl_test_list",
            remote=RemoteTarget(remote_id="list_123"),
        ),
    )


def test_connector_metadata_surfaces_and_auth() -> None:
    connector = klaviyo_connector()
    surfaces = cast(Mapping[str, DestinationSurface], connector.surfaces)
    surface = surfaces[PROFILES_SURFACE]

    assert connector.connector_ref == "retl/klaviyo"
    assert surface.declaration_family == "state"
    assert surface.supported_operations == ("upsert",)
    assert surface.target_mode == "unsupported"
    assert surface.delivery_outcome == "accepted"
    assert surface.accepted_identifier_types == ("email", "phone_e164", "external_id")
    list_memberships = surfaces[LIST_MEMBERSHIPS_SURFACE]
    assert list_memberships.supported_operations == ("upsert",)
    assert list_memberships.target_mode == "required"
    assert list_memberships.supports_managed_targets is True
    assert list_memberships.accepted_identifier_types == (
        "email",
        "phone_e164",
        "external_id",
        "klaviyo_profile_id",
    )
    assert list_memberships.delivery_outcome == "accepted"
    by_profile_id = surfaces[LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE]
    assert by_profile_id.supported_operations == ("upsert", "remove")
    assert by_profile_id.target_mode == "required"
    assert by_profile_id.supports_managed_targets is True
    assert by_profile_id.accepted_identifier_types == ("klaviyo_profile_id",)
    assert by_profile_id.delivery_outcome == "succeeded"
    assert connector.config_namespace_fields == ("api_revision",)
    assert callable(connector.managed_target_client_hook)
    auth_mode = connector.auth_modes[0]
    assert auth_mode.kind == "api_key"
    assert auth_mode.required_fields == ("private_api_key",)
    assert auth_mode.key == "Authorization"
    assert auth_mode.prefix == "Klaviyo-API-Key "


def test_plan_profiles_bulk_import_request() -> None:
    plan = plan_klaviyo_requests(
        binding=_binding(),
        surface=_surface(),
        reconciled=_reconciled(),
    )

    assert plan.record_count == 2
    assert plan.request_count == 1
    request = plan.plans[0].request
    assert request.method == "POST"
    assert request.path == "/api/profile-bulk-import-jobs"
    assert request.headers["revision"] == "2026-04-15"
    assert request.headers["content-type"] == "application/vnd.api+json"
    assert request.json_body == {
        "data": {
            "type": "profile-bulk-import-job",
            "attributes": {
                "profiles": {
                    "data": [
                        {
                            "type": "profile",
                            "attributes": {
                                "email": "Ada@Example.Test",
                                "external_id": "customer-1",
                                "first_name": "Ada",
                                "properties": {
                                    "favorite_color": "blue",
                                    "loyalty_tier": "gold",
                                },
                            },
                        },
                        {
                            "type": "profile",
                            "attributes": {
                                "phone_number": "+15551234567",
                                "last_name": "Lovelace",
                            },
                        },
                    ],
                },
            },
        },
    }


def test_plan_splits_at_klaviyo_profile_limit() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"profile-{index}",
            "key_json": {"profile_key": str(index)},
            "identifiers_json": [{"type": "email", "value": f"user{index}@example.test"}],
            "payload_json": {},
        }
        for index in range(10_001)
    ]
    page = pa.Table.from_pylist(rows).to_batches()[0]

    plan = plan_klaviyo_requests(
        binding=_binding(),
        surface=_surface(),
        reconciled=_reconciled(page),
    )

    assert plan.record_count == 10_001
    assert plan.request_count == 2
    assert [request.row_count for request in plan.plans] == [10_000, 1]


def test_plan_list_memberships_uses_bulk_import_with_list_relationship() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "profile-1",
                "target": "retl_test_list",
                "key_json": {"profile_key": "profile-1"},
                "identifiers_json": [{"type": "email", "value": "member@example.test"}],
                "payload_json": {"first_name": "List"},
            }
        ]
    ).to_batches()[0]

    plan = plan_klaviyo_requests(
        binding=_binding(target_mappings=_list_target_mapping()),
        surface=_surface(LIST_MEMBERSHIPS_SURFACE),
        reconciled=_reconciled(page),
    )

    assert plan.record_count == 1
    assert plan.request_count == 1
    request = plan.plans[0].request
    assert request.method == "POST"
    assert request.path == "/api/profile-bulk-import-jobs"
    assert request.json_body == {
        "data": {
            "type": "profile-bulk-import-job",
            "attributes": {
                "profiles": {
                    "data": [
                        {
                            "type": "profile",
                            "attributes": {
                                "email": "member@example.test",
                                "first_name": "List",
                            },
                        }
                    ]
                }
            },
            "relationships": {
                "lists": {
                    "data": [
                        {
                            "type": "list",
                            "id": "list_123",
                        }
                    ]
                }
            },
        }
    }


def test_plan_list_memberships_by_profile_id_adds_and_removes_relationships() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "profile-1-upsert",
                "target": "retl_test_list",
                "key_json": {"profile_key": "profile-1"},
                "identifiers_json": [{"type": "klaviyo_profile_id", "value": "profile_1"}],
                "payload_json": {},
            },
            {
                "operation": "remove",
                "record_identity": "profile-2-remove",
                "target": "retl_test_list",
                "key_json": {"profile_key": "profile-2"},
                "identifiers_json": [{"type": "klaviyo_profile_id", "value": "profile_2"}],
                "payload_json": {},
            },
        ]
    ).to_batches()[0]

    plan = plan_klaviyo_requests(
        binding=_binding(target_mappings=_list_target_mapping()),
        surface=_surface(LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE),
        reconciled=_reconciled(page),
    )

    assert plan.record_count == 2
    assert plan.request_count == 2
    assert [request.request.method for request in plan.plans] == ["POST", "DELETE"]
    assert [request.request.path for request in plan.plans] == [
        "/api/lists/list_123/relationships/profiles",
        "/api/lists/list_123/relationships/profiles",
    ]
    assert [request.request.json_body for request in plan.plans] == [
        {"data": [{"type": "profile", "id": "profile_1"}]},
        {"data": [{"type": "profile", "id": "profile_2"}]},
    ]


def test_plan_list_memberships_by_profile_id_splits_at_relationship_limit() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"profile-{index}",
            "target": "retl_test_list",
            "key_json": {"profile_key": str(index)},
            "identifiers_json": [{"type": "klaviyo_profile_id", "value": f"profile_{index}"}],
            "payload_json": {},
        }
        for index in range(1_001)
    ]
    page = pa.Table.from_pylist(rows).to_batches()[0]

    plan = plan_klaviyo_requests(
        binding=_binding(target_mappings=_list_target_mapping()),
        surface=_surface(LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE),
        reconciled=_reconciled(page),
    )

    assert plan.record_count == 1_001
    assert plan.request_count == 2
    assert [request.row_count for request in plan.plans] == [1_000, 1]


def test_submit_dry_run_does_not_send_request() -> None:
    transport = RecordingTransport([])
    evidence = submit_klaviyo_destination(
        binding=_binding(transport=transport),
        surface=_surface(),
        delivery_outcome="accepted",
        attempted_count=2,
        dry_run=True,
        resolved_auth=SimpleNamespace(headers={"Authorization": "Klaviyo-API-Key secret"}),
        reconciled=_reconciled(),
    )

    assert evidence.status == "planned"
    assert evidence.request_batch_count == 1
    assert transport.requests == []


def test_submit_uses_selected_plan_and_resolved_auth() -> None:
    transport = RecordingTransport([])
    binding = _binding(transport=transport)
    plan = plan_klaviyo_requests(binding=binding, surface=_surface(), reconciled=_reconciled())
    auth = apply_auth(
        mode=klaviyo_connector().auth_modes[0],
        values={"private_api_key": "private-key"},
    )

    evidence = submit_klaviyo_destination(
        binding=binding,
        surface=_surface(),
        delivery_outcome="accepted",
        attempted_count=2,
        dry_run=False,
        resolved_auth=auth,
        reconciled=cast(
            StateReconcileEvidence | EventReconcileEvidence,
            SimpleNamespace(sync_name="klaviyo_profile_sync", operation_pages=()),
        ),
        selected_request_plans=plan.plans,
    )

    assert evidence.status == "accepted"
    assert evidence.accepted_count == 2
    assert evidence.request_batch_count == 1
    assert evidence.remote_handles[0].kind == "klaviyo_profile_bulk_import_job"
    assert evidence.remote_handles[0].value == "job_123"
    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert request.url == "https://a.klaviyo.com/api/profile-bulk-import-jobs"
    assert request.headers["Authorization"] == "Klaviyo-API-Key private-key"
    assert request.headers["revision"] == "2026-04-15"


def test_submit_list_membership_relationship_204_is_confirmed() -> None:
    transport = StaticTransport(HttpResponse(status_code=204, json_body={}), [])
    binding = _binding(transport=transport, target_mappings=_list_target_mapping())
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "profile-1",
                "target": "retl_test_list",
                "key_json": {"profile_key": "profile-1"},
                "identifiers_json": [{"type": "klaviyo_profile_id", "value": "profile_1"}],
                "payload_json": {},
            }
        ]
    ).to_batches()[0]

    evidence = submit_klaviyo_destination(
        binding=binding,
        surface=_surface(LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE),
        delivery_outcome="succeeded",
        attempted_count=1,
        dry_run=False,
        resolved_auth=SimpleNamespace(headers={"Authorization": "Klaviyo-API-Key private-key"}),
        reconciled=_reconciled(page),
    )

    assert evidence.status == "confirmed"
    assert evidence.confirmed_count == 1
    assert evidence.request_batch_count == 1
    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert request.method == "POST"
    assert request.url == "https://a.klaviyo.com/api/lists/list_123/relationships/profiles"
    assert request.json_body == {"data": [{"type": "profile", "id": "profile_1"}]}


def test_managed_list_target_client_finds_and_creates_lists() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=200,
                json_body={
                    "data": [
                        {
                            "type": "list",
                            "id": "list_existing",
                            "attributes": {"name": "Newsletter"},
                        }
                    ],
                    "links": {"next": None},
                },
            ),
            HttpResponse(
                status_code=201,
                json_body={
                    "data": {
                        "type": "list",
                        "id": "list_created",
                        "attributes": {"name": "Churn Risk"},
                    }
                },
            ),
        ],
        requests=[],
    )
    connector = klaviyo_connector()
    hook = connector.managed_target_client_hook
    assert hook is not None
    binding = _binding(transport=transport)
    client = cast(
        ManagedTargetClient,
        hook(
            binding=binding,
            surface=_surface(LIST_MEMBERSHIPS_SURFACE),
            resolved_auth=SimpleNamespace(headers={"Authorization": "Klaviyo-API-Key private-key"}),
        ),
    )

    existing = client.find_target("Newsletter")
    assert existing == RemoteTarget(
        remote_id="list_existing",
        display_name="Newsletter",
        metadata={"kind": "klaviyo_list"},
    )
    created = client.create_target("Churn Risk", display_name="Churn Risk")
    assert created == RemoteTarget(
        remote_id="list_created",
        display_name="Churn Risk",
        metadata={"kind": "klaviyo_list"},
    )
    assert [request.method for request in transport.requests] == ["GET", "POST"]
    assert transport.requests[0].url == "https://a.klaviyo.com/api/lists"
    assert transport.requests[0].query == {
        "fields[list]": "name",
        "page[size]": "10",
    }
    assert transport.requests[1].json_body == {
        "data": {
            "type": "list",
            "attributes": {
                "name": "Churn Risk",
            },
        },
    }


def test_managed_target_client_is_not_exposed_for_profiles_surface() -> None:
    connector = klaviyo_connector()
    hook = connector.managed_target_client_hook
    assert hook is not None

    assert (
        hook(
            binding=_binding(),
            surface=_surface(PROFILES_SURFACE),
            resolved_auth=SimpleNamespace(headers={}),
        )
        is None
    )


def test_schema_failure_is_pre_acceptance_with_redacted_detail() -> None:
    transport = StaticTransport(
        HttpResponse(
            status_code=400,
            json_body={
                "errors": [
                    {
                        "title": "Invalid input.",
                        "detail": "api_key=private-key bad email",
                        "code": "invalid",
                    }
                ]
            },
        ),
        [],
    )

    evidence = submit_klaviyo_destination(
        binding=_binding(transport=transport),
        surface=_surface(),
        delivery_outcome="accepted",
        attempted_count=2,
        dry_run=False,
        resolved_auth=SimpleNamespace(headers={"Authorization": "Klaviyo-API-Key private-key"}),
        reconciled=_reconciled(),
    )

    assert evidence.status == "pre_acceptance_failure"
    assert evidence.pre_acceptance_failure_category == "schema"
    assert evidence.http_status == 400
    assert evidence.partner_error_detail is not None
    assert "private-key" not in evidence.partner_error_detail


def test_retryable_and_transport_failures() -> None:
    retry_transport = StaticTransport(
        HttpResponse(status_code=429, headers={"retry-after": "30"}, json_body={}),
        [],
    )
    retry_evidence = submit_klaviyo_destination(
        binding=_binding(transport=retry_transport),
        surface=_surface(),
        delivery_outcome="accepted",
        attempted_count=2,
        dry_run=False,
        resolved_auth=SimpleNamespace(headers={}),
        reconciled=_reconciled(),
    )
    assert retry_evidence.status == "retryable_failure"
    assert retry_evidence.retryable_failure_count == 2

    failing_transport = FailingTransport([])
    failure_evidence = submit_klaviyo_destination(
        binding=_binding(transport=failing_transport),
        surface=_surface(),
        delivery_outcome="accepted",
        attempted_count=2,
        dry_run=False,
        resolved_auth=SimpleNamespace(headers={}),
        reconciled=_reconciled(),
    )
    assert failure_evidence.status == "pre_acceptance_failure"
    assert failure_evidence.pre_acceptance_failure_category == "transport"


def test_missing_bulk_import_identifier_fails_before_transport() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "profile-1",
                "key_json": {"profile_key": "profile-1"},
                "identifiers_json": [],
                "payload_json": {"first_name": "No Identifier"},
            }
        ]
    ).to_batches()[0]

    with pytest.raises(ValueError, match="email, phone_e164, external_id"):
        plan_klaviyo_requests(
            binding=_binding(),
            surface=_surface(),
            reconciled=_reconciled(page),
        )


def test_list_membership_remove_without_profile_id_fails_before_transport() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "operation": "remove",
                "record_identity": "profile-1",
                "target": "retl_test_list",
                "key_json": {"profile_key": "profile-1"},
                "identifiers_json": [{"type": "email", "value": "member@example.test"}],
                "payload_json": {},
            }
        ]
    ).to_batches()[0]

    with pytest.raises(ValueError, match="klaviyo_profile_id"):
        plan_klaviyo_requests(
            binding=_binding(target_mappings=_list_target_mapping()),
            surface=_surface(LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE),
            reconciled=_reconciled(page),
        )
