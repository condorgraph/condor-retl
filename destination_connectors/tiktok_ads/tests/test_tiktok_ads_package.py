from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest
from retl_tiktok_ads.common import tiktok_ads_config
from retl_tiktok_ads.definitions import CUSTOM_AUDIENCES_SURFACE, tiktok_ads_connector
from retl_tiktok_ads.hooks import classify_tiktok_ads_response, plan_tiktok_ads_requests

import retl
from retl.auth import apply_auth
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.request_batch import RequestBatchPlan
from retl.destinations.targets import RemoteTarget, TargetMapping
from retl.errors import DeclarationValidationError
from retl.runtime import destination_progress_scope
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl.sync_runtime.submission import sync_destination


@dataclass
class RecordingTransport:
    requests: list[HttpRequest]
    uploads: list[dict[str, object]] = field(default_factory=list)

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return HttpResponse(status_code=200, json_body={"code": 0, "request_id": "tt-1"})

    def upload_audience_file(self, **kwargs: object) -> HttpResponse:
        self.uploads.append(dict(kwargs))
        return HttpResponse(
            status_code=200,
            json_body={"code": 0, "data": {"file_path": f"file_{len(self.uploads)}"}},
        )


@dataclass
class QueueTransport:
    responses: list[HttpResponse]
    requests: list[HttpRequest]
    upload_responses: list[HttpResponse] = field(default_factory=list)
    uploads: list[dict[str, object]] = field(default_factory=list)

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError(f"Unexpected TikTok Ads request: {request.method} {request.url}")
        return self.responses.pop(0)

    def upload_audience_file(self, **kwargs: object) -> HttpResponse:
        self.uploads.append(dict(kwargs))
        if not self.upload_responses:
            raise AssertionError("Unexpected TikTok Ads audience file upload")
        return self.upload_responses.pop(0)


@dataclass
class FailingTransport:
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        raise RuntimeError("transport unavailable")

    def upload_audience_file(self, **kwargs: object) -> HttpResponse:
        raise RuntimeError("transport unavailable")


def _binding(*, transport: object | None = None) -> DestinationBinding:
    connector = tiktok_ads_connector()
    config: dict[str, JSONValue] = {"advertiser_id": "1234567890"}
    if transport is not None:
        config["transport"] = transport  # type: ignore[assignment]
    return DestinationBinding(
        binding_name="tiktok_ads_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        credentials={"access_token": retl.secrets.literal("access-token")},
        config=config,
        target_mappings=(
            TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="aud_123")),
        ),
    )


def _audience_page() -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers": [{"type": "email", "value": "One@Example.Test"}],
                "payload": {},
            },
            {
                "operation": "remove",
                "record_identity": "customer-2",
                "target": "vip",
                "state_key": {"customer_id": "2"},
                "identifiers": [{"type": "phone_e164", "value": "+15551234567"}],
                "payload": {},
            },
            {
                "operation": "upsert",
                "record_identity": "customer-3",
                "target": "vip",
                "state_key": {"customer_id": "3"},
                "identifiers": [{"type": "mobile_advertising_id", "value": " A0B1 "}],
                "payload": {},
            },
        ]
    ).to_batches()[0]


def _canonical_audience_page(*, target: str = "vip") -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "collect_id": "00000000-0001-7000-8000-000000000000",
                "sequence_order": 0,
                "target_json": {"value": target},
                "key_json": {"customer_id": "1"},
                "identifiers_json": [{"type": "email", "value": "one@example.test"}],
                "payload_json": {},
            }
        ]
    ).to_batches()[0]


def _reconciled(page: pa.RecordBatch | None = None) -> StateReconcileEvidence:
    return cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="tiktok_ads_sync", operation_pages=(page or _audience_page(),)),
    )


def _audience_declaration() -> retl.State:
    return retl.state(
        name="tiktok_custom_audience",
        source=retl.source(
            name="customers",
            mode="snapshot",
            query="select customer_id, email, audience_key from customers",
        ),
        key={"customer_id": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
    )


def _managed_sync(*, transport: QueueTransport) -> retl.Sync:
    connector = tiktok_ads_connector()
    return retl.sync(
        name="tiktok_ads_custom_audience_sync",
        declaration=_audience_declaration(),
        destination=DestinationBinding(
            binding_name="tiktok_ads_primary",
            destination_ref=connector.connector_ref,
            connector=connector,
            credentials={"access_token": retl.secrets.literal("access-token")},
            config={
                "advertiser_id": "1234567890",
                "transport": transport,  # type: ignore[dict-item]
            },
        ),
        surface=CUSTOM_AUDIENCES_SURFACE,
    )


def test_tiktok_ads_connector_declares_custom_audience_surface_and_auth() -> None:
    connector = tiktok_ads_connector()
    surface = connector.surface(CUSTOM_AUDIENCES_SURFACE)

    assert connector.connector_ref == "retl/tiktok-ads"
    assert surface.declaration_family == "state"
    assert surface.supported_operations == ("upsert", "remove")
    assert surface.target_mode == "required"
    assert surface.supports_managed_targets is True
    assert surface.accepted_identifier_types == ("email", "phone_e164", "mobile_advertising_id")
    assert surface.delivery_outcome == "accepted"
    assert connector.auth_modes[0].kind == "api_key"
    assert connector.auth_modes[0].key == "Access-Token"
    assert callable(connector.batch_planning_hook)
    assert callable(connector.submission_hook)
    assert callable(connector.managed_target_client_hook)


def test_tiktok_ads_config_validates_binding_config() -> None:
    config = tiktok_ads_config(
        DestinationBinding(
            binding_name="tiktok_ads_primary",
            destination_ref="retl/tiktok-ads",
            connector=tiktok_ads_connector(),
            config={
                "advertiser_id": " 123 ",
                "api_version": "/" + "v" + "1.3/",
                "mobile_advertising_id_type": "gaid_sha256",
            },
        )
    )

    assert config.advertiser_id == "123"
    assert config.api_version == "v" + "1.3"
    assert config.mobile_advertising_id_type == "GAID_SHA256"

    invalid_configs: tuple[dict[str, JSONValue], ...] = (
        {},
        {"advertiser_id": "123", "api_version": "v" + "1.2"},
        {"advertiser_id": "123", "mobile_advertising_id_type": "EMAIL_MD5"},
    )
    for raw_config in invalid_configs:
        with pytest.raises(DeclarationValidationError):
            tiktok_ads_config(
                DestinationBinding(
                    binding_name="tiktok_ads_primary",
                    destination_ref="retl/tiktok-ads",
                    connector=tiktok_ads_connector(),
                    config=raw_config,
                )
            )


def test_tiktok_ads_loads_credential_and_config_namespaces_without_auth_in_plan() -> None:
    retl.configure(
        config_resolver=retl.MappingConfigResolver(
            {
                "destinations.tiktok_ads.advertiser_id": "1234567890",
                "destinations.tiktok_ads.api_version": "v" + "1.3",
            }
        )
    )
    try:
        connector = tiktok_ads_connector()
        registry = retl.destinations.DestinationRegistry()
        registry.register(connector)
        binding = retl.destinations.load(
            "retl/tiktok-ads",
            binding_name="tiktok_ads_primary",
            credential_namespace="destinations.tiktok_ads",
            config_namespace="destinations.tiktok_ads",
            target_mappings=(
                TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="aud_123")),
            ),
            registry=registry,
        )

        assert binding.credentials == {
            "access_token": retl.secrets["destinations.tiktok_ads.access_token"],
        }
        plan = plan_tiktok_ads_requests(
            binding=binding,
            surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
            reconciled=_reconciled(),
        )

        rendered_plan = str(plan.plans)
        assert "access-token" not in rendered_plan
        assert "Access-Token" not in rendered_plan
    finally:
        retl.configure(config_resolver=None)


def test_tiktok_ads_custom_audiences_plan_dmp_file_batches() -> None:
    connector = tiktok_ads_connector()

    plan = plan_tiktok_ads_requests(
        binding=_binding(),
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=_reconciled(),
    )

    assert plan.record_count == 3
    assert plan.request_count == 3
    assert [request.operation for request in plan.plans] == ["upsert", "remove", "upsert"]
    bodies = [cast(dict[str, Any], request.request.json_body) for request in plan.plans]
    assert [body["action"] for body in bodies] == ["APPEND", "REMOVE", "APPEND"]
    assert [body["custom_audience_id"] for body in bodies] == ["aud_123", "aud_123", "aud_123"]
    assert bodies[0]["advertiser_id"] == "1234567890"
    assert bodies[0]["calculate_type"] == "EMAIL_SHA256"
    assert bodies[1]["calculate_type"] == "PHONE_SHA256"
    assert bodies[2]["calculate_type"] == "MAID_SHA256"
    assert bodies[0]["identifiers"][0] != "One@Example.Test"
    assert len(bodies[0]["identifiers"][0]) == 64


def test_tiktok_ads_custom_audiences_split_at_file_row_limit() -> None:
    connector = tiktok_ads_connector()
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"customer-{index}",
            "target": "vip",
            "state_key": {"customer_id": str(index)},
            "identifiers": [{"type": "email", "value": f"user{index}@example.test"}],
            "payload": {},
        }
        for index in range(100_001)
    ]
    page = pa.Table.from_pylist(rows).to_batches()[0]

    plan = plan_tiktok_ads_requests(
        binding=_binding(),
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=_reconciled(page),
    )

    assert plan.record_count == 100_001
    assert plan.request_count == 2
    assert [request.row_count for request in plan.plans] == [100_000, 1]


def test_tiktok_ads_submission_applies_access_token_header() -> None:
    connector = tiktok_ads_connector()
    transport = RecordingTransport(requests=[])
    binding = _binding(transport=transport)
    resolved_auth = apply_auth(
        mode=connector.auth_modes[0], values={"access_token": "access-token"}
    )
    plan = plan_tiktok_ads_requests(
        binding=binding,
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=_reconciled(),
    )

    submission_hook = connector.submission_hook
    assert submission_hook is not None
    evidence = cast(
        DestinationSubmissionEvidence,
        submission_hook(
            binding=binding,
            surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
            delivery_outcome="accepted",
            attempted_count=1,
            dry_run=False,
            resolved_auth=resolved_auth,
            reconciled=_reconciled(),
            selected_request_plans=(cast(RequestBatchPlan, plan.plans[0]),),
        ),
    )

    assert evidence.status == "accepted"
    assert evidence.accepted_count == 1
    assert transport.uploads
    assert transport.uploads[0]["advertiser_id"] == "1234567890"
    assert transport.uploads[0]["calculate_type"] == "EMAIL_SHA256"
    assert transport.requests[0].method == "POST"
    assert transport.requests[0].url == (
        "https://business-api.tiktok.com/open_api/" + "v" + "1.3/dmp/custom_audience/update/"
    )
    assert transport.requests[0].headers["Access-Token"] == "access-token"
    assert transport.requests[0].json_body == {
        "advertiser_id": "1234567890",
        "custom_audience_id": "aud_123",
        "file_paths": ["file_1"],
        "calculate_type": "EMAIL_SHA256",
        "action": "APPEND",
    }


def test_tiktok_ads_submission_transport_failure_is_pre_acceptance() -> None:
    connector = tiktok_ads_connector()
    transport = FailingTransport(requests=[])
    binding = _binding(transport=transport)
    resolved_auth = apply_auth(
        mode=connector.auth_modes[0], values={"access_token": "access-token"}
    )
    plan = plan_tiktok_ads_requests(
        binding=binding,
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=_reconciled(),
    )

    submission_hook = connector.submission_hook
    assert submission_hook is not None
    evidence = cast(
        DestinationSubmissionEvidence,
        submission_hook(
            binding=binding,
            surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
            delivery_outcome="accepted",
            attempted_count=1,
            dry_run=False,
            resolved_auth=resolved_auth,
            reconciled=_reconciled(),
            selected_request_plans=(cast(RequestBatchPlan, plan.plans[0]),),
        ),
    )

    assert evidence.status == "pre_acceptance_failure"
    assert evidence.pre_acceptance_failure_category == "transport"


def test_tiktok_ads_nonzero_business_code_is_terminal_failure() -> None:
    classification = classify_tiktok_ads_response(
        HttpResponse(
            status_code=200,
            json_body={
                "code": 40001,
                "message": "No permission to operate advertiser.",
                "request_id": "req-1",
            },
        )
    )

    assert classification.outcome == "terminal_record_failure"
    assert classification.partner_error_code == "40001"
    assert "No permission" in (classification.partner_message or "")


def test_tiktok_ads_managed_target_finds_and_creates_custom_audience() -> None:
    connector = tiktok_ads_connector()
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=200,
                json_body={
                    "code": 0,
                    "data": {
                        "list": [
                            {
                                "custom_audience_name": "vip",
                                "custom_audience_id": "aud_existing",
                                "audience_type": "CUSTOMER_FILE",
                                "status": "READY",
                            }
                        ],
                        "page_info": {"total_page": 1},
                    },
                },
            ),
            HttpResponse(
                status_code=200,
                json_body={
                    "code": 0,
                    "data": {"custom_audience_id": "aud_created"},
                    "request_id": "create-1",
                },
            ),
        ],
        requests=[],
        upload_responses=[
            HttpResponse(
                status_code=200,
                json_body={"code": 0, "data": {"file_path": "seed_file_path"}},
            ),
        ],
    )
    binding = _binding(transport=transport)
    resolved_auth = apply_auth(
        mode=connector.auth_modes[0], values={"access_token": "access-token"}
    )
    managed_target_client_hook = connector.managed_target_client_hook
    assert managed_target_client_hook is not None
    client = cast(
        Any,
        managed_target_client_hook(
            binding=binding,
            surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
            resolved_auth=resolved_auth,
        ),
    )

    assert client is not None
    found = client.find_target("vip")
    created = client.create_target("new_vip", display_name="New VIP")

    assert found == RemoteTarget(
        remote_id="aud_existing",
        display_name="vip",
        metadata={
            "kind": "tiktok_custom_audience",
            "audience_type": "CUSTOMER_FILE",
            "status": "READY",
        },
    )
    assert created.remote_id == "aud_created"
    assert transport.requests[0].method == "GET"
    assert transport.requests[0].query["advertiser_id"] == "1234567890"
    assert transport.requests[1].method == "POST"
    assert transport.uploads
    assert transport.uploads[0]["calculate_type"] == "EMAIL_SHA256"
    assert transport.requests[1].url == (
        "https://business-api.tiktok.com/open_api/" + "v" + "1.3/dmp/custom_audience/create/"
    )
    assert transport.requests[1].json_body == {
        "custom_audience_name": "New VIP",
        "advertiser_id": "1234567890",
        "file_paths": ["seed_file_path"],
        "calculate_type": "EMAIL_SHA256",
    }


def test_tiktok_ads_sync_resolves_managed_target_before_submission() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=200,
                json_body={"code": 0, "data": {"list": [], "page_info": {"total_page": 1}}},
            ),
            HttpResponse(
                status_code=200,
                json_body={"code": 0, "data": {"custom_audience_id": "aud_created"}},
            ),
            HttpResponse(status_code=200, json_body={"code": 0, "request_id": "update-1"}),
        ],
        upload_responses=[
            HttpResponse(
                status_code=200,
                json_body={"code": 0, "data": {"file_path": "seed_file_path"}},
            ),
            HttpResponse(
                status_code=200,
                json_body={"code": 0, "data": {"file_path": "member_file_path"}},
            ),
        ],
        requests=[],
    )
    sync = _managed_sync(transport=transport)
    store = DuckDBRuntimeStore(database=":memory:")

    phase = sync_destination(
        sync=sync,
        dry_run=False,
        runtime_store=store,
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(
                phase="reconcile",
                sync_name=sync.name,
                operation_pages=(_canonical_audience_page(target="new_vip"),),
                status="succeeded",
                scope=destination_progress_scope(sync),
            ),
        ),
    )

    assert phase.submission.status == "accepted"
    assert len(transport.requests) == 3
    assert len(transport.uploads) == 2
    body = cast(dict[str, Any], transport.requests[2].json_body)
    assert body["custom_audience_id"] == "aud_created"
    assert body["file_paths"] == ["member_file_path"]
