from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest
from retl_bing_ads.common import bing_ads_config
from retl_bing_ads.definitions import CUSTOMER_LISTS_SURFACE, bing_ads_connector
from retl_bing_ads.hooks import classify_bing_ads_response, plan_bing_ads_requests

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.compatibility import DestinationCompatibilityError
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.request_batch import RequestBatchPlan
from retl.destinations.targets import (
    RemoteTarget,
    TargetMapping,
    TargetRegistryRecord,
    registry_key,
)
from retl.errors import DeclarationValidationError
from retl.runtime import destination_progress_scope
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl.sync_runtime.submission import sync_destination


@dataclass
class RecordingTransport:
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return HttpResponse(
            status_code=200,
            headers={"TrackingId": f"track-{len(self.requests)}"},
            json_body={},
        )


@dataclass
class QueueTransport:
    responses: list[HttpResponse]
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError(f"Unexpected Bing Ads request: {request.method} {request.url}")
        return self.responses.pop(0)


@dataclass
class FailingTransport:
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        raise RuntimeError("transport unavailable")


def _bing_ads_binding(*, transport: object | None = None) -> DestinationBinding:
    connector = bing_ads_connector()
    config: dict[str, JSONValue] = {
        "customer_account_id": "111",
        "customer_id": "222",
    }
    if transport is not None:
        config["transport"] = transport  # type: ignore[assignment]
    return DestinationBinding(
        binding_name="bing_ads_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        credentials={
            "access_token": retl.secrets.literal("access-token"),
            "developer_token": retl.secrets.literal("developer-token"),
        },
        config=config,
        target_mappings=(
            TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="987")),
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
                "identifiers": [{"type": "email", "value": "two@example.test"}],
                "payload": {},
            },
            {
                "operation": "upsert",
                "record_identity": "customer-3",
                "target": "vip",
                "state_key": {"customer_id": "3"},
                "identifiers": [{"type": "mobile_advertising_id", "value": " madid-1 "}],
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


def _audience_declaration() -> retl.State:
    return retl.state(
        name="bing_ads_customer_list",
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
    connector = bing_ads_connector()
    return retl.sync(
        name="bing_ads_customer_list_sync",
        declaration=_audience_declaration(),
        destination=DestinationBinding(
            binding_name="bing_ads_primary",
            destination_ref=connector.connector_ref,
            connector=connector,
            credentials={
                "access_token": retl.secrets.literal("access-token"),
                "developer_token": retl.secrets.literal("developer-token"),
            },
            config={
                "customer_account_id": "111",
                "customer_id": "222",
                "transport": transport,  # type: ignore[dict-item]
            },
        ),
        surface=CUSTOMER_LISTS_SURFACE,
    )


def test_bing_ads_connector_declares_customer_lists_surface_and_auth() -> None:
    connector = bing_ads_connector()

    assert connector.connector_ref == "retl/bing-ads"
    assert connector.auth_modes[0].name == "microsoft_advertising"
    assert connector.auth_modes[0].required_fields == ("access_token", "developer_token")
    assert connector.surface(CUSTOMER_LISTS_SURFACE).target_mode == "required"
    assert connector.surface(CUSTOMER_LISTS_SURFACE).supports_managed_targets is True
    assert callable(connector.batch_planning_hook)
    assert callable(connector.submission_hook)
    assert callable(connector.managed_target_client_hook)


def test_bing_ads_loads_credential_and_config_namespaces_without_auth_in_plan() -> None:
    retl.configure(
        config_resolver=retl.MappingConfigResolver(
            {
                "destinations.bing_ads.customer_account_id": "111",
                "destinations.bing_ads.customer_id": "222",
                "destinations.bing_ads.api_version": "v13",
                "destinations.bing_ads.membership_duration": "-1",
                "destinations.bing_ads.accept_customer_match_terms": "true",
            }
        )
    )
    try:
        connector = bing_ads_connector()
        registry = retl.destinations.DestinationRegistry()
        registry.register(connector)
        binding = retl.destinations.load(
            "retl/bing-ads",
            binding_name="bing_ads_primary",
            credential_namespace="destinations.bing_ads",
            config_namespace="destinations.bing_ads",
            target_mappings=(
                TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="987")),
            ),
            registry=registry,
        )

        assert binding.credentials == {
            "access_token": retl.secrets["destinations.bing_ads.access_token"],
            "developer_token": retl.secrets["destinations.bing_ads.developer_token"],
        }
        assert bing_ads_config(binding).membership_duration == -1
        assert bing_ads_config(binding).accept_customer_match_terms is True
        plan = plan_bing_ads_requests(
            binding=binding,
            surface=connector.surface(CUSTOMER_LISTS_SURFACE),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(sync_name="bing_ads_sync", operation_pages=(_audience_page(),)),
            ),
        )

        rendered_plan = str(plan.plans)
        assert "access-token" not in rendered_plan
        assert "developer-token" not in rendered_plan
        assert "Authorization" not in rendered_plan
    finally:
        retl.configure(config_resolver=None)


def test_bing_ads_loads_toml_scalar_config_namespace(tmp_path: Path) -> None:
    path = tmp_path / "retl.toml"
    path.write_text(
        """
        [destinations.bing_ads]
        customer_account_id = "111"
        customer_id = "222"
        api_version = "v13"
        membership_duration = -1
        accept_customer_match_terms = true
        """,
        encoding="utf-8",
    )
    retl.configure(config_resolver=retl.TomlConfigResolver(path))
    try:
        connector = bing_ads_connector()
        registry = retl.destinations.DestinationRegistry()
        registry.register(connector)
        binding = retl.destinations.load(
            "retl/bing-ads",
            binding_name="bing_ads_primary",
            credential_namespace="destinations.bing_ads",
            config_namespace="destinations.bing_ads",
            registry=registry,
        )

        config = bing_ads_config(binding)
        assert config.membership_duration == -1
        assert config.accept_customer_match_terms is True
    finally:
        retl.configure(config_resolver=None)


def test_bing_ads_config_validates_binding_config() -> None:
    connector = bing_ads_connector()
    config = bing_ads_config(
        DestinationBinding(
            binding_name="bing_ads_primary",
            destination_ref=connector.connector_ref,
            connector=connector,
            config={
                "customer_account_id": " 111 ",
                "customer_id": " 222 ",
                "api_version": "/v13/",
                "target_scope": "Customer",
                "membership_duration": " -1 ",
                "accept_customer_match_terms": "false",
            },
        )
    )

    assert config.customer_account_id == "111"
    assert config.customer_id == "222"
    assert config.api_version == "v13"
    assert config.target_scope == "Customer"
    assert config.membership_duration == -1
    assert config.accept_customer_match_terms is False

    invalid_configs: tuple[dict[str, JSONValue], ...] = (
        {},
        {"customer_account_id": "111", "customer_id": "222", "api_version": "v14"},
        {"customer_account_id": "111", "customer_id": "222", "membership_duration": 391},
        {"customer_account_id": "111", "customer_id": "222", "membership_duration": "forever"},
        {
            "customer_account_id": "111",
            "customer_id": "222",
            "accept_customer_match_terms": "yes",
        },
    )
    for raw_config in invalid_configs:
        with pytest.raises(DeclarationValidationError):
            bing_ads_config(
                DestinationBinding(
                    binding_name="bing_ads_primary",
                    destination_ref=connector.connector_ref,
                    connector=connector,
                    config=raw_config,
                )
            )


def test_bing_ads_customer_lists_plans_membership_batches_by_operation_and_subtype() -> None:
    connector = bing_ads_connector()

    plan = plan_bing_ads_requests(
        binding=_bing_ads_binding(),
        surface=connector.surface(CUSTOMER_LISTS_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="bing_ads_sync", operation_pages=(_audience_page(),)),
        ),
    )

    assert plan.request_count == 3
    assert [request.operation for request in plan.plans] == ["upsert", "remove", "upsert"]
    bodies = [cast(dict[str, Any], request.request.json_body) for request in plan.plans]
    assert [body["CustomerListUserData"]["ActionType"] for body in bodies] == [
        "Add",
        "Remove",
        "Add",
    ]
    assert [body["CustomerListUserData"]["CustomerListItemSubType"] for body in bodies] == [
        "Email",
        "Email",
        "MobileAdvertisingId",
    ]
    assert bodies[0]["CustomerListUserData"]["AudienceId"] == "987"
    assert bodies[0]["CustomerListUserData"]["AcceptCustomerMatchTerm"] is True
    assert "One@Example.Test" not in str(bodies[0])
    assert bodies[2]["CustomerListUserData"]["CustomerListItems"] == ["madid-1"]


def test_bing_ads_preserves_prehashed_email_and_counts_repeated_identifiers() -> None:
    first_hash = "A" * 64
    second_hash = "b" * 64
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers": [
                    {"type": "email", "value": f" {first_hash} "},
                    {"type": "email", "value": second_hash},
                ],
                "payload": {},
            }
        ]
    ).to_batches()[0]
    connector = bing_ads_connector()

    plan = plan_bing_ads_requests(
        binding=_bing_ads_binding(),
        surface=connector.surface(CUSTOMER_LISTS_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="bing_ads_sync", operation_pages=(page,)),
        ),
    )

    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    assert body["CustomerListUserData"]["CustomerListItems"] == [first_hash.lower(), second_hash]
    assert plan.plans[0].request_item_count == 2
    assert plan.plans[0].request_item_counts == (2,)


def test_bing_ads_counts_canonical_identifiers_json_strings_for_batching() -> None:
    rows = [
        {
            "operation": "upsert",
            "record_identity": f"customer-{index}",
            "target": "vip",
            "state_key": {"customer_id": str(index)},
            "identifiers_json": '[{"type": "email", "value": "user-%04d@example.test"}]' % index,
            "payload": {},
        }
        for index in range(1001)
    ]
    page = pa.Table.from_pylist(rows).to_batches()[0]
    connector = bing_ads_connector()

    plan = plan_bing_ads_requests(
        binding=_bing_ads_binding(),
        surface=connector.surface(CUSTOMER_LISTS_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="bing_ads_sync", operation_pages=(page,)),
        ),
    )

    assert [request.request_item_count for request in plan.plans] == [1000, 1]


def test_bing_ads_managed_customer_list_reuses_existing_remote_target() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=200,
                json_body={
                    "Audiences": [
                        {"Id": "123", "Name": "other", "Type": "CustomerList"},
                        {
                            "Id": "987",
                            "Name": "vip",
                            "Type": "CustomerList",
                            "Scope": "Account",
                        },
                    ]
                },
            ),
            HttpResponse(status_code=200, headers={"TrackingId": "members-1"}, json_body={}),
        ],
        requests=[],
    )
    sync = _managed_sync(transport=transport)
    store = DuckDBRuntimeStore(database=":memory:")

    result = sync_destination(
        sync=sync,
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(
                phase="reconcile",
                sync_name=sync.name,
                operation_pages=(_canonical_audience_page(),),
                status="succeeded",
                scope=destination_progress_scope(sync),
            ),
        ),
        dry_run=False,
        runtime_store=store,
    )

    assert result.target_resolution is not None
    assert result.target_resolution.managed_reused_count == 1
    binding = cast(DestinationBinding, sync.destination)
    record = store.get(
        registry_key(binding=binding, surface=CUSTOMER_LISTS_SURFACE, logical_target="vip")
    )
    assert record is not None
    assert record.remote.remote_id == "987"
    assert [request.method for request in transport.requests] == ["POST", "POST"]
    assert (
        transport.requests[0].url
        == "https://campaign.api.bingads.microsoft.com/CampaignManagement/v13/Audiences/QueryByIds"
    )
    assert transport.requests[0].json_body == {
        "AudienceIds": None,
        "Type": "CustomerList",
        "ReturnAdditionalFields": None,
    }
    assert transport.requests[1].url == (
        "https://campaign.api.bingads.microsoft.com/"
        "CampaignManagement/v13/CustomerListUserData/Apply"
    )


def test_bing_ads_managed_customer_list_creates_missing_target_before_membership() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"Audiences": []}),
            HttpResponse(status_code=200, json_body={"AudienceIds": ["999"]}),
            HttpResponse(status_code=200, headers={"TrackingId": "members-1"}, json_body={}),
        ],
        requests=[],
    )
    sync = _managed_sync(transport=transport)
    store = DuckDBRuntimeStore(database=":memory:")

    result = sync_destination(
        sync=sync,
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(
                phase="reconcile",
                sync_name=sync.name,
                operation_pages=(_canonical_audience_page(),),
                status="succeeded",
                scope=destination_progress_scope(sync),
            ),
        ),
        dry_run=False,
        runtime_store=store,
    )

    assert result.target_resolution is not None
    assert result.target_resolution.managed_created_count == 1
    create_request = transport.requests[1]
    assert create_request.headers["Authorization"] == "Bearer access-token"
    assert create_request.headers["DeveloperToken"] == "developer-token"
    assert create_request.headers["CustomerAccountId"] == "111"
    assert create_request.headers["CustomerId"] == "222"
    assert create_request.json_body == {
        "Audiences": [
            {
                "Description": "Managed by RETL for `vip`.",
                "MembershipDuration": -1,
                "Name": "vip",
                "ParentId": "111",
                "Scope": "Account",
                "Type": "CustomerList",
            }
        ]
    }
    assert (
        cast(dict[str, Any], transport.requests[2].json_body)["CustomerListUserData"]["AudienceId"]
        == "999"
    )


def test_bing_ads_managed_customer_list_uses_runtime_registry_without_lookup() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(status_code=200, headers={"TrackingId": "members-1"}, json_body={})
        ],
        requests=[],
    )
    sync = _managed_sync(transport=transport)
    binding = cast(DestinationBinding, sync.destination)
    store = DuckDBRuntimeStore(database=":memory:")
    store.put(
        TargetRegistryRecord(
            key=registry_key(
                binding=binding,
                surface=CUSTOMER_LISTS_SURFACE,
                logical_target="vip",
            ),
            remote=RemoteTarget(remote_id="stored-987", display_name="vip"),
        )
    )

    result = sync_destination(
        sync=sync,
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(
                phase="reconcile",
                sync_name=sync.name,
                operation_pages=(_canonical_audience_page(),),
                status="succeeded",
                scope=destination_progress_scope(sync),
            ),
        ),
        dry_run=False,
        runtime_store=store,
    )

    assert result.target_resolution is not None
    assert result.target_resolution.registry_count == 1
    assert len(transport.requests) == 1
    body = cast(dict[str, Any], transport.requests[0].json_body)
    assert body["CustomerListUserData"]["AudienceId"] == "stored-987"


def test_bing_ads_managed_customer_list_partner_error_fails_target_resolution() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=400,
                json_body={
                    "PartialErrors": [
                        {
                            "Code": 8003,
                            "ErrorCode": "CustomerListTermsAndConditionsNotAccepted",
                            "Message": "terms not accepted",
                        }
                    ]
                },
            )
        ],
        requests=[],
    )
    sync = _managed_sync(transport=transport)

    with pytest.raises(DestinationCompatibilityError, match="terms not accepted"):
        sync_destination(
            sync=sync,
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    phase="reconcile",
                    sync_name=sync.name,
                    operation_pages=(_canonical_audience_page(),),
                    status="succeeded",
                    scope=destination_progress_scope(sync),
                ),
            ),
            dry_run=False,
            runtime_store=DuckDBRuntimeStore(database=":memory:"),
        )


def test_bing_ads_managed_customer_list_auth_error_is_actionable() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=401,
                json_body={"Message": "AuthenticationTokenExpired"},
                body_text="AuthenticationTokenExpired",
            )
        ],
        requests=[],
    )
    sync = _managed_sync(transport=transport)

    with pytest.raises(DestinationCompatibilityError) as exc_info:
        sync_destination(
            sync=sync,
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    phase="reconcile",
                    sync_name=sync.name,
                    operation_pages=(_canonical_audience_page(target="sample retl"),),
                    status="succeeded",
                    scope=destination_progress_scope(sync),
                ),
            ),
            dry_run=False,
            runtime_store=DuckDBRuntimeStore(database=":memory:"),
        )

    message = str(exc_info.value)
    assert "target=`sample retl`" in message
    assert "action=find_target" in message
    assert "http_status=401" in message
    assert "category=auth" in message
    assert "summary=AuthenticationTokenExpired" in message
    assert "partner_detail=AuthenticationTokenExpired" in message


def test_bing_ads_non_dry_run_uses_selected_request_plan_and_headers() -> None:
    connector = bing_ads_connector()
    transport = RecordingTransport(requests=[])
    binding = _bing_ads_binding(transport=transport)
    resolved_auth = SimpleNamespace(
        mode="microsoft_advertising",
        headers={
            "Authorization": "Bearer access-token",
            "DeveloperToken": "developer-token",
        },
    )
    hook = connector.submission_hook
    assert hook is not None
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="bing_ads_sync", operation_pages=(_audience_page(),)),
    )
    selected_plan: tuple[RequestBatchPlan, ...] = plan_bing_ads_requests(
        binding=binding,
        surface=connector.surface(CUSTOMER_LISTS_SURFACE),
        reconciled=reconciled,
    ).plans[:1]

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(CUSTOMER_LISTS_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=selected_plan[0].row_count,
            dry_run=False,
            resolved_auth=resolved_auth,
            reconciled=reconciled,
            selected_request_plans=selected_plan,
        ),
    )

    assert evidence.status == "confirmed"
    assert evidence.confirmed_count == selected_plan[0].row_count
    assert evidence.remote_handles[0].kind == "bing_ads_tracking_id"
    request = transport.requests[0]
    assert request.headers["Authorization"] == "Bearer access-token"
    assert request.headers["DeveloperToken"] == "developer-token"
    assert request.headers["CustomerAccountId"] == "111"
    assert request.headers["CustomerId"] == "222"


def test_bing_ads_partial_errors_are_terminal_record_failures() -> None:
    classification = classify_bing_ads_response(
        HttpResponse(
            status_code=200,
            json_body={
                "PartialErrors": [
                    {
                        "Code": 123,
                        "ErrorCode": "InvalidCustomerListItem",
                        "Message": "invalid customer list item",
                    }
                ]
            },
        )
    )

    assert classification.outcome == "terminal_record_failure"
    assert classification.partner_error_code == "InvalidCustomerListItem"
    assert (
        classification.partner_message
        == "invalid customer list item code=123 error_code=InvalidCustomerListItem"
    )


def test_bing_ads_transport_failure_is_pre_acceptance_failure() -> None:
    connector = bing_ads_connector()
    binding = _bing_ads_binding(transport=FailingTransport(requests=[]))
    hook = connector.submission_hook
    assert hook is not None
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="bing_ads_sync", operation_pages=(_audience_page(),)),
    )
    selected_plan = plan_bing_ads_requests(
        binding=binding,
        surface=connector.surface(CUSTOMER_LISTS_SURFACE),
        reconciled=reconciled,
    ).plans[:1]

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(CUSTOMER_LISTS_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=selected_plan[0].row_count,
            dry_run=False,
            resolved_auth=SimpleNamespace(headers={}),
            reconciled=reconciled,
            selected_request_plans=selected_plan,
        ),
    )

    assert evidence.status == "pre_acceptance_failure"
    assert evidence.pre_acceptance_failure_category == "transport"
