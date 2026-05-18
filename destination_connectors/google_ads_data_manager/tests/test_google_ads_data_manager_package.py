from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest
from retl_google_ads_data_manager.common import (
    DATA_MANAGER_API_VERSION,
    google_ads_data_manager_config,
    service_account_auth,
)
from retl_google_ads_data_manager.definitions import (
    CUSTOMER_MATCH_CONTACT_ID_SURFACE,
    CUSTOMER_MATCH_SURFACE,
    EVENTS_SURFACE,
    google_ads_data_manager_connector,
)
from retl_google_ads_data_manager.hooks import plan_google_ads_data_manager_requests

import retl
from retl.auth import ResolvedAuth
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.request_batch import RequestBatchPlan
from retl.destinations.targets import ManagedTargetClient, RemoteTarget, TargetMapping, registry_key
from retl.errors import DeclarationValidationError
from retl.events.reconcile import EventReconcileEvidence
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
            json_body={"requestId": f"request-{len(self.requests)}"},
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
class SequenceTransport:
    responses: list[HttpResponse]
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError("SequenceTransport received more requests than responses")
        return self.responses.pop(0)


@dataclass
class QueueTransport:
    responses: list[HttpResponse]
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError(
                f"Unexpected Google Ads Data Manager request: {request.method} {request.url}"
            )
        return self.responses.pop(0)


def _customer_match_page() -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers_json": _json(
                    [
                        {"type": "email", "value": "First.Last@Gmail.com"},
                        {"type": "phone_e164", "value": "+18005550100"},
                    ]
                ),
                "payload_json": "{}",
            },
            {
                "operation": "upsert",
                "record_identity": "customer-2",
                "target": "vip",
                "state_key": {"customer_id": "2"},
                "identifiers_json": _json(
                    [
                        {
                            "type": "address",
                            "value": {
                                "given_name": "Ada",
                                "family_name": "Lovelace",
                                "region_code": "US",
                                "postal_code": "10001",
                            },
                        }
                    ]
                ),
                "payload_json": _json({"ad_user_data_consent": "CONSENT_DENIED"}),
            },
            {
                "operation": "remove",
                "record_identity": "customer-3",
                "target": "vip",
                "state_key": {"customer_id": "3"},
                "identifiers_json": _json(
                    [{"type": "mobile_advertising_id", "value": "madid-123"}]
                ),
                "payload_json": "{}",
            },
            {
                "operation": "remove",
                "record_identity": "customer-4",
                "target": "vip",
                "state_key": {"customer_id": "4"},
                "identifiers_json": _json([{"type": "external_id", "value": "user-123"}]),
                "payload_json": "{}",
            },
        ]
    ).to_batches()[0]


def _canonical_contact_id_page(*, target: str = "vip") -> pa.RecordBatch:
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


def _event_page() -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "import",
                "event_identity": "purchase-1",
                "event_key": {"event_id": "purchase-1"},
                "event_name": "Purchase",
                "occurred_at": "2026-04-30T12:00:00Z",
                "identifiers": [
                    {"type": "email", "value": "Buyer@Example.Test"},
                    {"type": "phone_e164", "value": "+18005550100"},
                    {"type": "external_id", "value": "customer-123"},
                ],
                "payload": {
                    "currency": "USD",
                    "conversion_value": 123.45,
                    "event_source": "WEB",
                    "gclid": "gclid-123",
                    "user_agent": "Mozilla/5.0 RETL fixture",
                    "ip_address": "203.0.113.10",
                },
            },
            {
                "operation": "import",
                "event_identity": "lead-1",
                "event_key": {"event_id": "lead-1"},
                "event_name": "Lead",
                "occurred_at": "2026-04-30T12:01:00Z",
                "identifiers": [{"type": "email", "value": "Lead@Example.Test"}],
                "payload": {"currency": "USD", "conversion_value": 1},
            },
        ]
    ).to_batches()[0]


def _binding(
    *,
    transport: object | None = None,
    customer_match_terms_accepted: bool = True,
) -> DestinationBinding:
    connector = google_ads_data_manager_connector()
    config: dict[str, JSONValue] = {
        "operating_account_id": "customers/123",
        "login_account_id": "manager-456",
        "customer_match_terms_accepted": customer_match_terms_accepted,
        "ad_user_data_consent": "CONSENT_GRANTED",
        "ad_personalization_consent": "CONSENT_GRANTED",
    }
    if transport is not None:
        config["transport"] = transport  # type: ignore[assignment]
    return DestinationBinding(
        binding_name="google_ads_customer_match",
        destination_ref=connector.connector_ref,
        connector=connector,
        config=config,
        credentials={"access_token": retl.secrets.literal("token")},
        target_mappings=(
            TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="aud_123")),
        ),
    )


def _contact_id_declaration() -> retl.State:
    return retl.state(
        name="google_ads_contact_customers",
        source=retl.source(
            name="google_ads_contacts",
            mode="snapshot",
            query="select customer_id, email, audience_key from customers",
        ),
        key={"customer_id": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
    )


def _managed_contact_id_sync(*, transport: QueueTransport) -> retl.Sync:
    connector = google_ads_data_manager_connector()
    return retl.sync(
        name="google_ads_contact_customer_match_sync",
        declaration=_contact_id_declaration(),
        destination=DestinationBinding(
            binding_name="google_ads_customer_match",
            destination_ref=connector.connector_ref,
            connector=connector,
            credentials={"access_token": retl.secrets.literal("token")},
            config={
                "operating_account_id": "customers/123",
                "login_account_id": "manager-456",
                "customer_match_terms_accepted": True,
                "transport": transport,  # type: ignore[dict-item]
            },
        ),
        surface=CUSTOMER_MATCH_CONTACT_ID_SURFACE,
    )


def test_google_ads_data_manager_connector_declares_customer_match_surface_and_auth() -> None:
    connector = google_ads_data_manager_connector()
    surface = connector.surface(CUSTOMER_MATCH_SURFACE)
    contact_surface = connector.surface(CUSTOMER_MATCH_CONTACT_ID_SURFACE)
    events_surface = connector.surface(EVENTS_SURFACE)

    assert connector.connector_ref == "retl/google-ads-data-manager"
    assert [mode.name for mode in connector.auth_modes] == ["access_token", "service_account"]
    service_account_mode = connector.auth_modes[1]
    assert service_account_mode.required_fields == ("project_id", "client_email", "private_key")
    assert service_account_mode.optional_fields == ("private_key_id", "token_uri")
    assert surface.declaration_family == "state"
    assert surface.supported_operations == ("upsert", "remove")
    assert surface.target_mode == "required"
    assert surface.execution_mode == "asynchronous"
    assert surface.delivery_outcome == "accepted"
    assert surface.supports_managed_targets is False
    assert "email" in surface.accepted_identifier_types
    assert "external_id" in surface.accepted_identifier_types
    assert contact_surface.declaration_family == "state"
    assert contact_surface.supported_operations == ("upsert", "remove")
    assert contact_surface.target_mode == "required"
    assert contact_surface.supports_managed_targets is True
    assert contact_surface.accepted_identifier_types == ("email", "phone_e164", "address")
    assert contact_surface.delivery_outcome == "accepted"
    assert contact_surface.execution_mode == "asynchronous"
    assert events_surface.declaration_family == "event"
    assert events_surface.supported_operations == ("import",)
    assert events_surface.target_mode == "unsupported"
    assert events_surface.delivery_outcome == "accepted"
    assert events_surface.execution_mode == "asynchronous"
    assert events_surface.required_payload_fields == ("event_name",)
    assert callable(connector.submission_hook)
    assert callable(connector.managed_target_client_hook)


def test_google_ads_data_manager_service_account_uses_namespaces_without_auth_in_plan() -> None:
    retl.configure(
        config_resolver=retl.MappingConfigResolver(
            {
                "destinations.google_ads.operating_account_id": "customers/123",
                "destinations.google_ads.login_account_id": "manager-456",
                "destinations.google_ads.customer_match_terms_accepted": "true",
                "destinations.google_ads.request_status_poll_interval_seconds": "0.5",
                "destinations.google_ads.request_status_poll_timeout_seconds": "2",
            }
        )
    )
    try:
        connector = google_ads_data_manager_connector()
        registry = retl.destinations.DestinationRegistry()
        registry.register(connector)
        binding = retl.destinations.load(
            "retl/google-ads-data-manager",
            binding_name="google_ads_customer_match",
            auth_mode="service_account",
            credential_namespace="destinations.google_ads.service_account",
            config_namespace="destinations.google_ads",
            target_mappings=(
                TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="aud_123")),
            ),
            registry=registry,
        )

        assert binding.credentials == {
            "project_id": retl.secrets["destinations.google_ads.service_account.project_id"],
            "client_email": retl.secrets["destinations.google_ads.service_account.client_email"],
            "private_key": retl.secrets["destinations.google_ads.service_account.private_key"],
        }
        config = google_ads_data_manager_config(binding)
        assert config.operating_account_id == "customers/123"
        assert config.customer_match_terms_accepted is True
        assert config.request_status_poll_interval_seconds == 0.5
        assert config.request_status_poll_timeout_seconds == 2.0
        plan = plan_google_ads_data_manager_requests(
            binding=binding,
            surface=connector.surface(CUSTOMER_MATCH_SURFACE),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    sync_name="google_ads_sync",
                    operation_pages=(_customer_match_page(),),
                ),
            ),
        )

        rendered_plan = str(plan.plans)
        assert "private_key" not in rendered_plan
        assert "Authorization" not in rendered_plan
    finally:
        retl.configure(config_resolver=None)


def test_service_account_auth_exchanges_credentials_without_leaking_secret_material(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeCredentials:
        token = "service-token"
        expiry = datetime(2026, 5, 7, 12, 0, tzinfo=timezone.utc)

        def refresh(self, request: object) -> None:
            captured["request"] = request

    def from_service_account_info(info: object, *, scopes: list[str]) -> FakeCredentials:
        captured["info"] = info
        captured["scopes"] = scopes
        return FakeCredentials()

    monkeypatch.setitem(
        sys.modules,
        "google.oauth2.service_account",
        SimpleNamespace(
            Credentials=SimpleNamespace(from_service_account_info=from_service_account_info)
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "google.auth.transport.requests",
        SimpleNamespace(Request=lambda: "request"),
    )

    resolved = service_account_auth(
        {
            "project_id": "project-123",
            "client_email": "retl@example.iam.gserviceaccount.com",
            "private_key": "line-1\\nline-2",
            "private_key_id": "key-123",
        },
        {"mode": "service_account"},
    )

    assert isinstance(resolved, ResolvedAuth)
    assert resolved.mode == "service_account"
    assert resolved.headers == {"Authorization": "Bearer service-token"}
    assert resolved.token_expires_at == 1778155200
    assert captured["request"] == "request"
    assert captured["scopes"] == ["https://www.googleapis.com/auth/datamanager"]
    info = cast(dict[str, str], captured["info"])
    assert info["private_key"] == "line-1\nline-2"
    assert "line-1" not in repr(resolved)


def test_google_ads_data_manager_config_normalizes_and_validates_binding_config() -> None:
    connector = google_ads_data_manager_connector()
    config = google_ads_data_manager_config(
        DestinationBinding(
            binding_name="google_ads_customer_match",
            destination_ref=connector.connector_ref,
            connector=connector,
            config={
                "operating_account_id": " 123 ",
                "login_account_id": " 456 ",
                "linked_account_id": " 789 ",
                "linked_account_type": "google_ads",
                "customer_match_terms_accepted": True,
                "encoding": "hex",
                "ad_user_data_consent": "consent_granted",
            },
        )
    )

    assert config.operating_account_id == "123"
    assert config.login_account_id == "456"
    assert config.linked_account_id == "789"
    assert config.linked_account_type == "GOOGLE_ADS"
    assert config.customer_match_terms_accepted is True
    assert config.encoding == "HEX"
    assert config.ad_user_data_consent == "CONSENT_GRANTED"

    invalid_configs: tuple[dict[str, JSONValue], ...] = (
        {},
        {"operating_account_id": "123", "encoding": "raw"},
        {"operating_account_id": "123", "linked_account_id": "789"},
        {"operating_account_id": "123", "ad_user_data_consent": "maybe"},
    )
    for raw_config in invalid_configs:
        with pytest.raises(DeclarationValidationError):
            google_ads_data_manager_config(
                DestinationBinding(
                    binding_name="google_ads_customer_match",
                    destination_ref=connector.connector_ref,
                    connector=connector,
                    config=raw_config,
                )
            )


def test_contact_id_managed_target_client_finds_and_creates_user_lists() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=200,
                json_body={
                    "userLists": [
                        {
                            "id": "111",
                            "displayName": "other",
                            "ingestedUserListInfo": {"uploadKeyTypes": ["CONTACT_ID"]},
                        },
                        {
                            "name": "accountTypes/GOOGLE_ADS/accounts/123/userLists/222",
                            "id": "222",
                            "displayName": "vip",
                            "integrationCode": "retl:existing",
                            "ingestedUserListInfo": {"uploadKeyTypes": ["CONTACT_ID"]},
                        },
                    ]
                },
            ),
            HttpResponse(
                status_code=200,
                json_body={
                    "id": "333",
                    "displayName": "New Customers",
                    "integrationCode": "retl:created",
                    "ingestedUserListInfo": {"uploadKeyTypes": ["CONTACT_ID"]},
                },
            ),
        ],
        requests=[],
    )
    connector = google_ads_data_manager_connector()
    hook = connector.managed_target_client_hook
    assert hook is not None
    client = cast(
        ManagedTargetClient,
        hook(
            binding=_binding(transport=transport),
            surface=connector.surface(CUSTOMER_MATCH_CONTACT_ID_SURFACE),
            resolved_auth=SimpleNamespace(headers={"Authorization": "Bearer token"}),
        ),
    )

    existing = client.find_target("vip")
    created = client.create_target("new-customers", display_name="New Customers")

    assert existing == RemoteTarget(
        remote_id="222",
        display_name="vip",
        metadata={
            "kind": "google_ads_data_manager_user_list",
            "upload_key_type": "CONTACT_ID",
            "integration_code": "retl:existing",
        },
    )
    assert created == RemoteTarget(
        remote_id="333",
        display_name="New Customers",
        metadata={
            "kind": "google_ads_data_manager_user_list",
            "upload_key_type": "CONTACT_ID",
            "integration_code": "retl:created",
        },
    )
    assert [request.method for request in transport.requests] == ["GET", "POST"]
    assert transport.requests[0].url == (
        "https://datamanager.googleapis.com/"
        f"{DATA_MANAGER_API_VERSION}/accountTypes/GOOGLE_ADS/accounts/123/userLists"
    )
    assert transport.requests[0].query == {
        "pageSize": "100",
        "filter": (
            'display_name = "vip" AND ingested_user_list_info.upload_key_types = "CONTACT_ID"'
        ),
    }
    assert transport.requests[0].headers == {
        "Authorization": "Bearer token",
        "login-account": "accountTypes/GOOGLE_ADS/accounts/manager-456",
    }
    assert transport.requests[1].json_body == {
        "displayName": "New Customers",
        "description": "Managed by RETL for `new-customers`.",
        "membershipStatus": "OPEN",
        "membershipDuration": "46656000s",
        "integrationCode": "retl:google_ads_customer_match:customer_match_contact_id:"
        "074eb00e565a37948b233f6a",
        "ingestedUserListInfo": {
            "uploadKeyTypes": ["CONTACT_ID"],
            "contactIdInfo": {"dataSourceType": "DATA_SOURCE_TYPE_FIRST_PARTY"},
        },
    }


def test_contact_id_managed_target_client_is_not_exposed_for_other_surfaces() -> None:
    connector = google_ads_data_manager_connector()
    hook = connector.managed_target_client_hook
    assert hook is not None

    assert (
        hook(
            binding=_binding(),
            surface=connector.surface(CUSTOMER_MATCH_SURFACE),
            resolved_auth=SimpleNamespace(headers={}),
        )
        is None
    )


def test_contact_id_managed_target_creation_runs_before_membership_submission() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"userLists": []}),
            HttpResponse(
                status_code=200,
                json_body={
                    "id": "user_list_123",
                    "displayName": "vip",
                    "integrationCode": "retl:created",
                    "ingestedUserListInfo": {"uploadKeyTypes": ["CONTACT_ID"]},
                },
            ),
            HttpResponse(status_code=200, json_body={"requestId": "request-123"}),
        ],
        requests=[],
    )
    sync = _managed_contact_id_sync(transport=transport)
    store = DuckDBRuntimeStore(database=":memory:")

    result = sync_destination(
        sync=sync,
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(
                phase="reconcile",
                sync_name=sync.name,
                operation_pages=(_canonical_contact_id_page(),),
                status="succeeded",
                scope=destination_progress_scope(sync),
            ),
        ),
        dry_run=False,
        runtime_store=store,
    )

    assert result.target_resolution is not None
    assert result.target_resolution.managed_created_count == 1
    assert [request.method for request in transport.requests] == ["GET", "POST", "POST"]
    create_request = transport.requests[1]
    assert create_request.json_body == {
        "displayName": "vip",
        "description": "Managed by RETL for `vip`.",
        "membershipStatus": "OPEN",
        "membershipDuration": "46656000s",
        "integrationCode": "retl:google_ads_customer_match:customer_match_contact_id:"
        "f9505a739a8fb8e851942c3f",
        "ingestedUserListInfo": {
            "uploadKeyTypes": ["CONTACT_ID"],
            "contactIdInfo": {"dataSourceType": "DATA_SOURCE_TYPE_FIRST_PARTY"},
        },
    }
    membership_body = cast(dict[str, Any], transport.requests[2].json_body)
    destination = cast(list[dict[str, Any]], membership_body["destinations"])[0]
    assert destination["productDestinationId"] == "user_list_123"
    binding = cast(DestinationBinding, sync.destination)
    record = store.get(
        registry_key(
            binding=binding,
            surface=CUSTOMER_MATCH_CONTACT_ID_SURFACE,
            logical_target="vip",
        )
    )
    assert record is not None
    assert record.remote.remote_id == "user_list_123"


def test_customer_match_plans_ingest_and_remove_batches() -> None:
    connector = google_ads_data_manager_connector()
    plan = plan_google_ads_data_manager_requests(
        binding=_binding(),
        surface=connector.surface(CUSTOMER_MATCH_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="google_ads_sync", operation_pages=(_customer_match_page(),)),
        ),
    )

    assert plan.request_count == 2
    assert [request.operation for request in plan.plans] == ["upsert", "remove"]
    assert [request.request_item_count for request in plan.plans] == [2, 2]
    assert [request.request_item_counts for request in plan.plans] == [(1, 1), (1, 1)]
    assert [request.request.path for request in plan.plans] == [
        f"/{DATA_MANAGER_API_VERSION}/audienceMembers:ingest",
        f"/{DATA_MANAGER_API_VERSION}/audienceMembers:remove",
    ]
    ingest_body = cast(dict[str, Any], plan.plans[0].request.json_body)
    remove_body = cast(dict[str, Any], plan.plans[1].request.json_body)
    assert ingest_body["encoding"] == "HEX"
    assert ingest_body["termsOfService"] == {"customerMatchTermsOfServiceStatus": "ACCEPTED"}
    assert ingest_body["consent"] == {
        "adUserData": "CONSENT_GRANTED",
        "adPersonalization": "CONSENT_GRANTED",
    }
    destination = cast(list[dict[str, Any]], ingest_body["destinations"])[0]
    assert destination == {
        "operatingAccount": {"accountId": "customers/123", "accountType": "GOOGLE_ADS"},
        "loginAccount": {"accountId": "manager-456", "accountType": "GOOGLE_ADS"},
        "productDestinationId": "aud_123",
    }
    members = cast(list[dict[str, Any]], ingest_body["audienceMembers"])
    assert "First.Last@Gmail.com" not in str(ingest_body)
    assert "emailAddress" in members[0]["userData"]["userIdentifiers"][0]
    assert "phoneNumber" in members[0]["userData"]["userIdentifiers"][1]
    assert members[1]["consent"] == {"adUserData": "CONSENT_DENIED"}
    address = members[1]["userData"]["userIdentifiers"][0]["address"]
    assert address["givenName"] == (
        "fdee430d40bd57deeac186cd9790033d0f06f909a8806e7ce6e717ab7c7d5029"
    )
    assert address["regionCode"] == "US"
    assert "termsOfService" not in remove_body
    remove_members = cast(list[dict[str, Any]], remove_body["audienceMembers"])
    assert remove_members == [
        {"mobileData": {"mobileIds": ["madid-123"]}},
        {"userIdData": {"userId": "user-123"}},
    ]


def test_customer_match_preserves_prehashed_email_and_phone_identifiers() -> None:
    email_hash = "A" * 64
    phone_hash = "b" * 64
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers_json": _json(
                    [
                        {"type": "email", "value": f" {email_hash} "},
                        {"type": "phone_e164", "value": phone_hash},
                    ]
                ),
                "payload_json": "{}",
            }
        ]
    ).to_batches()[0]
    connector = google_ads_data_manager_connector()

    plan = plan_google_ads_data_manager_requests(
        binding=_binding(),
        surface=connector.surface(CUSTOMER_MATCH_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="google_ads_sync", operation_pages=(page,)),
        ),
    )

    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    members = cast(list[dict[str, Any]], body["audienceMembers"])
    user_identifiers = members[0]["userData"]["userIdentifiers"]
    assert user_identifiers == [
        {"emailAddress": email_hash.lower()},
        {"phoneNumber": phone_hash},
    ]


def test_events_plan_ingest_batches_with_event_payload_shape() -> None:
    connector = google_ads_data_manager_connector()
    binding = DestinationBinding(
        binding_name="google_events",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "operating_account_id": "customers/123",
            "event_destination_id": "conversion_action_123",
        },
    )

    plan = plan_google_ads_data_manager_requests(
        binding=binding,
        surface=connector.surface(EVENTS_SURFACE),
        reconciled=cast(
            EventReconcileEvidence,
            SimpleNamespace(sync_name="google_events", import_pages=(_event_page(),)),
        ),
    )

    assert plan.request_count == 1
    assert plan.plans[0].request.path == f"/{DATA_MANAGER_API_VERSION}/events:ingest"
    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    assert body["encoding"] == "HEX"
    assert body["destinations"] == [
        {
            "operatingAccount": {"accountId": "customers/123", "accountType": "GOOGLE_ADS"},
            "productDestinationId": "conversion_action_123",
        }
    ]
    event = cast(list[dict[str, Any]], body["events"])[0]
    assert event["transactionId"] == "purchase-1"
    assert event["eventTimestamp"] == "2026-04-30T12:00:00Z"
    assert event["eventName"] == "Purchase"
    assert event["currency"] == "USD"
    assert event["conversionValue"] == 123.45
    assert event["eventSource"] == "WEB"
    assert event["adIdentifiers"] == {"gclid": "gclid-123"}
    assert event["eventDeviceInfo"] == {
        "ipAddress": "203.0.113.10",
        "userAgent": "Mozilla/5.0 RETL fixture",
    }
    user_data = cast(dict[str, Any], event["userData"])
    assert user_data["userIdentifiers"] == [
        {"emailAddress": "f979a1713bf697bbbf1fc65fa352d2bb2376ee4ddf41d3bb07f9fb8f6126d1db"},
        {"phoneNumber": "fb4f73a6ec5fdb7077d564cdd22c3554b43ce49168550c3b12c547b78c517b30"},
    ]
    assert event["userId"] == "customer-123"


def test_events_non_dry_run_polls_status_404_until_processing_without_resubmitting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = google_ads_data_manager_connector()
    transport = SequenceTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"requestId": "request-123"}),
            HttpResponse(status_code=404, json_body={"error": {"status": "NOT_FOUND"}}),
            HttpResponse(
                status_code=200,
                json_body={
                    "requestStatusPerDestination": [
                        {"requestStatus": "PROCESSING", "eventsIngestionStatus": {}}
                    ]
                },
            ),
        ],
        requests=[],
    )
    sleeps: list[float] = []
    monkeypatch.setattr(
        "retl_google_ads_data_manager.hooks.sleep",
        lambda seconds: sleeps.append(seconds),
    )
    hook = connector.submission_hook
    assert hook is not None

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=DestinationBinding(
                binding_name="google_events",
                destination_ref=connector.connector_ref,
                connector=connector,
                config={
                    "operating_account_id": "customers/123",
                    "event_destination_id": "conversion_action_123",
                    "request_status_poll_interval_seconds": 1,
                    "request_status_poll_timeout_seconds": 16,
                    "transport": transport,  # type: ignore[dict-item]
                },
                credentials={"access_token": retl.secrets.literal("token")},
            ),
            surface=connector.surface(EVENTS_SURFACE),
            delivery_outcome="accepted",
            attempted_count=2,
            dry_run=False,
            resolved_auth=SimpleNamespace(
                mode="access_token", headers={"Authorization": "Bearer token"}
            ),
            reconciled=cast(
                EventReconcileEvidence,
                SimpleNamespace(sync_name="google_events", import_pages=(_event_page(),)),
            ),
        ),
    )

    assert evidence.status == "accepted"
    assert evidence.accepted_count == 2
    assert evidence.remote_handles[0].value == "request-123"
    assert sleeps == [1, 1]
    assert [request.method for request in transport.requests] == ["POST", "GET", "GET"]
    assert transport.requests[0].url.endswith(f"/{DATA_MANAGER_API_VERSION}/events:ingest")
    assert transport.requests[1].query == {"requestId": "request-123"}


def test_events_status_404_through_visibility_window_stays_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = google_ads_data_manager_connector()
    transport = SequenceTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"requestId": "request-404"}),
            HttpResponse(status_code=404, json_body={"error": {"status": "NOT_FOUND"}}),
            HttpResponse(status_code=404, json_body={"error": {"status": "NOT_FOUND"}}),
            HttpResponse(status_code=404, json_body={"error": {"status": "NOT_FOUND"}}),
            HttpResponse(status_code=404, json_body={"error": {"status": "NOT_FOUND"}}),
            HttpResponse(status_code=404, json_body={"error": {"status": "NOT_FOUND"}}),
        ],
        requests=[],
    )
    sleeps: list[float] = []
    monkeypatch.setattr(
        "retl_google_ads_data_manager.hooks.sleep",
        lambda seconds: sleeps.append(seconds),
    )
    hook = connector.submission_hook
    assert hook is not None

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=DestinationBinding(
                binding_name="google_events",
                destination_ref=connector.connector_ref,
                connector=connector,
                config={
                    "operating_account_id": "customers/123",
                    "event_destination_id": "conversion_action_123",
                    "request_status_poll_interval_seconds": 1,
                    "request_status_poll_timeout_seconds": 16,
                    "transport": transport,  # type: ignore[dict-item]
                },
            ),
            surface=connector.surface(EVENTS_SURFACE),
            delivery_outcome="accepted",
            attempted_count=2,
            dry_run=False,
            resolved_auth=SimpleNamespace(mode="access_token", headers={}),
            reconciled=cast(
                EventReconcileEvidence,
                SimpleNamespace(sync_name="google_events", import_pages=(_event_page(),)),
            ),
        ),
    )

    assert evidence.status == "accepted"
    assert evidence.accepted_count == 2
    assert evidence.request_batch_count == 1
    assert evidence.remote_handles[0].value == "request-404"
    assert "not visible within 16 seconds" in evidence.summary
    assert evidence.partner_error_detail is not None
    assert "404 NOT_FOUND" in evidence.partner_error_detail
    assert sleeps == [1, 1, 2, 4, 8]
    assert [request.method for request in transport.requests].count("POST") == 1


def test_customer_match_requires_terms_acceptance_for_user_data_uploads() -> None:
    connector = google_ads_data_manager_connector()

    with pytest.raises(DeclarationValidationError, match="customer_match_terms_accepted"):
        plan_google_ads_data_manager_requests(
            binding=_binding(customer_match_terms_accepted=False),
            surface=connector.surface(CUSTOMER_MATCH_SURFACE),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    sync_name="google_ads_sync",
                    operation_pages=(_customer_match_page(),),
                ),
            ),
        )


def test_customer_match_rejects_mixed_identifier_families_in_one_record() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers_json": _json(
                    [
                        {"type": "email", "value": "one@example.test"},
                        {"type": "mobile_advertising_id", "value": "madid-123"},
                    ]
                ),
                "payload_json": "{}",
            }
        ]
    ).to_batches()[0]
    connector = google_ads_data_manager_connector()

    with pytest.raises(ValueError, match="exactly one identifier family"):
        plan_google_ads_data_manager_requests(
            binding=_binding(),
            surface=connector.surface(CUSTOMER_MATCH_SURFACE),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(sync_name="google_ads_sync", operation_pages=(page,)),
            ),
        )


def test_customer_match_non_dry_run_posts_to_data_manager_with_bearer_auth() -> None:
    connector = google_ads_data_manager_connector()
    transport = RecordingTransport(requests=[])
    hook = connector.submission_hook
    assert hook is not None
    binding = _binding(transport=transport)
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="google_ads_sync", operation_pages=(_customer_match_page(),)),
    )
    selected_plan = plan_google_ads_data_manager_requests(
        binding=binding,
        surface=connector.surface(CUSTOMER_MATCH_SURFACE),
        reconciled=reconciled,
    ).plans[:1]
    assert len(selected_plan) == 1

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(CUSTOMER_MATCH_SURFACE),
            delivery_outcome="accepted",
            attempted_count=selected_plan[0].row_count,
            dry_run=False,
            resolved_auth=SimpleNamespace(
                mode="access_token", headers={"Authorization": "Bearer token"}
            ),
            reconciled=reconciled,
            selected_request_plans=selected_plan,
        ),
    )

    assert evidence.status == "accepted"
    assert evidence.accepted_count == selected_plan[0].row_count
    assert evidence.request_batch_count == 1
    assert [request.url for request in transport.requests] == [
        f"https://datamanager.googleapis.com/{DATA_MANAGER_API_VERSION}/audienceMembers:ingest",
    ]
    assert all(request.headers["Authorization"] == "Bearer token" for request in transport.requests)


def test_customer_match_selected_request_plans_drive_submission_without_replanning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import retl_google_ads_data_manager.hooks as hooks

    connector = google_ads_data_manager_connector()
    transport = FailingTransport(requests=[])
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="google_ads_sync", operation_pages=(_customer_match_page(),)),
    )
    selected_plan: tuple[RequestBatchPlan, ...] = plan_google_ads_data_manager_requests(
        binding=_binding(),
        surface=connector.surface(CUSTOMER_MATCH_SURFACE),
        reconciled=reconciled,
    ).plans[:1]

    def fail_replanning(**_: object) -> object:
        raise AssertionError("selected request plans must not be planned again")

    monkeypatch.setattr(hooks, "plan_google_ads_data_manager_requests", fail_replanning)
    hook = connector.submission_hook
    assert hook is not None

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=_binding(transport=transport),
            surface=connector.surface(CUSTOMER_MATCH_SURFACE),
            delivery_outcome="accepted",
            attempted_count=selected_plan[0].row_count,
            dry_run=False,
            resolved_auth=SimpleNamespace(mode="access_token", headers={}),
            reconciled=reconciled,
            selected_request_plans=selected_plan,
        ),
    )

    assert evidence.status == "pre_acceptance_failure"
    assert evidence.pre_acceptance_failure_category == "transport"
    assert evidence.request_batch_count == 1
    assert len(transport.requests) == 1


def test_customer_match_classifies_google_error_detail_without_leaking_auth() -> None:
    connector = google_ads_data_manager_connector()
    transport = StaticTransport(
        response=HttpResponse(
            status_code=400,
            json_body={
                "error": {
                    "code": 400,
                    "status": "INVALID_ARGUMENT",
                    "message": "authorization: Bearer secret-token bad user data",
                }
            },
        ),
        requests=[],
    )
    hook = connector.submission_hook
    assert hook is not None
    binding = _binding(transport=transport)
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="google_ads_sync", operation_pages=(_customer_match_page(),)),
    )
    selected_plan = plan_google_ads_data_manager_requests(
        binding=binding,
        surface=connector.surface(CUSTOMER_MATCH_SURFACE),
        reconciled=reconciled,
    ).plans[:1]
    assert len(selected_plan) == 1

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(CUSTOMER_MATCH_SURFACE),
            delivery_outcome="accepted",
            attempted_count=4,
            dry_run=False,
            resolved_auth=SimpleNamespace(
                mode="access_token", headers={"Authorization": "Bearer token"}
            ),
            reconciled=reconciled,
            selected_request_plans=selected_plan,
        ),
    )

    assert evidence.status == "pre_acceptance_failure"
    assert evidence.pre_acceptance_failure_category == "schema"
    assert evidence.http_status == 400
    assert evidence.partner_error_detail is not None
    assert "secret-token" not in evidence.partner_error_detail


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True)
