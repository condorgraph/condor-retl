from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest
import retl_meta.hooks as meta_hooks
from retl_meta.common import meta_config
from retl_meta.definitions import CUSTOM_AUDIENCES_SURFACE, EVENTS_SURFACE, meta_connector
from retl_meta.hooks import plan_meta_requests

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.compatibility import (
    DestinationCompatibilityError,
    validate_surface_compatibility,
)
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.request_batch import DryRunSubmissionPlan, RequestBatchPlan
from retl.destinations.surfaces import DestinationSurface
from retl.destinations.targets import (
    RemoteTarget,
    TargetMapping,
    TargetRegistryRecord,
    registry_key,
)
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
        return HttpResponse(status_code=200, json_body={"request_id": f"meta-{len(self.requests)}"})


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
            raise AssertionError(f"Unexpected Meta request: {request.method} {request.url}")
        return self.responses.pop(0)


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
                "operation": "upsert",
                "record_identity": "customer-2",
                "target": "vip",
                "state_key": {"customer_id": "2"},
                "identifiers": [{"type": "email", "value": "two@example.test"}],
                "payload": {},
            },
            {
                "operation": "remove",
                "record_identity": "customer-3",
                "target": "vip",
                "state_key": {"customer_id": "3"},
                "identifiers": [{"type": "email", "value": "three@example.test"}],
                "payload": {},
            },
        ]
    ).to_batches()[0]


def _meta_audience_binding() -> DestinationBinding:
    connector = meta_connector()
    return DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={"ad_account_id": "123", "api_version": "v25.0"},
        target_mappings=(
            TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="aud_123")),
        ),
    )


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
                    {"type": "external_id", "value": "customer-123"},
                ],
                "payload": {
                    "value": 123.45,
                    "currency": "USD",
                    "order_id": "order-123",
                    "event_source_url": "https://example.test/checkout",
                    "fbc": "fb.1.1777334400.click-id",
                    "fbp": "fb.1.1777334400.browser-id",
                    "client_ip_address": "203.0.113.10",
                    "client_user_agent": "Mozilla/5.0 RETL fixture",
                },
            },
            {
                "operation": "import",
                "event_identity": "lead-1",
                "event_key": {"event_id": "lead-1"},
                "event_name": "Lead",
                "occurred_at": "2026-04-30T12:01:00Z",
                "identifiers": [{"type": "email", "value": "Lead@Example.Test"}],
                "payload": {"value": 1, "currency": "USD"},
            },
        ]
    ).to_batches()[0]


def _coordinated_event_page() -> SimpleNamespace:
    payload = pa.Table.from_pylist(
        [
            {
                "operation": "import",
                "event_identity": "purchase-1",
                "event_key": {"event_id": "purchase-1"},
                "event_name": "Purchase",
                "occurred_at": "2026-04-30T12:00:00Z",
                "collect_id": "00000000-0007-7000-8000-000000000000",
                "sequence_order": 0,
                "event_cursor_value": "2026-04-30T12:00:00Z",
                "event_primary_key_value": "purchase-1",
                "identifiers": [{"type": "email", "value": "Buyer@Example.Test"}],
                "payload": {
                    "event_name": "Purchase",
                    "value": 123.45,
                    "currency": "USD",
                },
            },
            {
                "operation": "import",
                "event_identity": "purchase-2",
                "event_key": {"event_id": "purchase-2"},
                "event_name": "Purchase",
                "occurred_at": "2026-04-30T12:01:00Z",
                "collect_id": "00000000-0007-7000-8000-000000000000",
                "sequence_order": 1,
                "event_cursor_value": "2026-04-30T12:01:00Z",
                "event_primary_key_value": "purchase-2",
                "identifiers": [{"type": "email", "value": "Second@Example.Test"}],
                "payload": {
                    "event_name": "Purchase",
                    "value": 456.78,
                    "currency": "USD",
                },
            },
        ]
    ).to_batches()[0]
    return SimpleNamespace(
        payload=payload,
        event_cursor_kind="string",
        event_primary_key_kind="string",
    )


def _event_declaration() -> retl.Event:
    return retl.event(
        name="meta_purchase",
        source=retl.source(
            name="meta_conversions",
            mode="checkpointed",
            query="select event_id, email, event_name, occurred_at, value, currency from events",
            checkpoint={
                "cursor": "occurred_at",
                "primary_key": "event_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
        ),
        key={"event_id": "event_id"},
        occurred_at="occurred_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"event_name": "event_name", "value": "value", "currency": "currency"},
    )


def _event_sync(
    *,
    transport: StaticTransport,
    on_failure: retl.FailureHandlingMode = "continue_on_any",
) -> retl.Sync:
    connector = meta_connector()
    return retl.sync(
        name="meta_purchase_imports",
        declaration=_event_declaration(),
        destination=DestinationBinding(
            binding_name="meta_events",
            destination_ref=connector.connector_ref,
            connector=connector,
            credentials={"access_token": retl.secrets.literal("token")},
            config={
                "ad_account_id": "123",
                "api_version": "v25.0",
                "action_source": "website",
                "pixel_id": "pixel_123",
                "transport": transport,  # type: ignore[dict-item]
            },
        ),
        surface=EVENTS_SURFACE,
        on_failure=on_failure,
    )


def _audience_declaration() -> retl.State:
    return retl.state(
        name="meta_customer_audience",
        source=retl.source(
            name="meta_customers",
            mode="snapshot",
            query="select customer_id, email, audience_key from customers",
        ),
        key={"customer_id": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
    )


def _managed_audience_sync(*, transport: QueueTransport) -> retl.Sync:
    connector = meta_connector()
    return retl.sync(
        name="meta_customer_audience_sync",
        declaration=_audience_declaration(),
        destination=DestinationBinding(
            binding_name="meta_primary",
            destination_ref=connector.connector_ref,
            connector=connector,
            credentials={"access_token": retl.secrets.literal("token")},
            config={
                "ad_account_id": "123",
                "api_version": "v25.0",
                "transport": transport,  # type: ignore[dict-item]
            },
        ),
        surface=CUSTOM_AUDIENCES_SURFACE,
    )


def test_meta_connector_declares_custom_audiences_surface_and_auth() -> None:
    connector = meta_connector()

    assert connector.connector_ref == "retl/meta"
    assert connector.auth_modes[0].name == "access_token"
    assert connector.surface(CUSTOM_AUDIENCES_SURFACE).target_mode == "required"
    assert connector.surface(CUSTOM_AUDIENCES_SURFACE).supports_managed_targets is True
    assert connector.surface(EVENTS_SURFACE).target_mode == "unsupported"
    assert connector.surface(EVENTS_SURFACE).supports_managed_targets is False
    assert callable(connector.submission_hook)
    assert callable(connector.managed_target_client_hook)
    assert meta_hooks.META_RESPONSE_POLICY.error_code_prefix == "meta"


def test_meta_custom_audiences_accepts_minimal_readme_static_target_state() -> None:
    connector = meta_connector()
    source = retl.source(
        name="newsletter_customers",
        mode="snapshot",
        query="select lower(trim(email)) as email from customers",
    )
    audience = retl.state(
        name="newsletter_audience",
        source=source,
        key={"customer_id": "email"},
        target=retl.target("newsletter_customers"),
        identifiers=[{"type": "email", "value": "email"}],
    )
    sync = retl.sync(
        name="newsletter_audience_to_meta",
        declaration=audience,
        destination=object(),
        surface=CUSTOM_AUDIENCES_SURFACE,
    )

    compatibility = validate_surface_compatibility(
        sync=sync,
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
    )

    assert compatibility.valid is True


def test_meta_custom_audiences_accepts_docs_static_target_state() -> None:
    connector = meta_connector()
    source = retl.source(
        name="newsletter_customers",
        mode="snapshot",
        query="select customer_id, email from mart.newsletter_customers",
    )
    audience = retl.state(
        name="newsletter_audience",
        source=source,
        key={"customer_id": "customer_id"},
        target=retl.target("newsletter_customers"),
        identifiers=[{"type": "email", "value": "email"}],
    )
    sync = retl.sync(
        name="newsletter_audience_to_meta",
        declaration=audience,
        destination=object(),
        surface=CUSTOM_AUDIENCES_SURFACE,
    )

    compatibility = validate_surface_compatibility(
        sync=sync,
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
    )

    assert compatibility.valid is True


def test_meta_loads_credential_and_config_namespaces_without_auth_in_request_plan() -> None:
    retl.configure(
        config_resolver=retl.MappingConfigResolver(
            {
                "destinations.meta.ad_account_id": "123",
                "destinations.meta.api_version": "v25.0",
            }
        )
    )
    try:
        connector = meta_connector()
        registry = retl.destinations.DestinationRegistry()
        registry.register(connector)
        binding = retl.destinations.load(
            "retl/meta",
            binding_name="meta_primary",
            credential_namespace="destinations.meta",
            config_namespace="destinations.meta",
            target_mappings=(
                TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="aud_123")),
            ),
            registry=registry,
        )

        assert binding.credentials == {
            "access_token": retl.secrets["destinations.meta.access_token"]
        }
        assert meta_config(binding).normalized_ad_account_id == "act_123"
        plan = plan_meta_requests(
            binding=binding,
            surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(sync_name="meta_sync", operation_pages=(_audience_page(),)),
            ),
        )

        rendered_plan = str(plan.plans)
        assert "access_token" not in rendered_plan
        assert "Authorization" not in rendered_plan
    finally:
        retl.configure(config_resolver=None)


def test_meta_config_normalizes_and_validates_binding_config() -> None:
    connector = meta_connector()
    config = meta_config(
        DestinationBinding(
            binding_name="meta_primary",
            destination_ref=connector.connector_ref,
            connector=connector,
            config={
                "ad_account_id": " 123 ",
                "api_version": "/v25.0/",
            },
        )
    )

    assert config.ad_account_id == "123"
    assert config.normalized_ad_account_id == "act_123"
    assert config.api_version == "v25.0"

    invalid_configs: tuple[dict[str, JSONValue], ...] = (
        {},
        {"ad_account_id": "123", "api_version": "/"},
    )
    for raw_config in invalid_configs:
        with pytest.raises(DeclarationValidationError):
            meta_config(
                DestinationBinding(
                    binding_name="meta_primary",
                    destination_ref=connector.connector_ref,
                    connector=connector,
                    config=raw_config,
                )
            )


def test_meta_custom_audiences_plans_add_and_remove_payload_batches() -> None:
    connector = meta_connector()
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={"ad_account_id": "123", "api_version": "v25.0"},
        target_mappings=(
            TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="aud_123")),
        ),
    )

    plan = plan_meta_requests(
        binding=binding,
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="meta_sync", operation_pages=(_audience_page(),)),
        ),
    )

    assert plan.request_count == 2
    assert [request.operation for request in plan.plans] == ["upsert", "remove"]
    assert [request.request.method for request in plan.plans] == ["POST", "DELETE"]
    assert [request.request.path for request in plan.plans] == [
        "/v25.0/aud_123/users",
        "/v25.0/aud_123/users",
    ]
    add_body = cast(dict[str, Any], plan.plans[0].request.json_body)
    remove_body = cast(dict[str, Any], plan.plans[1].request.json_body)
    assert add_body["payload"]["schema"] == ["EMAIL"]
    assert "One@Example.Test" not in str(add_body)
    assert "operation" not in add_body
    assert "operation" not in remove_body
    assert remove_body["payload"]["schema"] == ["EMAIL"]


def test_meta_custom_audiences_preserves_prehashed_email_and_phone_identifiers() -> None:
    email_hash = "C" * 64
    phone_hash = "d" * 64
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers": [
                    {"type": "email", "value": f" {email_hash} "},
                    {"type": "phone_e164", "value": phone_hash},
                ],
                "payload": {},
            }
        ]
    ).to_batches()[0]
    connector = meta_connector()
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={"ad_account_id": "123", "api_version": "v25.0"},
        target_mappings=(
            TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="aud_123")),
        ),
    )

    plan = plan_meta_requests(
        binding=binding,
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="meta_sync", operation_pages=(page,)),
        ),
    )

    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    assert body["payload"]["schema"] == ["EMAIL", "PHONE"]
    assert body["payload"]["data"] == [[email_hash.lower(), phone_hash]]
    assert plan.plans[0].request_item_count == 1
    assert plan.plans[0].request_item_counts == (1,)


def test_meta_custom_audiences_repeated_email_identifiers_render_multiple_rows() -> None:
    first_hash = "a" * 64
    second_hash = "b" * 64
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers": [
                    {"type": "email", "value": first_hash},
                    {"type": "email", "value": second_hash},
                ],
                "payload": {},
            }
        ]
    ).to_batches()[0]
    connector = meta_connector()

    plan = plan_meta_requests(
        binding=_meta_audience_binding(),
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="meta_sync", operation_pages=(page,)),
        ),
    )

    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    assert body["payload"]["schema"] == ["EMAIL"]
    assert body["payload"]["data"] == [[first_hash], [second_hash]]
    assert plan.plans[0].request_item_count == 2
    assert plan.plans[0].request_item_counts == (2,)


def test_meta_custom_audiences_mixed_repeated_identifiers_use_per_value_fanout() -> None:
    first_email = "a" * 64
    second_email = "b" * 64
    first_phone = "c" * 64
    second_phone = "d" * 64
    third_phone = "e" * 64
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers": [
                    {"type": "email", "value": first_email},
                    {"type": "email", "value": second_email},
                    {"type": "phone_e164", "value": first_phone},
                    {"type": "phone_e164", "value": second_phone},
                    {"type": "phone_e164", "value": third_phone},
                ],
                "payload": {},
            }
        ]
    ).to_batches()[0]
    connector = meta_connector()

    plan = plan_meta_requests(
        binding=_meta_audience_binding(),
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="meta_sync", operation_pages=(page,)),
        ),
    )

    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    assert body["payload"]["schema"] == ["EMAIL", "PHONE"]
    assert body["payload"]["data"] == [
        [first_email, None],
        [second_email, None],
        [None, first_phone],
        [None, second_phone],
        [None, third_phone],
    ]
    assert plan.plans[0].request_item_count == 5
    assert plan.plans[0].request_item_counts == (5,)
    assert len(body["payload"]["data"]) == plan.plans[0].request_item_count


def test_meta_custom_audiences_request_item_counts_align_with_rendered_rows() -> None:
    first_hash = "a" * 64
    second_hash = "b" * 64
    third_hash = "c" * 64
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers": [
                    {"type": "email", "value": first_hash},
                    {"type": "email", "value": second_hash},
                ],
                "payload": {},
            },
            {
                "operation": "upsert",
                "record_identity": "customer-2",
                "target": "vip",
                "state_key": {"customer_id": "2"},
                "identifiers": [{"type": "email", "value": third_hash}],
                "payload": {},
            },
        ]
    ).to_batches()[0]
    connector = meta_connector()

    plan = plan_meta_requests(
        binding=_meta_audience_binding(),
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=cast(
            StateReconcileEvidence,
            SimpleNamespace(sync_name="meta_sync", operation_pages=(page,)),
        ),
    )

    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    assert plan.plans[0].request_item_counts == (2, 1)
    assert plan.plans[0].request_item_count == 3
    assert body["payload"]["data"] == [[first_hash], [second_hash], [third_hash]]
    assert len(body["payload"]["data"]) == plan.plans[0].request_item_count


def test_meta_custom_audiences_rejects_single_record_over_payload_data_limit() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "target": "vip",
                "state_key": {"customer_id": "1"},
                "identifiers": [
                    {"type": "email", "value": f"{index:064x}"} for index in range(10_001)
                ],
                "payload": {},
            }
        ]
    ).to_batches()[0]
    connector = meta_connector()

    with pytest.raises(ValueError, match="exceeds request batching `max_rows`"):
        plan_meta_requests(
            binding=_meta_audience_binding(),
            surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(sync_name="meta_sync", operation_pages=(page,)),
            ),
        )


def test_meta_managed_custom_audience_reuses_existing_remote_target() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=200,
                json_body={
                    "data": [
                        {"id": "aud_other", "name": "other", "subtype": "CUSTOM"},
                        {"id": "aud_vip", "name": "vip", "subtype": "CUSTOM"},
                    ]
                },
            ),
            HttpResponse(status_code=200, json_body={"request_id": "members-1"}),
        ],
        requests=[],
    )
    sync = _managed_audience_sync(transport=transport)
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
        registry_key(binding=binding, surface=CUSTOM_AUDIENCES_SURFACE, logical_target="vip")
    )
    assert record is not None
    assert record.remote == RemoteTarget(
        remote_id="aud_vip",
        display_name="vip",
        metadata={"subtype": "CUSTOM"},
    )
    assert [request.method for request in transport.requests] == ["GET", "POST"]
    assert transport.requests[0].url == ("https://graph.facebook.com/v25.0/act_123/customaudiences")
    assert transport.requests[1].url == "https://graph.facebook.com/v25.0/aud_vip/users"


def test_meta_managed_custom_audience_creates_missing_target_before_membership() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(status_code=200, json_body={"data": []}),
            HttpResponse(status_code=200, json_body={"id": "aud_created"}),
            HttpResponse(status_code=200, json_body={"request_id": "members-1"}),
        ],
        requests=[],
    )
    sync = _managed_audience_sync(transport=transport)
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
    assert create_request.url == "https://graph.facebook.com/v25.0/act_123/customaudiences"
    assert create_request.headers["Authorization"] == "Bearer token"
    assert create_request.json_body == {
        "name": "vip",
        "subtype": "CUSTOM",
        "description": "Managed by RETL for `vip`.",
        "customer_file_source": "USER_PROVIDED_ONLY",
    }
    assert transport.requests[2].url == "https://graph.facebook.com/v25.0/aud_created/users"
    binding = cast(DestinationBinding, sync.destination)
    record = store.get(
        registry_key(binding=binding, surface=CUSTOM_AUDIENCES_SURFACE, logical_target="vip")
    )
    assert record is not None
    assert record.remote.remote_id == "aud_created"


def test_meta_managed_custom_audience_dry_run_plans_without_create_or_registry_write() -> None:
    transport = QueueTransport(
        responses=[HttpResponse(status_code=200, json_body={"data": []})],
        requests=[],
    )
    sync = _managed_audience_sync(transport=transport)
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
        dry_run=True,
        runtime_store=store,
    )

    assert result.target_resolution is not None
    assert result.target_resolution.status == "planned"
    assert result.target_resolution.planned_create_count == 1
    assert [request.method for request in transport.requests] == ["GET"]
    binding = cast(DestinationBinding, sync.destination)
    assert (
        store.get(
            registry_key(binding=binding, surface=CUSTOM_AUDIENCES_SURFACE, logical_target="vip")
        )
        is None
    )


def test_meta_managed_custom_audience_uses_runtime_registry_without_remote_lookup() -> None:
    transport = QueueTransport(
        responses=[HttpResponse(status_code=200, json_body={"request_id": "members-1"})],
        requests=[],
    )
    sync = _managed_audience_sync(transport=transport)
    binding = cast(DestinationBinding, sync.destination)
    store = DuckDBRuntimeStore(database=":memory:")
    store.put(
        TargetRegistryRecord(
            key=registry_key(
                binding=binding,
                surface=CUSTOM_AUDIENCES_SURFACE,
                logical_target="vip",
            ),
            remote=RemoteTarget(remote_id="aud_stored", display_name="vip"),
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
    assert [request.method for request in transport.requests] == ["POST"]
    assert transport.requests[0].url == "https://graph.facebook.com/v25.0/aud_stored/users"


def test_meta_managed_custom_audience_partner_error_fails_target_resolution() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=400,
                json_body={"error": {"message": "Custom Audience Terms Not Accepted", "code": 200}},
            )
        ],
        requests=[],
    )
    sync = _managed_audience_sync(transport=transport)

    with pytest.raises(DestinationCompatibilityError, match="Custom Audience Terms Not Accepted"):
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


def test_meta_managed_custom_audience_rejects_name_collision_for_non_custom_subtype() -> None:
    transport = QueueTransport(
        responses=[
            HttpResponse(
                status_code=200,
                json_body={"data": [{"id": "aud_web", "name": "vip", "subtype": "WEBSITE"}]},
            )
        ],
        requests=[],
    )
    sync = _managed_audience_sync(transport=transport)

    with pytest.raises(DestinationCompatibilityError, match="unsupported subtype"):
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


def test_meta_non_dry_run_uses_bounded_custom_audience_batches() -> None:
    connector = meta_connector()
    transport = RecordingTransport(requests=[])
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "ad_account_id": "123",
            "api_version": "v25.0",
            "transport": transport,  # type: ignore[dict-item]
        },
        credentials={"access_token": retl.secrets.literal("token")},
        target_mappings=(
            TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="aud_123")),
        ),
    )
    resolved_auth = SimpleNamespace(mode="access_token", headers={"Authorization": "Bearer token"})
    hook = connector.submission_hook
    assert hook is not None
    reconciled = cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="meta_sync", operation_pages=(_audience_page(),)),
    )
    selected_plan = plan_meta_requests(
        binding=binding,
        surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
        reconciled=reconciled,
    ).plans[:1]
    assert len(selected_plan) == 1

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
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
    assert evidence.request_batch_count == 1
    assert [request.method for request in transport.requests] == ["POST"]
    assert all(
        "operation" not in cast(dict[str, Any], request.json_body) for request in transport.requests
    )
    assert all(request.headers["Authorization"] == "Bearer token" for request in transport.requests)


def test_meta_events_plans_conversions_api_batches_by_pixel_route() -> None:
    connector = meta_connector()
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "ad_account_id": "123",
            "api_version": "v25.0",
            "action_source": "website",
            "test_event_code": "TEST123",
            "event_routes": {"Purchase": "pixel_purchase", "Lead": "pixel_lead"},
        },
    )

    plan = plan_meta_requests(
        binding=binding,
        surface=connector.surface(EVENTS_SURFACE),
        reconciled=cast(
            EventReconcileEvidence,
            SimpleNamespace(sync_name="meta_events", import_pages=(_event_page(),)),
        ),
    )

    assert plan.request_count == 2
    assert [request.request.path for request in plan.plans] == [
        "/v25.0/pixel_purchase/events",
        "/v25.0/pixel_lead/events",
    ]
    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    event = cast(list[dict[str, Any]], body["data"])[0]
    assert body["test_event_code"] == "TEST123"
    assert event["event_name"] == "Purchase"
    assert event["event_time"] == 1777550400
    assert event["event_id"] == "purchase-1"
    assert event["action_source"] == "website"
    assert event["event_source_url"] == "https://example.test/checkout"
    assert event["user_data"]["em"] == [
        "f979a1713bf697bbbf1fc65fa352d2bb2376ee4ddf41d3bb07f9fb8f6126d1db"
    ]
    assert event["user_data"]["external_id"] == [
        "a28b5da378b043b0e62b924e1b055bb96ada180943e81cfc9838cf9933a91f03"
    ]
    assert event["user_data"]["fbc"] == "fb.1.1777334400.click-id"
    assert event["user_data"]["fbp"] == "fb.1.1777334400.browser-id"
    assert event["user_data"]["client_ip_address"] == "203.0.113.10"
    assert event["user_data"]["client_user_agent"] == "Mozilla/5.0 RETL fixture"
    assert event["custom_data"] == {
        "value": 123.45,
        "currency": "USD",
        "order_id": "order-123",
    }


def test_meta_events_preserves_prehashed_email_phone_and_external_id() -> None:
    email_hash = "E" * 64
    phone_hash = "f" * 64
    external_id_hash = "1" * 64
    page = pa.Table.from_pylist(
        [
            {
                "operation": "import",
                "event_identity": "purchase-1",
                "event_key": {"event_id": "purchase-1"},
                "event_name": "Purchase",
                "occurred_at": "2026-04-30T12:00:00Z",
                "identifiers": [
                    {"type": "email", "value": f" {email_hash} "},
                    {"type": "phone_e164", "value": phone_hash},
                    {"type": "external_id", "value": external_id_hash},
                ],
                "payload": {"value": 123.45, "currency": "USD"},
            }
        ]
    ).to_batches()[0]
    connector = meta_connector()
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "ad_account_id": "123",
            "api_version": "v25.0",
            "action_source": "website",
            "pixel_id": "pixel_123",
        },
    )

    plan = plan_meta_requests(
        binding=binding,
        surface=connector.surface(EVENTS_SURFACE),
        reconciled=cast(
            EventReconcileEvidence,
            SimpleNamespace(sync_name="meta_events", import_pages=(page,)),
        ),
    )

    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    event = cast(list[dict[str, Any]], body["data"])[0]
    assert event["user_data"]["em"] == [email_hash.lower()]
    assert event["user_data"]["ph"] == [phone_hash]
    assert event["user_data"]["external_id"] == [external_id_hash]


def test_meta_events_plans_user_data_from_nested_json_string_identifiers() -> None:
    page = pa.Table.from_pylist(
        [
            {
                "operation": "import",
                "event_identity": "purchase-1",
                "event_key": {"event_id": "purchase-1"},
                "event_name": "Purchase",
                "occurred_at": "2026-04-30T12:00:00Z",
                "identifiers_json": [json.dumps({"type": "email", "value": "Buyer@Example.Test"})],
                "payload": {"value": 123.45, "currency": "USD"},
            }
        ]
    ).to_batches()[0]
    connector = meta_connector()
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "ad_account_id": "123",
            "api_version": "v25.0",
            "action_source": "website",
            "pixel_id": "pixel_123",
        },
    )

    plan = plan_meta_requests(
        binding=binding,
        surface=connector.surface(EVENTS_SURFACE),
        reconciled=cast(
            EventReconcileEvidence,
            SimpleNamespace(sync_name="meta_events", import_pages=(page,)),
        ),
    )

    body = cast(dict[str, Any], plan.plans[0].request.json_body)
    event = cast(list[dict[str, Any]], body["data"])[0]
    assert event["user_data"]["em"] == [
        "f979a1713bf697bbbf1fc65fa352d2bb2376ee4ddf41d3bb07f9fb8f6126d1db"
    ]


def test_meta_events_planning_keeps_origin_and_routing_out_of_request_body() -> None:
    connector = meta_connector()
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "ad_account_id": "123",
            "api_version": "v25.0",
            "action_source": "website",
            "test_event_code": "TEST123",
            "event_routes": {"Purchase": "pixel_purchase", "Lead": "pixel_lead"},
        },
    )

    plan = plan_meta_requests(
        binding=binding,
        surface=connector.surface(EVENTS_SURFACE),
        reconciled=cast(
            EventReconcileEvidence,
            SimpleNamespace(sync_name="meta_events", import_pages=(_event_page(),)),
        ),
    )

    assert plan.request_count == 2
    rendered_bodies = [str(request.request.json_body) for request in plan.plans]
    assert all("https://graph.facebook.com" not in body for body in rendered_bodies)
    assert all("pixel_" not in body for body in rendered_bodies)
    assert all("act_123" not in body for body in rendered_bodies)
    assert all("TEST123" in body for body in rendered_bodies)


def test_meta_events_non_dry_run_posts_to_pixel_with_bearer_auth() -> None:
    connector = meta_connector()
    transport = RecordingTransport(requests=[])
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "ad_account_id": "123",
            "api_version": "v25.0",
            "action_source": "website",
            "pixel_id": "pixel_123",
            "transport": transport,  # type: ignore[dict-item]
        },
        credentials={"access_token": retl.secrets.literal("token")},
    )
    resolved_auth = SimpleNamespace(mode="access_token", headers={"Authorization": "Bearer token"})
    hook = connector.submission_hook
    assert hook is not None

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(EVENTS_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=2,
            dry_run=False,
            resolved_auth=resolved_auth,
            reconciled=cast(
                EventReconcileEvidence,
                SimpleNamespace(sync_name="meta_events", import_pages=(_event_page(),)),
            ),
        ),
    )

    assert evidence.status == "confirmed"
    assert evidence.confirmed_count == 2
    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert request.url == "https://graph.facebook.com/v25.0/pixel_123/events"
    assert request.headers["Authorization"] == "Bearer token"


def test_meta_invalid_transport_config_raises_validation_error() -> None:
    connector = meta_connector()
    hook = connector.submission_hook
    assert hook is not None

    with pytest.raises(DeclarationValidationError, match="transport"):
        hook(
            binding=DestinationBinding(
                binding_name="meta_primary",
                destination_ref=connector.connector_ref,
                connector=connector,
                config={
                    "ad_account_id": "123",
                    "api_version": "v25.0",
                    "action_source": "website",
                    "pixel_id": "pixel_123",
                    "transport": object(),  # type: ignore[dict-item]
                },
                credentials={"access_token": retl.secrets.literal("token")},
            ),
            surface=connector.surface(EVENTS_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=2,
            dry_run=False,
            resolved_auth=SimpleNamespace(mode="access_token", headers={}),
            reconciled=cast(
                EventReconcileEvidence,
                SimpleNamespace(sync_name="meta_events", import_pages=(_event_page(),)),
            ),
        )


def test_meta_selected_request_plans_drive_transport_failure_evidence_without_replanning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = meta_connector()
    transport = FailingTransport(requests=[])
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "ad_account_id": "123",
            "api_version": "v25.0",
            "action_source": "website",
            "pixel_id": "pixel_123",
            "transport": transport,  # type: ignore[dict-item]
        },
        credentials={"access_token": retl.secrets.literal("token")},
    )
    reconciled = cast(
        EventReconcileEvidence,
        SimpleNamespace(sync_name="meta_events", import_pages=(_coordinated_event_page(),)),
    )
    selected_plan = plan_meta_requests(
        binding=binding,
        surface=connector.surface(EVENTS_SURFACE),
        reconciled=reconciled,
    ).plans[:1]

    def fail_replanning(**_: object) -> object:
        raise AssertionError("selected request plans must not be planned again")

    monkeypatch.setattr(meta_hooks, "plan_meta_requests", fail_replanning)
    hook = connector.submission_hook
    assert hook is not None

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(EVENTS_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=2,
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


def test_meta_sync_dry_run_reuses_ledger_request_plans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_plan = meta_hooks.plan_meta_requests
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

    connector = replace(meta_connector(), batch_planning_hook=counting_plan)
    monkeypatch.setattr(meta_hooks, "plan_meta_requests", fail_replanning)
    transport = RecordingTransport(requests=[])
    sync = retl.sync(
        name="meta_purchase_imports",
        declaration=_event_declaration(),
        destination=DestinationBinding(
            binding_name="meta_events",
            destination_ref=connector.connector_ref,
            connector=connector,
            credentials={"access_token": retl.secrets.literal("token")},
            config={
                "ad_account_id": "123",
                "api_version": "v25.0",
                "action_source": "website",
                "pixel_id": "pixel_123",
                "transport": transport,  # type: ignore[dict-item]
            },
        ),
        surface=EVENTS_SURFACE,
    )
    reconciled = SimpleNamespace(
        phase="reconcile",
        status="succeeded",
        sync_name=sync.name,
        scope=destination_progress_scope(sync),
        import_pages=(_coordinated_event_page(),),
        import_count=2,
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


def test_meta_sync_dry_run_reuses_empty_connector_request_plan(
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

    connector = replace(meta_connector(), batch_planning_hook=empty_plan)
    monkeypatch.setattr(meta_hooks, "plan_meta_requests", fail_replanning)
    transport = RecordingTransport(requests=[])
    sync = retl.sync(
        name="meta_purchase_imports",
        declaration=_event_declaration(),
        destination=DestinationBinding(
            binding_name="meta_events",
            destination_ref=connector.connector_ref,
            connector=connector,
            credentials={"access_token": retl.secrets.literal("token")},
            config={
                "ad_account_id": "123",
                "api_version": "v25.0",
                "action_source": "website",
                "pixel_id": "pixel_123",
                "transport": transport,  # type: ignore[dict-item]
            },
        ),
        surface=EVENTS_SURFACE,
    )
    reconciled = SimpleNamespace(
        phase="reconcile",
        status="succeeded",
        sync_name=sync.name,
        scope=destination_progress_scope(sync),
        import_pages=(_coordinated_event_page(),),
        import_count=2,
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


def test_meta_submission_uses_selected_plans_that_seeded_batch_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_plan = meta_hooks.plan_meta_requests
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

    connector = replace(meta_connector(), batch_planning_hook=counting_plan)
    monkeypatch.setattr(meta_hooks, "plan_meta_requests", fail_replanning)
    transport = RecordingTransport(requests=[])
    sync = retl.sync(
        name="meta_purchase_imports",
        declaration=_event_declaration(),
        destination=DestinationBinding(
            binding_name="meta_events",
            destination_ref=connector.connector_ref,
            connector=connector,
            credentials={"access_token": retl.secrets.literal("token")},
            config={
                "ad_account_id": "123",
                "api_version": "v25.0",
                "action_source": "website",
                "pixel_id": "pixel_123",
                "transport": transport,  # type: ignore[dict-item]
            },
        ),
        surface=EVENTS_SURFACE,
    )
    reconciled = SimpleNamespace(
        phase="reconcile",
        status="succeeded",
        sync_name=sync.name,
        scope=destination_progress_scope(sync),
        import_pages=(_coordinated_event_page(),),
        import_count=2,
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


def test_meta_submission_hook_rejects_multi_batch_selected_plans_without_sending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = meta_connector()
    transport = RecordingTransport(requests=[])
    binding = DestinationBinding(
        binding_name="meta_events",
        destination_ref=connector.connector_ref,
        connector=connector,
        credentials={"access_token": retl.secrets.literal("token")},
        config={
            "ad_account_id": "123",
            "api_version": "v25.0",
            "action_source": "website",
            "pixel_id": "pixel_123",
            "transport": transport,  # type: ignore[dict-item]
        },
    )
    surface = connector.surface(EVENTS_SURFACE)
    reconciled = cast(
        EventReconcileEvidence,
        SimpleNamespace(
            sync_name="meta_purchase_imports",
            import_pages=(_coordinated_event_page(),),
        ),
    )
    selected = meta_hooks.plan_meta_requests(
        binding=binding,
        surface=surface,
        reconciled=reconciled,
    ).plans[:1]
    assert len(selected) == 1

    def fail_replanning(**_: object) -> object:
        raise AssertionError("selected request plans must not be planned again")

    monkeypatch.setattr(meta_hooks, "plan_meta_requests", fail_replanning)
    hook = connector.submission_hook
    assert hook is not None

    with pytest.raises(ValueError, match="exactly one selected request batch"):
        hook(
            binding=binding,
            surface=surface,
            delivery_outcome="succeeded",
            attempted_count=4,
            dry_run=False,
            resolved_auth=SimpleNamespace(mode="access_token", headers={}),
            reconciled=reconciled,
            selected_request_plans=(selected[0], selected[0]),
        )

    assert transport.requests == []


def test_meta_submission_evidence_carries_http_and_partner_error_diagnostics() -> None:
    connector = meta_connector()
    transport = StaticTransport(
        response=HttpResponse(
            status_code=400,
            json_body={
                "error": {
                    "message": "Invalid parameter",
                    "code": 100,
                    "error_subcode": 2804003,
                    "error_data": {
                        "blame_field_specs": [
                            ["custom_data", "currency"],
                            ["custom_data", "value"],
                        ],
                        "access_token": "secret-token",
                    },
                }
            },
        ),
        requests=[],
    )
    binding = DestinationBinding(
        binding_name="meta_primary",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "ad_account_id": "123",
            "api_version": "v25.0",
            "action_source": "website",
            "pixel_id": "pixel_123",
            "transport": transport,  # type: ignore[dict-item]
        },
        credentials={"access_token": retl.secrets.literal("token")},
    )
    resolved_auth = SimpleNamespace(mode="access_token", headers={"Authorization": "Bearer token"})
    hook = connector.submission_hook
    assert hook is not None

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(EVENTS_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=2,
            dry_run=False,
            resolved_auth=resolved_auth,
            reconciled=cast(
                EventReconcileEvidence,
                SimpleNamespace(sync_name="meta_events", import_pages=(_event_page(),)),
            ),
        ),
    )

    assert evidence.status == "terminal_record_failure"
    assert evidence.pre_acceptance_failure_category is None
    assert evidence.http_status == 400
    assert evidence.terminal_record_failure_count == 2
    assert evidence.partner_error_code == "100"
    assert evidence.partner_error_subcode == "2804003"
    assert evidence.summary == "Invalid parameter code=100 subcode=2804003"
    assert evidence.partner_error_detail is not None
    assert "blame_field_specs" in evidence.partner_error_detail
    assert "custom_data" in evidence.partner_error_detail
    assert "access_token=[redacted]" in evidence.partner_error_detail
    assert "secret-token" not in evidence.partner_error_detail


def test_meta_event_terminal_batch_is_not_duplicate_submitted_for_same_request_plan(
    tmp_path: Path,
) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")
    failing_transport = StaticTransport(
        response=HttpResponse(
            status_code=400,
            json_body={
                "error": {
                    "message": "Invalid parameter",
                    "code": 100,
                    "error_subcode": 2804003,
                }
            },
        ),
        requests=[],
    )
    failing_sync = _event_sync(transport=failing_transport, on_failure="stop_on_any")
    reconciled = SimpleNamespace(
        phase="reconcile",
        status="succeeded",
        sync_name=failing_sync.name,
        scope=destination_progress_scope(failing_sync),
        import_pages=(_coordinated_event_page(),),
        import_count=2,
    )

    failed = sync_destination(
        sync=failing_sync,
        reconciled=cast(Any, reconciled),
        dry_run=False,
        runtime_store=store,
        run_id="run-1",
        attempt_id="attempt-1",
        page_index=1,
    )

    assert failed.status == "failed"
    assert len(failing_transport.requests) == 1
    assert failed.submission.status == "terminal_record_failure"
    failed_batches = store.list_destination_batches(scope=destination_progress_scope(failing_sync))
    assert len(failed_batches) == 1
    assert failed_batches[0].status == "failed"
    assert failed_batches[0].completion_state == "unresolved"
    assert failed_batches[0].http_status == 400
    assert failed_batches[0].last_failure_category == "terminal_record"
    assert failed_batches[0].last_error_detail is not None

    retry_transport = StaticTransport(
        response=HttpResponse(status_code=200, json_body={"request_id": "duplicate"}),
        requests=[],
    )
    retry_sync = _event_sync(transport=retry_transport, on_failure="stop_on_any")

    retried = sync_destination(
        sync=retry_sync,
        reconciled=cast(Any, reconciled),
        dry_run=False,
        runtime_store=store,
        run_id="run-2",
        attempt_id="attempt-2",
        page_index=1,
    )

    assert retried.status == "failed"
    assert len(retry_transport.requests) == 0
    assert retried.submission.status == "planned"
    assert "unresolved destination batch ledger state" in retried.submission.summary
    retried_batches = store.list_destination_batches(scope=destination_progress_scope(retry_sync))
    assert len(retried_batches) == 1
    assert retried_batches[0].batch_id == failed_batches[0].batch_id
    assert retried_batches[0].attempt_count == 1
    assert retried_batches[0].status == "failed"
    assert retried_batches[0].completion_state == "unresolved"


def test_meta_skipped_destination_batch_is_terminal_for_page_local_submission(
    tmp_path: Path,
) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")
    failing_transport = StaticTransport(
        response=HttpResponse(
            status_code=400,
            json_body={
                "error": {
                    "message": "Invalid parameter",
                    "code": 100,
                    "error_subcode": 2804003,
                }
            },
        ),
        requests=[],
    )
    sync = _event_sync(transport=failing_transport, on_failure="stop_on_any")
    reconciled = SimpleNamespace(
        phase="reconcile",
        status="succeeded",
        sync_name=sync.name,
        scope=destination_progress_scope(sync),
        import_pages=(_coordinated_event_page(),),
        import_count=2,
    )
    failed = sync_destination(
        sync=sync,
        reconciled=cast(Any, reconciled),
        dry_run=False,
        runtime_store=store,
        run_id="run-1",
        attempt_id="attempt-1",
        page_index=1,
    )
    failed_batch = store.list_destination_batches(scope=destination_progress_scope(sync))[0]
    skipped = store.upsert_destination_batch(
        replace(
            failed_batch,
            status="skipped",
            completion_state="resolved",
            retry_eligible=False,
        )
    )
    retry_transport = StaticTransport(
        response=HttpResponse(status_code=200, json_body={"request_id": "duplicate"}),
        requests=[],
    )
    retry_sync = _event_sync(transport=retry_transport)

    retried = sync_destination(
        sync=retry_sync,
        reconciled=cast(Any, reconciled),
        dry_run=False,
        runtime_store=store,
        run_id="run-2",
        attempt_id="attempt-2",
        page_index=1,
    )
    retried_batches = store.list_destination_batches(scope=destination_progress_scope(retry_sync))

    assert failed.status == "failed"
    assert skipped.status == "skipped"
    assert len(failing_transport.requests) == 1
    assert len(retry_transport.requests) == 0
    assert retried.status == "succeeded"
    assert retried.submission.status == "confirmed"
    assert retried.submission.confirmed_count == 0
    assert retried.submission.accepted_count == 0
    assert retried.submission.attempted_count == 0
    assert "Skipped 1 resolved destination batch(es)." in retried.submission.summary
    assert retried_batches == (skipped,)
