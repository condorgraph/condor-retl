from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pyarrow as pa
import pytest
from retl_google_ads_data_manager.definitions import (
    CUSTOMER_MATCH_SURFACE,
    EVENTS_SURFACE,
    google_ads_data_manager_connector,
)

import retl
from retl.auth import EnvironmentSecretResolver, resolve_auth
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.targets import RemoteTarget, TargetMapping
from retl.events.reconcile import EventReconcileEvidence
from retl.state_runtime.reconcile import StateReconcileEvidence

pytestmark = pytest.mark.live_sandbox


def test_customer_match_live_sandbox_adds_and_removes_synthetic_customer() -> None:
    _require_live_sandbox()
    _preflight_required_binding_env()
    connector = google_ads_data_manager_connector()
    synthetic_customer_id = f"retl-sandbox-{uuid4().hex}"
    hook = connector.submission_hook
    assert hook is not None
    registry = retl.destinations.DestinationRegistry()
    registry.register(connector)
    binding = retl.destinations.load(
        "retl/google-ads-data-manager",
        binding_name="google_ads_customer_match_live_sandbox",
        auth_mode="service_account",
        credential_namespace="destinations.google_ads.service_account",
        config_namespace="destinations.google_ads",
        config={"customer_match_terms_accepted": True},
        target_mappings=(
            TargetMapping(
                logical_target="sample_customers",
                remote=RemoteTarget(
                    remote_id=_required_env("CUSTOMER_MATCH", "CONTAINERS", "SAMPLE_CUSTOMERS")
                ),
            ),
        ),
        registry=registry,
    )

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(CUSTOMER_MATCH_SURFACE),
            delivery_outcome="accepted",
            attempted_count=2,
            dry_run=False,
            resolved_auth=resolve_auth(
                mode=connector.auth_modes[1],
                credentials=binding.credentials,
                resolver=EnvironmentSecretResolver(),
            ),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    sync_name="google_ads_customer_match_live_sandbox",
                    operation_pages=(_audience_add_remove_page(synthetic_customer_id),),
                ),
            ),
        ),
    )

    assert evidence.status == "accepted"
    assert evidence.accepted_count == 2
    assert evidence.request_batch_count == 2
    assert len(evidence.receipts) == 2
    assert evidence.remote_handles


def test_events_live_sandbox_ingests_synthetic_event() -> None:
    _require_live_sandbox()
    _preflight_required_binding_env()
    event_destination_id = _required_env("EVENTS", "DESTINATION_ID")
    connector = google_ads_data_manager_connector()
    synthetic_event_id = f"retl-sandbox-event-{uuid4().hex}"
    hook = connector.submission_hook
    assert hook is not None
    registry = retl.destinations.DestinationRegistry()
    registry.register(connector)
    binding = retl.destinations.load(
        "retl/google-ads-data-manager",
        binding_name="google_ads_events_live_sandbox",
        auth_mode="service_account",
        credential_namespace="destinations.google_ads.service_account",
        config_namespace="destinations.google_ads",
        config={
            "event_destination_id": event_destination_id,
            "request_status_poll_interval_seconds": 1,
            "request_status_poll_timeout_seconds": 0,
        },
        registry=registry,
    )

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(EVENTS_SURFACE),
            delivery_outcome="accepted",
            attempted_count=1,
            dry_run=False,
            resolved_auth=resolve_auth(
                mode=connector.auth_modes[1],
                credentials=binding.credentials,
                resolver=EnvironmentSecretResolver(),
            ),
            reconciled=cast(
                EventReconcileEvidence,
                SimpleNamespace(
                    sync_name="google_ads_events_live_sandbox",
                    import_pages=(_event_import_page(synthetic_event_id),),
                ),
            ),
        ),
    )

    assert evidence.status == "accepted"
    assert evidence.accepted_count == 1
    assert evidence.request_batch_count == 1
    assert len(evidence.receipts) == 1
    assert evidence.remote_handles


def _require_live_sandbox() -> None:
    if os.environ.get("RETL_RUN_LIVE_SANDBOX") != "1":
        pytest.skip(
            "Set RETL_RUN_LIVE_SANDBOX=1 to run Google Ads Data Manager live sandbox tests."
        )


def _preflight_required_binding_env() -> None:
    _required_env("SERVICE_ACCOUNT", "PROJECT_ID")
    _required_env("SERVICE_ACCOUNT", "CLIENT_EMAIL")
    _required_env("SERVICE_ACCOUNT", "PRIVATE_KEY")
    _required_env("OPERATING_ACCOUNT_ID")


def _required_env(*parts: str) -> str:
    full_name = "DESTINATIONS__GOOGLE_ADS__" + "__".join(parts)
    value = os.environ.get(full_name)
    if value is not None and value.strip():
        return value.strip()
    pytest.skip(f"Missing required Google Ads sandbox env var `{full_name}`.")


def _audience_add_remove_page(customer_id: str) -> pa.RecordBatch:
    email = f"{customer_id}@example.test"
    rows = [
        {
            "operation": operation,
            "record_identity": f"{customer_id}-{operation}",
            "target": "sample_customers",
            "state_key": {"customer_id": customer_id},
            "identifiers_json": json.dumps([{"type": "email", "value": email}]),
            "payload_json": "{}",
        }
        for operation in ("upsert", "remove")
    ]
    return pa.Table.from_pylist(rows).to_batches()[0]


def _event_import_page(event_id: str) -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "import",
                "event_identity": event_id,
                "event_key": {"event_id": event_id},
                "event_name": "Purchase",
                "occurred_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "identifiers_json": json.dumps(
                    [
                        {
                            "type": "email",
                            "value": f"{event_id}@example.test",
                        }
                    ]
                ),
                "payload_json": json.dumps(
                    {
                        "currency": "USD",
                        "conversion_value": 1.0,
                        "event_source": "WEB",
                    },
                    sort_keys=True,
                ),
            }
        ]
    ).to_batches()[0]
