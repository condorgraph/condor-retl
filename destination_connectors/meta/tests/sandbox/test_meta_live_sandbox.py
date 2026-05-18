from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pyarrow as pa
import pytest
from retl_meta.definitions import CUSTOM_AUDIENCES_SURFACE, EVENTS_SURFACE, meta_connector

import retl
from retl.declarations import DestinationBinding
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.targets import RemoteTarget, TargetMapping
from retl.events.reconcile import EventReconcileEvidence
from retl.state_runtime.reconcile import StateReconcileEvidence

pytestmark = pytest.mark.live_sandbox


def test_meta_events_live_sandbox_posts_test_event() -> None:
    _require_live_sandbox()
    connector = meta_connector()
    event_name = "Purchase"
    hook = connector.submission_hook
    assert hook is not None
    binding = DestinationBinding(
        binding_name="meta_events_live_sandbox",
        destination_ref=connector.connector_ref,
        connector=connector,
        credentials={"access_token": retl.secrets.literal(_required_env("ACCESS_TOKEN"))},
        config={
            "ad_account_id": _required_env("AD_ACCOUNT_ID"),
            "action_source": _required_env("EVENTS__ACTION_SOURCE"),
            "event_routes": {event_name: _required_env("EVENTS__EVENT_ROUTES__PURCHASE")},
            "test_event_code": _required_env("EVENTS__TEST_EVENT_CODE"),
        },
    )

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(EVENTS_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=1,
            dry_run=False,
            resolved_auth=_resolved_auth(),
            reconciled=cast(
                EventReconcileEvidence,
                SimpleNamespace(
                    sync_name="meta_events_live_sandbox",
                    import_pages=(_event_page(event_name=event_name),),
                ),
            ),
        ),
    )

    assert evidence.status in {"confirmed", "accepted"}
    assert evidence.confirmed_count + evidence.accepted_count == 1
    assert evidence.request_batch_count == 1
    assert evidence.receipts


def test_meta_custom_audiences_live_sandbox_adds_and_removes_synthetic_customer() -> None:
    _require_live_sandbox()
    connector = meta_connector()
    synthetic_customer_id = f"retl-sandbox-{uuid4().hex}"
    hook = connector.submission_hook
    assert hook is not None
    binding = DestinationBinding(
        binding_name="meta_custom_audiences_live_sandbox",
        destination_ref=connector.connector_ref,
        connector=connector,
        credentials={"access_token": retl.secrets.literal(_required_env("ACCESS_TOKEN"))},
        config={"ad_account_id": _required_env("AD_ACCOUNT_ID")},
        target_mappings=(
            TargetMapping(
                logical_target="sample_customers",
                remote=RemoteTarget(
                    remote_id=_required_env("CUSTOM_AUDIENCES__CONTAINERS__SAMPLE_CUSTOMERS")
                ),
            ),
        ),
    )

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(CUSTOM_AUDIENCES_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=2,
            dry_run=False,
            resolved_auth=_resolved_auth(),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    sync_name="meta_custom_audiences_live_sandbox",
                    operation_pages=(_audience_add_remove_page(synthetic_customer_id),),
                ),
            ),
        ),
    )

    assert evidence.status in {"confirmed", "accepted"}
    assert evidence.confirmed_count + evidence.accepted_count == 2
    assert evidence.request_batch_count == 2
    assert evidence.receipts


def _require_live_sandbox() -> None:
    if os.environ.get("RETL_RUN_LIVE_SANDBOX") != "1":
        pytest.skip("Set RETL_RUN_LIVE_SANDBOX=1 to run Meta live sandbox tests.")


def _required_env(name: str) -> str:
    full_name = f"DESTINATIONS__META__{name}"
    value = os.environ.get(full_name)
    if value is None or not value.strip():
        pytest.skip(f"Missing required Meta sandbox env var `{full_name}`.")
    return value.strip()


def _resolved_auth() -> SimpleNamespace:
    return SimpleNamespace(headers={"Authorization": f"Bearer {_required_env('ACCESS_TOKEN')}"})


def _event_page(*, event_name: str) -> pa.RecordBatch:
    event_id = f"retl-sandbox-{uuid4().hex}"
    return pa.Table.from_pylist(
        [
            {
                "operation": "import",
                "event_identity": event_id,
                "event_key": {"event_id": event_id},
                "event_name": event_name,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "identifiers": [{"type": "external_id", "value": f"customer-{uuid4().hex}"}],
                "payload": {
                    "event_name": event_name,
                    "value": 1.23,
                    "currency": "USD",
                    "order_id": event_id,
                },
            },
        ]
    ).to_batches()[0]


def _audience_add_remove_page(customer_id: str) -> pa.RecordBatch:
    email = f"{customer_id}@example.test"
    rows = [
        {
            "operation": operation,
            "record_identity": f"{customer_id}-{operation}",
            "target": "sample_customers",
            "state_key": {"customer_id": customer_id},
            "identifiers": [{"type": "email", "value": email}],
            "payload": {},
        }
        for operation in ("upsert", "remove")
    ]
    return pa.Table.from_pylist(rows).to_batches()[0]
