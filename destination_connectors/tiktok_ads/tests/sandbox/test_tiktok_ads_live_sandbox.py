from __future__ import annotations

import os
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pyarrow as pa
import pytest
from retl_tiktok_ads.common import RequestsTikTokAdsTransport, join_url, tiktok_ads_config
from retl_tiktok_ads.definitions import CUSTOM_AUDIENCES_SURFACE, tiktok_ads_connector

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.declarations import DestinationBinding
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.targets import registry_key
from retl.runtime import destination_progress_scope
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl.sync_runtime.submission import sync_destination

pytestmark = pytest.mark.live_sandbox


def test_tiktok_ads_managed_custom_audience_live_sandbox() -> None:
    _require_live_sandbox()
    connector = tiktok_ads_connector()
    target = _sandbox_target_name()
    binding = DestinationBinding(
        binding_name="tiktok_ads_live_sandbox",
        destination_ref=connector.connector_ref,
        connector=connector,
        credentials={"access_token": retl.secrets.literal(_required_env("ACCESS_TOKEN"))},
        config={
            "advertiser_id": _required_env("ADVERTISER_ID"),
            "api_version": os.environ.get("DESTINATIONS__TIKTOK_ADS__API_VERSION", "v" + "1.3"),
            "mobile_advertising_id_type": os.environ.get(
                "DESTINATIONS__TIKTOK_ADS__MOBILE_ADVERTISING_ID_TYPE",
                "MAID_SHA256",
            ),
        },
    )
    sync = retl.sync(
        name="tiktok_ads_custom_audience_live_sandbox",
        declaration=_audience_declaration(),
        destination=binding,
        surface=CUSTOM_AUDIENCES_SURFACE,
    )
    store = DuckDBRuntimeStore(database=":memory:")
    remote_id: str | None = None
    try:
        result = sync_destination(
            sync=sync,
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    phase="reconcile",
                    sync_name=sync.name,
                    operation_pages=(_audience_add_remove_page(target),),
                    status="succeeded",
                    scope=destination_progress_scope(sync),
                ),
            ),
            dry_run=False,
            runtime_store=store,
        )

        assert result.submission.status in {"confirmed", "accepted"}
        assert result.submission.confirmed_count + result.submission.accepted_count == 2
        assert result.submission.request_batch_count == 2
        assert result.target_resolution is not None
        assert result.target_resolution.managed_created_count in {0, 1}
        record = store.get(
            registry_key(binding=binding, surface=CUSTOM_AUDIENCES_SURFACE, logical_target=target)
        )
        assert record is not None
        remote_id = record.remote.remote_id
    finally:
        if remote_id is not None:
            _delete_custom_audience(binding, audience_id=remote_id)


def _require_live_sandbox() -> None:
    if os.environ.get("RETL_RUN_LIVE_SANDBOX") != "1":
        pytest.skip("Set RETL_RUN_LIVE_SANDBOX=1 to run TikTok Ads live sandbox tests.")


def _required_env(name: str) -> str:
    full_name = f"DESTINATIONS__TIKTOK_ADS__{name}"
    value = os.environ.get(full_name)
    if value is None or not value.strip():
        pytest.skip(f"Missing required TikTok Ads sandbox env var `{full_name}`.")
    return value.strip()


def _sandbox_target_name() -> str:
    configured = os.environ.get(
        "DESTINATIONS__TIKTOK_ADS__CUSTOM_AUDIENCES__CONTAINERS__SAMPLE_CUSTOMERS"
    )
    if configured is not None and configured.strip():
        return f"{configured.strip()}_{uuid4().hex[:12]}"
    return f"retl_tiktok_ads_sandbox_{uuid4().hex[:12]}"


def _audience_declaration() -> retl.State:
    return retl.state(
        name="tiktok_ads_custom_audience_live_sandbox",
        source=retl.source(
            name="tiktok_ads_live_customers",
            mode="snapshot",
            query="select customer_id, email, audience_key from customers",
        ),
        key={"customer_id": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
    )


def _audience_add_remove_page(target: str) -> pa.RecordBatch:
    synthetic_customer_id = uuid4().hex
    email = f"{synthetic_customer_id}@example.test"
    rows = [
        {
            "operation": operation,
            "record_identity": f"{synthetic_customer_id}-{operation}",
            "collect_id": "00000000-0001-7000-8000-000000000000",
            "sequence_order": index,
            "target_json": {"value": target},
            "key_json": {"customer_id": synthetic_customer_id},
            "identifiers_json": [{"type": "email", "value": email}],
            "payload_json": {},
        }
        for index, operation in enumerate(("upsert", "remove"))
    ]
    return pa.Table.from_pylist(rows).to_batches()[0]


def _delete_custom_audience(binding: DestinationBinding, *, audience_id: str) -> None:
    config = tiktok_ads_config(binding)
    response = RequestsTikTokAdsTransport().send(
        HttpRequest(
            method="POST",
            url=join_url(config, f"/open_api/{config.api_version}/dmp/custom_audience/delete/"),
            headers={
                "Access-Token": _required_env("ACCESS_TOKEN"),
                "Content-Type": "application/json",
            },
            json_body={
                "advertiser_id": config.advertiser_id,
                "custom_audience_ids": [audience_id],
            },
        )
    )
    assert _delete_succeeded(response), response.json_body or response.body_text


def _delete_succeeded(response: HttpResponse) -> bool:
    return response.status_code in range(200, 300) and response.json_body.get("code") in {
        0,
        "0",
        None,
    }
