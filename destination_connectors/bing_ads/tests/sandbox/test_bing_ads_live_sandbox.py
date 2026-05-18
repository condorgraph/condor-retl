from __future__ import annotations

import os
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pyarrow as pa
import pytest
import requests
from retl_bing_ads.common import (
    RequestsBingAdsTransport,
    bing_ads_config,
    bing_ads_headers,
    join_url,
)
from retl_bing_ads.definitions import CUSTOMER_LISTS_SURFACE, bing_ads_connector

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.declarations import DestinationBinding
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.targets import registry_key
from retl.runtime import destination_progress_scope
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl.sync_runtime.submission import sync_destination

MICROSOFT_OAUTH_VERSION = "v" + "2.0"


@pytest.mark.live_sandbox
def test_bing_ads_managed_customer_list_live_sandbox() -> None:
    _require_live_sandbox()
    target = f"retl-sandbox-{uuid4().hex[:12]}"
    connector = bing_ads_connector()
    access_token = _access_token()
    binding = DestinationBinding(
        binding_name="bing_ads_live_sandbox",
        destination_ref=connector.connector_ref,
        connector=connector,
        credentials={
            "access_token": retl.secrets.literal(access_token),
            "developer_token": retl.secrets.literal(_required_env("DEVELOPER_TOKEN")),
        },
        config={
            "customer_account_id": _required_env("CUSTOMER_ACCOUNT_ID"),
            "customer_id": _required_env("CUSTOMER_ID"),
            "api_version": os.environ.get("DESTINATIONS__BING_ADS__API_VERSION", "v13"),
            "accept_customer_match_terms": _bool_env("ACCEPT_CUSTOMER_MATCH_TERMS", True),
        },
    )
    sync = retl.sync(
        name="bing_ads_customer_list_live_sandbox",
        declaration=_audience_declaration(),
        destination=binding,
        surface=CUSTOMER_LISTS_SURFACE,
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

        assert result.submission is not None
        assert result.submission.status in {"confirmed", "accepted"}
        assert result.submission.confirmed_count + result.submission.accepted_count == 2
        assert result.submission.request_batch_count == 2
        assert result.target_resolution is not None
        assert result.target_resolution.managed_created_count == 1
        record = store.get(
            registry_key(binding=binding, surface=CUSTOMER_LISTS_SURFACE, logical_target=target)
        )
        assert record is not None
        remote_id = record.remote.remote_id
    finally:
        if remote_id is not None:
            _delete_audience(binding, access_token=access_token, audience_id=remote_id)


def _require_live_sandbox() -> None:
    if os.environ.get("RETL_RUN_LIVE_SANDBOX") != "1":
        pytest.skip("Set RETL_RUN_LIVE_SANDBOX=1 to run Bing Ads live sandbox tests.")


def _required_env(name: str) -> str:
    full_name = f"DESTINATIONS__BING_ADS__{name}"
    value = os.environ.get(full_name)
    if value is not None and value.strip():
        return value.strip()
    pytest.skip(f"Missing required Bing Ads sandbox env var `{full_name}`.")


def _bool_env(name: str, default: bool) -> bool:
    full_name = f"DESTINATIONS__BING_ADS__{name}"
    raw = os.environ.get(full_name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _access_token() -> str:
    refresh_token = os.environ.get("DESTINATIONS__BING_ADS__REFRESH_TOKEN")
    client_id = os.environ.get("DESTINATIONS__BING_ADS__CLIENT_ID")
    if refresh_token and refresh_token.strip() and client_id and client_id.strip():
        return _refresh_access_token(
            client_id=client_id.strip(), refresh_token=refresh_token.strip()
        )
    return _required_env("ACCESS_TOKEN")


def _refresh_access_token(*, client_id: str, refresh_token: str) -> str:
    response = requests.post(
        f"https://login.microsoftonline.com/common/oauth2/{MICROSOFT_OAUTH_VERSION}/token",
        data={
            "client_id": client_id,
            "scope": "https://ads.microsoft.com/msads.manage offline_access",
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
            "redirect_uri": "https://login.microsoftonline.com/common/oauth2/nativeclient",
        },
        timeout=30,
    )
    if response.status_code != 200:
        pytest.fail(f"Bing Ads refresh-token exchange failed: HTTP {response.status_code}.")
    access_token = response.json().get("access_token")
    if not isinstance(access_token, str) or not access_token.strip():
        pytest.fail("Bing Ads refresh-token exchange did not return `access_token`.")
    return access_token.strip()


def _audience_declaration() -> retl.State:
    return retl.state(
        name="bing_ads_customer_list_live_sandbox",
        source=retl.source(
            name="bing_ads_live_customers",
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


def _delete_audience(
    binding: DestinationBinding,
    *,
    access_token: str,
    audience_id: str,
) -> None:
    config = bing_ads_config(binding)
    response = RequestsBingAdsTransport().send(
        HttpRequest(
            method="DELETE",
            url=join_url(config, f"/CampaignManagement/{config.api_version}/Audiences"),
            headers=bing_ads_headers(
                config=config,
                resolved_auth=SimpleNamespace(
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "DeveloperToken": _required_env("DEVELOPER_TOKEN"),
                    }
                ),
            ),
            json_body={"AudienceIds": [audience_id]},
        )
    )
    assert _delete_succeeded(response), response.json_body or response.body_text


def _delete_succeeded(response: HttpResponse) -> bool:
    if response.status_code not in range(200, 300):
        return False
    errors = response.json_body.get("PartialErrors")
    return not isinstance(errors, list) or not errors
