from __future__ import annotations

import os
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pyarrow as pa
import pytest
from retl_klaviyo.common import join_url, klaviyo_config, transport_from_config
from retl_klaviyo.definitions import (
    LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE,
    LIST_MEMBERSHIPS_SURFACE,
    PROFILES_SURFACE,
    klaviyo_connector,
)

import retl
from retl.auth import EnvironmentSecretResolver, resolve_auth
from retl.declarations import JSONValue
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.http import HttpRequest, HttpResponse
from retl.destinations.targets import ManagedTargetClient, RemoteTarget, TargetMapping
from retl.state_runtime.reconcile import StateReconcileEvidence

pytestmark = pytest.mark.live_sandbox
SANDBOX_PROFILE_EMAIL = "lanacooper735@fake.com"


def test_profiles_live_sandbox_upserts_fixed_profile_email() -> None:
    _require_live_sandbox()
    _required_env("PRIVATE_API_KEY")
    connector = klaviyo_connector()
    hook = connector.submission_hook
    assert hook is not None
    registry = retl.destinations.DestinationRegistry()
    registry.register(connector)
    binding = retl.destinations.load(
        "retl/klaviyo",
        binding_name="klaviyo_profiles_live_sandbox",
        credential_namespace="destinations.klaviyo",
        config_namespace="destinations.klaviyo",
        registry=registry,
    )
    synthetic_profile_key = f"retl-klaviyo-sandbox-{uuid4().hex}"

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(PROFILES_SURFACE),
            delivery_outcome="accepted",
            attempted_count=1,
            dry_run=False,
            resolved_auth=resolve_auth(
                mode=connector.auth_modes[0],
                credentials=binding.credentials,
                resolver=EnvironmentSecretResolver(),
            ),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    sync_name="klaviyo_profiles_live_sandbox",
                    operation_pages=(_profile_upsert_page(synthetic_profile_key),),
                ),
            ),
        ),
    )

    assert evidence.status == "accepted"
    assert evidence.accepted_count == 1
    assert evidence.request_batch_count == 1
    assert len(evidence.receipts) == 1
    assert evidence.remote_handles


def test_list_memberships_live_sandbox_adds_and_removes_fixed_profile() -> None:
    _require_live_sandbox()
    _required_env("PRIVATE_API_KEY")
    connector = klaviyo_connector()
    hook = connector.submission_hook
    assert hook is not None
    registry = retl.destinations.DestinationRegistry()
    registry.register(connector)
    binding = retl.destinations.load(
        "retl/klaviyo",
        binding_name="klaviyo_list_memberships_live_sandbox",
        credential_namespace="destinations.klaviyo",
        config_namespace="destinations.klaviyo",
        registry=registry,
    )
    resolved_auth = resolve_auth(
        mode=connector.auth_modes[0],
        credentials=binding.credentials,
        resolver=EnvironmentSecretResolver(),
    )
    target_client_hook = connector.managed_target_client_hook
    assert target_client_hook is not None
    target_client = cast(
        ManagedTargetClient,
        target_client_hook(
            binding=binding,
            surface=connector.surface(LIST_MEMBERSHIPS_SURFACE),
            resolved_auth=resolved_auth,
        ),
    )
    logical_list_name = f"RETL Klaviyo Sandbox {uuid4().hex}"
    list_target = target_client.create_target(
        logical_list_name,
        display_name=logical_list_name,
    )
    list_id = list_target.remote_id
    try:
        found_target = target_client.find_target(logical_list_name)
        assert found_target is not None
        assert found_target.remote_id == list_id
        list_binding = _list_binding(registry=registry, list_id=list_id)
        import_evidence = cast(
            DestinationSubmissionEvidence,
            hook(
                binding=list_binding,
                surface=connector.surface(LIST_MEMBERSHIPS_SURFACE),
                delivery_outcome="accepted",
                attempted_count=1,
                dry_run=False,
                resolved_auth=resolved_auth,
                reconciled=cast(
                    StateReconcileEvidence,
                    SimpleNamespace(
                        sync_name="klaviyo_list_memberships_live_sandbox",
                        operation_pages=(_list_membership_import_page(),),
                    ),
                ),
            ),
        )
        assert import_evidence.status == "accepted"
        assert import_evidence.accepted_count == 1
        assert import_evidence.request_batch_count == 1
        assert import_evidence.remote_handles

        profile_id = _wait_for_profile_id(
            binding,
            resolved_auth=resolved_auth,
            email=SANDBOX_PROFILE_EMAIL,
        )
        relationship_evidence = cast(
            DestinationSubmissionEvidence,
            hook(
                binding=list_binding,
                surface=connector.surface(LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE),
                delivery_outcome="succeeded",
                attempted_count=2,
                dry_run=False,
                resolved_auth=resolved_auth,
                reconciled=cast(
                    StateReconcileEvidence,
                    SimpleNamespace(
                        sync_name="klaviyo_list_memberships_by_profile_id_live_sandbox",
                        operation_pages=(_list_membership_profile_id_page(profile_id),),
                    ),
                ),
            ),
        )
    finally:
        _delete_sandbox_list(binding, resolved_auth=resolved_auth, list_id=list_id)

    assert relationship_evidence.status == "confirmed"
    assert relationship_evidence.confirmed_count == 2
    assert relationship_evidence.request_batch_count == 2


def _require_live_sandbox() -> None:
    if os.environ.get("RETL_RUN_LIVE_SANDBOX") != "1":
        pytest.skip("Set RETL_RUN_LIVE_SANDBOX=1 to run Klaviyo live sandbox tests.")


def _required_env(*parts: str) -> str:
    full_name = "DESTINATIONS__KLAVIYO__" + "__".join(parts)
    value = os.environ.get(full_name)
    if value is not None and value.strip():
        return value.strip()
    pytest.skip(f"Missing required Klaviyo sandbox env var `{full_name}`.")


def _list_binding(
    *,
    registry: retl.destinations.DestinationRegistry,
    list_id: str,
) -> retl.DestinationBinding:
    return retl.destinations.load(
        "retl/klaviyo",
        binding_name="klaviyo_list_memberships_live_sandbox",
        credential_namespace="destinations.klaviyo",
        config_namespace="destinations.klaviyo",
        target_mappings=(
            TargetMapping(
                logical_target="sample_list",
                remote=RemoteTarget(remote_id=list_id),
            ),
        ),
        registry=registry,
    )


def _delete_sandbox_list(
    binding: retl.DestinationBinding,
    *,
    resolved_auth: object,
    list_id: str,
) -> None:
    response = _send_klaviyo_request(
        binding,
        resolved_auth=resolved_auth,
        method="DELETE",
        path=f"/api/lists/{list_id}",
    )
    assert response.status_code in {204, 404}, response.json_body or response.body_text


def _wait_for_profile_id(
    binding: retl.DestinationBinding,
    *,
    resolved_auth: object,
    email: str,
) -> str:
    last_response: HttpResponse | None = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        response = _send_klaviyo_request(
            binding,
            resolved_auth=resolved_auth,
            method="GET",
            path="/api/profiles",
            query={
                "filter": f'equals(email,"{email}")',
                "fields[profile]": "email",
                "page[size]": "1",
            },
        )
        last_response = response
        if response.status_code == 200:
            data = response.json_body.get("data")
            if isinstance(data, list) and data:
                profile = data[0]
                if isinstance(profile, dict):
                    profile_id = profile.get("id")
                    if isinstance(profile_id, str) and profile_id:
                        return profile_id
        time.sleep(2)
    detail = last_response.json_body if last_response is not None else None
    raise AssertionError(f"Klaviyo profile for `{email}` was not readable: {detail}")


def _send_klaviyo_request(
    binding: retl.DestinationBinding,
    *,
    resolved_auth: object,
    method: str,
    path: str,
    query: dict[str, str] | None = None,
    json_body: object | None = None,
) -> HttpResponse:
    config = klaviyo_config(binding)
    transport = transport_from_config(binding.config)
    assert transport is not None
    auth_headers = getattr(resolved_auth, "headers", {})
    assert isinstance(auth_headers, Mapping)
    return transport.send(
        HttpRequest(
            method=method,
            url=join_url(config, path),
            query=query or {},
            headers={
                "accept": "application/vnd.api+json",
                "content-type": "application/vnd.api+json",
                "revision": config.api_revision,
                **dict(auth_headers),
            },
            json_body=cast(JSONValue | None, json_body),
        )
    )


def _profile_upsert_page(profile_key: str) -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": profile_key,
                "state_key": {"profile_key": profile_key},
                "identifiers_json": [
                    {
                        "type": "email",
                        "value": SANDBOX_PROFILE_EMAIL,
                    },
                    {
                        "type": "external_id",
                        "value": profile_key,
                    },
                ],
                "payload_json": {
                    "first_name": "RETL",
                    "last_name": "Sandbox",
                    "retl_sandbox": True,
                    "retl_sandbox_run_id": profile_key,
                    "retl_sandbox_submitted_at": datetime.now(UTC)
                    .isoformat()
                    .replace("+00:00", "Z"),
                },
            }
        ]
    ).to_batches()[0]


def _list_membership_import_page() -> pa.RecordBatch:
    profile_key = f"retl-klaviyo-list-sandbox-{uuid4().hex}"
    return pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": profile_key,
                "target": "sample_list",
                "state_key": {"profile_key": profile_key},
                "identifiers_json": [
                    {
                        "type": "email",
                        "value": SANDBOX_PROFILE_EMAIL,
                    },
                ],
                "payload_json": {
                    "first_name": "RETL",
                    "last_name": "List Sandbox",
                    "retl_sandbox": True,
                    "retl_sandbox_run_id": profile_key,
                },
            }
        ]
    ).to_batches()[0]


def _list_membership_profile_id_page(profile_id: str) -> pa.RecordBatch:
    rows = [
        {
            "operation": operation,
            "record_identity": f"{profile_id}-{operation}",
            "target": "sample_list",
            "state_key": {"profile_key": profile_id},
            "identifiers_json": [
                {
                    "type": "klaviyo_profile_id",
                    "value": profile_id,
                }
            ],
            "payload_json": {},
        }
        for operation in ("upsert", "remove")
    ]
    return pa.Table.from_pylist(rows).to_batches()[0]
