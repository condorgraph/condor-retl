from __future__ import annotations

import os
from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pytest
from retl_reference_http.definitions import STATE_SURFACE, reference_http_connector

from retl.declarations import DestinationBinding
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.http import HttpRequest, HttpResponse
from retl.state_runtime.reconcile import StateReconcileEvidence

pytestmark = pytest.mark.live_sandbox


@dataclass
class SandboxTemplateTransport:
    requests: list[HttpRequest]

    def send(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        return HttpResponse(
            status_code=200,
            json_body={"request_id": "reference_http_sandbox_template"},
        )


def test_reference_http_sandbox_template_uses_opt_in_guard_and_destination_env() -> None:
    if os.environ.get("RETL_RUN_LIVE_SANDBOX") != "1":
        pytest.skip("Set RETL_RUN_LIVE_SANDBOX=1 to exercise sandbox-template tests.")

    connector = reference_http_connector()
    transport = SandboxTemplateTransport(requests=[])
    binding = DestinationBinding(
        binding_name="reference_http_sandbox_template",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "base_url": os.environ.get(
                "DESTINATIONS__REFERENCE_HTTP__BASE_URL",
                "https://reference-http.example.test",
            ),
            "request_batch_max_rows": int(
                os.environ.get("DESTINATIONS__REFERENCE_HTTP__REQUEST_BATCH_MAX_ROWS", "10")
            ),
            "transport": transport,  # type: ignore[dict-item]
        },
    )
    page = pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "sandbox-template-customer-1",
                "state_key": {"customer_id": "sandbox-template-1"},
                "identifiers": [{"type": "email", "value": "sandbox-template@example.test"}],
                "payload": {"status": "active"},
            },
        ]
    ).to_batches()[0]
    hook = connector.submission_hook
    assert hook is not None

    evidence = cast(
        DestinationSubmissionEvidence,
        hook(
            binding=binding,
            surface=connector.surface(STATE_SURFACE),
            delivery_outcome="succeeded",
            attempted_count=1,
            dry_run=False,
            resolved_auth=object(),
            reconciled=cast(
                StateReconcileEvidence,
                SimpleNamespace(
                    sync_name="reference_http_sandbox_template",
                    operation_pages=(page,),
                ),
            ),
        ),
    )

    assert evidence.status == "confirmed"
    assert evidence.confirmed_count == 1
    assert len(transport.requests) == 1
    assert transport.requests[0].url.endswith("/reference-http/state-records")
