from __future__ import annotations

from retl.auth import none
from retl.destinations.registry import DestinationConnector, declarative_connector
from retl.destinations.surfaces import DestinationSurface, IdentifierRequirement
from retl_reference_http.hooks import (
    plan_reference_http_requests,
    submit_reference_http_destination,
)

STATE_SURFACE = "state_records"
EVENT_SURFACE = "event_imports"


def reference_http_surfaces() -> tuple[DestinationSurface, ...]:
    return (
        DestinationSurface(
            name=STATE_SURFACE,
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="unsupported",
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
            required_key_fields=("customer_id",),
            required_payload_fields=("status",),
            delivery_outcome="succeeded",
            request_template={
                "method": "POST",
                "path": "/reference-http/state-records",
                "headers": {"X-RETL-Surface": STATE_SURFACE},
            },
        ),
        DestinationSurface(
            name=EVENT_SURFACE,
            declaration_family="event",
            supported_operations=("import",),
            target_mode="unsupported",
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
            required_key_fields=("event_id",),
            required_payload_fields=("event_name",),
            delivery_outcome="succeeded",
            request_template={
                "method": "POST",
                "path": "/reference-http/events",
                "headers": {"X-RETL-Surface": EVENT_SURFACE},
            },
        ),
    )


def reference_http_connector() -> DestinationConnector:
    return declarative_connector(
        ref="retl/reference-http",
        display_name="RETL Reference HTTP",
        surfaces=reference_http_surfaces(),
        auth_modes=(none(),),
        config_namespace_fields=("base_url", "request_batch_max_rows"),
        batch_planning_hook=plan_reference_http_requests,
        submission_hook=submit_reference_http_destination,
    )


connector = reference_http_connector()

__all__ = [
    "EVENT_SURFACE",
    "STATE_SURFACE",
    "connector",
    "reference_http_connector",
    "reference_http_surfaces",
]
