from __future__ import annotations

from retl.auth import bearer_token
from retl.destinations.registry import DestinationConnector, declarative_connector
from retl.destinations.surfaces import DestinationSurface, IdentifierRequirement
from retl_meta.hooks import meta_managed_target_client, plan_meta_requests, submit_meta_destination

META_CONNECTOR_REF = "retl/meta"
CUSTOM_AUDIENCES_SURFACE = "custom_audiences"
EVENTS_SURFACE = "events"


def meta_surfaces() -> tuple[DestinationSurface, ...]:
    return (
        DestinationSurface(
            name=CUSTOM_AUDIENCES_SURFACE,
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="required",
            supports_managed_targets=True,
            accepted_identifier_types=("email", "phone_e164", "mobile_advertising_id"),
            identifier_requirements=(
                IdentifierRequirement(
                    match="any_of",
                    identifier_types=("email", "phone_e164", "mobile_advertising_id"),
                ),
            ),
            required_key_fields=("customer_id",),
            delivery_outcome="succeeded",
            request_template={"method": "POST", "path": "/{{ config.api_version }}/users"},
        ),
        DestinationSurface(
            name=EVENTS_SURFACE,
            declaration_family="event",
            supported_operations=("import",),
            target_mode="unsupported",
            accepted_identifier_types=("email", "phone_e164", "external_id"),
            identifier_requirements=(
                IdentifierRequirement(
                    match="any_of",
                    identifier_types=("email", "phone_e164", "external_id"),
                ),
            ),
            required_key_fields=("event_id",),
            required_payload_fields=("value", "currency"),
            delivery_outcome="succeeded",
            request_template={
                "method": "POST",
                "path": "/{{ config.api_version }}/{{ target }}/events",
            },
        ),
    )


def meta_connector() -> DestinationConnector:
    return declarative_connector(
        ref=META_CONNECTOR_REF,
        display_name="Meta Ads",
        surfaces=meta_surfaces(),
        auth_modes=(bearer_token(name="access_token", field="access_token"),),
        config_namespace_fields=(
            "ad_account_id",
            "api_version",
            "custom_audience_customer_file_source",
        ),
        batch_planning_hook=plan_meta_requests,
        submission_hook=submit_meta_destination,
        managed_target_client_hook=meta_managed_target_client,
    )


connector = meta_connector()

__all__ = [
    "CUSTOM_AUDIENCES_SURFACE",
    "EVENTS_SURFACE",
    "META_CONNECTOR_REF",
    "connector",
    "meta_connector",
    "meta_surfaces",
]
