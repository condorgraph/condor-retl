from __future__ import annotations

from retl.auth import bearer_token, custom
from retl.destinations.registry import DestinationConnector, declarative_connector
from retl.destinations.surfaces import DestinationSurface, IdentifierRequirement
from retl_google_ads_data_manager.common import DATA_MANAGER_API_VERSION, service_account_auth
from retl_google_ads_data_manager.hooks import (
    google_ads_data_manager_managed_target_client,
    plan_google_ads_data_manager_requests,
    submit_google_ads_data_manager_destination,
)

GOOGLE_ADS_DATA_MANAGER_CONNECTOR_REF = "retl/google-ads-data-manager"
CUSTOMER_MATCH_SURFACE = "customer_match"
CUSTOMER_MATCH_CONTACT_ID_SURFACE = "customer_match_contact_id"
EVENTS_SURFACE = "events"


def google_ads_data_manager_surfaces() -> tuple[DestinationSurface, ...]:
    return (
        DestinationSurface(
            name=CUSTOMER_MATCH_SURFACE,
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="required",
            accepted_identifier_types=(
                "email",
                "phone_e164",
                "address",
                "mobile_advertising_id",
                "external_id",
            ),
            identifier_requirements=(
                IdentifierRequirement(
                    match="any_of",
                    identifier_types=(
                        "email",
                        "phone_e164",
                        "address",
                        "mobile_advertising_id",
                        "external_id",
                    ),
                ),
            ),
            required_key_fields=("customer_id",),
            delivery_outcome="accepted",
            execution_mode="asynchronous",
            request_template={
                "method": "POST",
                "path": f"/{DATA_MANAGER_API_VERSION}/audienceMembers:ingest",
            },
        ),
        DestinationSurface(
            name=CUSTOMER_MATCH_CONTACT_ID_SURFACE,
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="required",
            supports_managed_targets=True,
            accepted_identifier_types=("email", "phone_e164", "address"),
            identifier_requirements=(
                IdentifierRequirement(
                    match="any_of",
                    identifier_types=("email", "phone_e164", "address"),
                ),
            ),
            required_key_fields=("customer_id",),
            delivery_outcome="accepted",
            execution_mode="asynchronous",
            request_template={
                "method": "POST",
                "path": f"/{DATA_MANAGER_API_VERSION}/audienceMembers:ingest",
            },
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
            required_payload_fields=("event_name",),
            delivery_outcome="accepted",
            execution_mode="asynchronous",
            request_template={
                "method": "POST",
                "path": f"/{DATA_MANAGER_API_VERSION}/events:ingest",
            },
        ),
    )


def google_ads_data_manager_connector() -> DestinationConnector:
    return declarative_connector(
        ref=GOOGLE_ADS_DATA_MANAGER_CONNECTOR_REF,
        display_name="Google Ads Data Manager",
        surfaces=google_ads_data_manager_surfaces(),
        auth_modes=(
            bearer_token(name="access_token", field="access_token"),
            custom(
                name="service_account",
                required_fields=("project_id", "client_email", "private_key"),
                optional_fields=("private_key_id", "token_uri"),
                hook=service_account_auth,
            ),
        ),
        config_namespace_fields=(
            "operating_account_id",
            "operating_account_type",
            "login_account_id",
            "login_account_type",
            "linked_account_id",
            "linked_account_type",
            "event_destination_id",
            "encoding",
            "customer_match_terms_accepted",
            "ad_user_data_consent",
            "ad_personalization_consent",
            "request_status_poll_interval_seconds",
            "request_status_poll_timeout_seconds",
        ),
        batch_planning_hook=plan_google_ads_data_manager_requests,
        submission_hook=submit_google_ads_data_manager_destination,
        managed_target_client_hook=google_ads_data_manager_managed_target_client,
    )


connector = google_ads_data_manager_connector()

__all__ = [
    "CUSTOMER_MATCH_CONTACT_ID_SURFACE",
    "CUSTOMER_MATCH_SURFACE",
    "EVENTS_SURFACE",
    "GOOGLE_ADS_DATA_MANAGER_CONNECTOR_REF",
    "connector",
    "google_ads_data_manager_connector",
    "google_ads_data_manager_surfaces",
]
