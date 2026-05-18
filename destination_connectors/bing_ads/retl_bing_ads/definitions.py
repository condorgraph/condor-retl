from __future__ import annotations

from retl.auth import custom
from retl.destinations.registry import DestinationConnector, declarative_connector
from retl.destinations.surfaces import DestinationSurface, IdentifierRequirement
from retl_bing_ads.common import BING_ADS_API_VERSION, microsoft_advertising_auth
from retl_bing_ads.hooks import (
    bing_ads_managed_target_client,
    plan_bing_ads_requests,
    submit_bing_ads_destination,
)

BING_ADS_CONNECTOR_REF = "retl/bing-ads"
CUSTOMER_LISTS_SURFACE = "customer_lists"


def bing_ads_surfaces() -> tuple[DestinationSurface, ...]:
    return (
        DestinationSurface(
            name=CUSTOMER_LISTS_SURFACE,
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
            request_template={
                "method": "POST",
                "path": f"/CampaignManagement/{BING_ADS_API_VERSION}/CustomerListUserData/Apply",
            },
        ),
    )


def bing_ads_connector() -> DestinationConnector:
    return declarative_connector(
        ref=BING_ADS_CONNECTOR_REF,
        display_name="Microsoft Advertising",
        surfaces=bing_ads_surfaces(),
        auth_modes=(
            custom(
                name="microsoft_advertising",
                required_fields=("access_token", "developer_token"),
                hook=microsoft_advertising_auth,
            ),
        ),
        config_namespace_fields=(
            "customer_account_id",
            "customer_id",
            "api_version",
            "target_scope",
            "membership_duration",
            "accept_customer_match_terms",
        ),
        batch_planning_hook=plan_bing_ads_requests,
        submission_hook=submit_bing_ads_destination,
        managed_target_client_hook=bing_ads_managed_target_client,
    )


connector = bing_ads_connector()

__all__ = [
    "BING_ADS_CONNECTOR_REF",
    "CUSTOMER_LISTS_SURFACE",
    "bing_ads_connector",
    "bing_ads_surfaces",
    "connector",
]
