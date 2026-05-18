from __future__ import annotations

from retl.auth import api_key
from retl.destinations.registry import DestinationConnector, declarative_connector
from retl.destinations.surfaces import DestinationSurface, IdentifierRequirement
from retl_tiktok_ads.common import TIKTOK_ADS_API_VERSION
from retl_tiktok_ads.hooks import (
    plan_tiktok_ads_requests,
    submit_tiktok_ads_destination,
    tiktok_ads_managed_target_client,
)

TIKTOK_ADS_CONNECTOR_REF = "retl/tiktok-ads"
CUSTOM_AUDIENCES_SURFACE = "custom_audiences"


def tiktok_ads_surfaces() -> tuple[DestinationSurface, ...]:
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
            delivery_outcome="accepted",
            request_template={
                "method": "POST",
                "path": f"/open_api/{TIKTOK_ADS_API_VERSION}/dmp/custom_audience/update/",
            },
        ),
    )


def tiktok_ads_connector() -> DestinationConnector:
    return declarative_connector(
        ref=TIKTOK_ADS_CONNECTOR_REF,
        display_name="TikTok Ads",
        surfaces=tiktok_ads_surfaces(),
        auth_modes=(
            api_key(
                name="access_token",
                field="access_token",
                location="header",
                key="Access-Token",
            ),
        ),
        config_namespace_fields=(
            "advertiser_id",
            "api_version",
            "mobile_advertising_id_type",
        ),
        batch_planning_hook=plan_tiktok_ads_requests,
        submission_hook=submit_tiktok_ads_destination,
        managed_target_client_hook=tiktok_ads_managed_target_client,
    )


connector = tiktok_ads_connector()

__all__ = [
    "CUSTOM_AUDIENCES_SURFACE",
    "TIKTOK_ADS_CONNECTOR_REF",
    "connector",
    "tiktok_ads_connector",
    "tiktok_ads_surfaces",
]
