from __future__ import annotations

from retl.auth import api_key
from retl.destinations.registry import DestinationConnector, declarative_connector
from retl.destinations.surfaces import DestinationSurface, IdentifierRequirement
from retl_klaviyo.hooks import (
    klaviyo_managed_target_client,
    plan_klaviyo_requests,
    submit_klaviyo_destination,
)

KLAVIYO_CONNECTOR_REF = "retl/klaviyo"
PROFILES_SURFACE = "profiles"
LIST_MEMBERSHIPS_SURFACE = "list_memberships"
LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE = "list_memberships_by_profile_id"


def klaviyo_surfaces() -> tuple[DestinationSurface, ...]:
    return (
        DestinationSurface(
            name=PROFILES_SURFACE,
            declaration_family="state",
            supported_operations=("upsert",),
            target_mode="unsupported",
            supports_managed_targets=False,
            accepted_identifier_types=("email", "phone_e164", "external_id"),
            identifier_requirements=(
                IdentifierRequirement(
                    match="any_of",
                    identifier_types=("email", "phone_e164", "external_id"),
                ),
            ),
            required_key_fields=("profile_key",),
            delivery_outcome="accepted",
            request_template={
                "method": "POST",
                "path": "/api/profile-bulk-import-jobs",
            },
        ),
        DestinationSurface(
            name=LIST_MEMBERSHIPS_SURFACE,
            declaration_family="state",
            supported_operations=("upsert",),
            target_mode="required",
            supports_managed_targets=True,
            accepted_identifier_types=(
                "email",
                "phone_e164",
                "external_id",
                "klaviyo_profile_id",
            ),
            identifier_requirements=(
                IdentifierRequirement(
                    match="any_of",
                    identifier_types=(
                        "email",
                        "phone_e164",
                        "external_id",
                        "klaviyo_profile_id",
                    ),
                ),
            ),
            required_key_fields=("profile_key",),
            delivery_outcome="accepted",
            request_template={
                "method": "POST",
                "path": "/api/profile-bulk-import-jobs",
            },
        ),
        DestinationSurface(
            name=LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE,
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="required",
            supports_managed_targets=True,
            accepted_identifier_types=("klaviyo_profile_id",),
            identifier_requirements=(
                IdentifierRequirement(
                    match="any_of",
                    identifier_types=("klaviyo_profile_id",),
                ),
            ),
            required_key_fields=("profile_key",),
            delivery_outcome="succeeded",
            request_template={
                "method": "{{ http_method }}",
                "path": "/api/lists/{{ target }}/relationships/profiles",
            },
        ),
    )


def klaviyo_connector() -> DestinationConnector:
    return declarative_connector(
        ref=KLAVIYO_CONNECTOR_REF,
        display_name="Klaviyo",
        surfaces=klaviyo_surfaces(),
        auth_modes=(
            api_key(
                name="private_api_key",
                field="private_api_key",
                location="header",
                key="Authorization",
                prefix="Klaviyo-API-Key ",
            ),
        ),
        config_namespace_fields=("api_revision",),
        batch_planning_hook=plan_klaviyo_requests,
        submission_hook=submit_klaviyo_destination,
        managed_target_client_hook=klaviyo_managed_target_client,
    )


connector = klaviyo_connector()

__all__ = [
    "KLAVIYO_CONNECTOR_REF",
    "LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE",
    "LIST_MEMBERSHIPS_SURFACE",
    "PROFILES_SURFACE",
    "connector",
    "klaviyo_connector",
    "klaviyo_surfaces",
]
