from __future__ import annotations

from retl.auth import none
from retl.destinations.registry import DestinationConnector, declarative_connector
from retl.destinations.surfaces import DestinationSurface
from retl_file.hooks import plan_file_requests, submit_file_destination

FILE_CONNECTOR_REF = "retl/file"
STATE_SURFACE = "csv_state_operations"
EVENT_SURFACE = "csv_event_imports"
FILE_ACCEPTED_IDENTIFIER_TYPES = (
    "email",
    "phone_e164",
    "mobile_advertising_id",
    "external_id",
    "address",
)


def file_surfaces() -> tuple[DestinationSurface, ...]:
    return (
        DestinationSurface(
            name=STATE_SURFACE,
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="unsupported",
            accepted_identifier_types=FILE_ACCEPTED_IDENTIFIER_TYPES,
            delivery_outcome="succeeded",
            request_template={
                "method": "{{http_method}}",
                "path": "/file/{{surface}}/{{operation}}",
            },
        ),
        DestinationSurface(
            name=EVENT_SURFACE,
            declaration_family="event",
            supported_operations=("import",),
            target_mode="unsupported",
            accepted_identifier_types=FILE_ACCEPTED_IDENTIFIER_TYPES,
            delivery_outcome="succeeded",
            request_template={
                "method": "POST",
                "path": "/file/{{surface}}/imports",
            },
        ),
    )


def file_connector() -> DestinationConnector:
    return declarative_connector(
        ref=FILE_CONNECTOR_REF,
        display_name="File Output",
        surfaces=file_surfaces(),
        auth_modes=(none(),),
        config_namespace_fields=(
            "output_dir",
            "file_batch_max_rows",
            "create_parent_dirs",
        ),
        batch_planning_hook=plan_file_requests,
        submission_hook=submit_file_destination,
    )


connector = file_connector()

__all__ = [
    "EVENT_SURFACE",
    "FILE_ACCEPTED_IDENTIFIER_TYPES",
    "FILE_CONNECTOR_REF",
    "STATE_SURFACE",
    "connector",
    "file_connector",
    "file_surfaces",
]
