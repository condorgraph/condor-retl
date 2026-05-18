from __future__ import annotations

from collections.abc import Mapping

from retl.auth import none
from retl.declarations import JSONValue
from retl.destinations.acknowledgements import (
    DestinationReceipt,
    DestinationSubmissionEvidence,
    RemoteHandle,
)
from retl.destinations.registry import DestinationConnector, declarative_connector
from retl.destinations.request_batch import (
    RequestBatchingPolicy,
    RequestBatchPlan,
    plan_request_batches,
)
from retl.destinations.surfaces import DestinationSurface, IdentifierRequirement


def reference_surfaces() -> tuple[DestinationSurface, ...]:
    return (
        DestinationSurface(
            name="user_profile",
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="unsupported",
            accepted_identifier_types=("email",),
        ),
        DestinationSurface(
            name="audience_membership",
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="required",
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
        ),
        DestinationSurface(
            name="subscription_group_membership",
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="required",
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
        ),
        DestinationSurface(
            name="profile_properties",
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="unsupported",
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
        ),
        DestinationSurface(
            name="purchase_event",
            declaration_family="event",
            supported_operations=("import",),
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
        ),
        DestinationSurface(
            name="event_import",
            declaration_family="event",
            supported_operations=("import",),
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
        ),
    )


def reference_connector() -> DestinationConnector:
    return declarative_connector(
        ref="retl/reference",
        display_name="RETL Reference",
        visibility="internal",
        surfaces=reference_surfaces(),
        auth_modes=(none(),),
        batch_planning_hook=plan_reference_requests,
    )


def plan_reference_requests(
    *,
    binding: object,
    surface: DestinationSurface,
    reconciled: object,
) -> object:
    _ = binding
    work = getattr(reconciled, "operation_pages", None) or getattr(reconciled, "import_pages", None)
    return plan_request_batches(
        sync_name=str(getattr(reconciled, "sync_name", "sync")),
        surface_name=surface.name,
        work=work,
        request_template={"method": "POST", "path": f"/reference/{surface.name}/batches"},
        batching_policy=RequestBatchingPolicy(),
        family="event_imports" if getattr(reconciled, "import_pages", None) is not None else None,
    )


def submit_reference_destination(
    *,
    surface: DestinationSurface,
    delivery_outcome: str,
    attempted_count: int,
    config: Mapping[str, JSONValue],
    selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
) -> DestinationSubmissionEvidence:
    _ = surface, delivery_outcome
    request_batch_count = _selected_request_batch_count(
        selected_request_plans=selected_request_plans,
        destination_name="Reference",
    )
    handle_value = str(config.get("remote_handle", "") or "reference_receipt_confirmed")
    handle = RemoteHandle(kind="reference_receipt", value=handle_value)
    return DestinationSubmissionEvidence(
        status="confirmed",
        attempted_count=attempted_count,
        confirmed_count=attempted_count,
        request_batch_count=request_batch_count,
        receipts=(
            DestinationReceipt(status="confirmed", count=attempted_count, remote_handle=handle),
        ),
        remote_handles=(handle,),
        summary="Reference destination confirmed submitted work.",
    )


def _selected_request_batch_count(
    *,
    selected_request_plans: tuple[RequestBatchPlan, ...] | None,
    destination_name: str,
) -> int:
    if selected_request_plans is None:
        return 0
    if len(selected_request_plans) > 1:
        raise ValueError(
            f"{destination_name} submission hooks require exactly one selected request batch."
        )
    return len(selected_request_plans)


__all__ = [
    "plan_reference_requests",
    "reference_connector",
    "reference_surfaces",
    "submit_reference_destination",
]
