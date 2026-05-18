from __future__ import annotations

from collections.abc import Mapping

from retl.auth import none
from retl.declarations import JSONValue
from retl.destinations.acknowledgements import (
    DestinationReceipt,
    DestinationSubmissionEvidence,
    PreAcceptanceFailureCategory,
    RemoteHandle,
)
from retl.destinations.registry import DestinationConnector, declarative_connector
from retl.destinations.request_batch import (
    RequestBatchingPolicy,
    RequestBatchPlan,
    plan_request_batches,
)
from retl.destinations.surfaces import DestinationSurface, IdentifierRequirement


def mock_surfaces() -> tuple[DestinationSurface, ...]:
    return (
        DestinationSurface(
            name="profile_properties",
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="unsupported",
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
            required_payload_fields=("plan",),
        ),
        DestinationSurface(
            name="list_membership",
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="required",
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
        ),
        DestinationSurface(
            name="upsert_only_profile",
            declaration_family="state",
            supported_operations=("upsert",),
            target_mode="unsupported",
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
        ),
        DestinationSurface(
            name="managed_list_membership",
            declaration_family="state",
            supported_operations=("upsert", "remove"),
            target_mode="required",
            supports_managed_targets=True,
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
            required_payload_fields=("order_total",),
        ),
        DestinationSurface(
            name="accepted_event_import",
            declaration_family="event",
            supported_operations=("import",),
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="all_of", identifier_types=("email",)),
            ),
            delivery_outcome="accepted",
            execution_mode="asynchronous",
        ),
    )


def mock_connector() -> DestinationConnector:
    return declarative_connector(
        ref="retl/mock",
        display_name="RETL Mock",
        visibility="internal",
        surfaces=mock_surfaces(),
        auth_modes=(none(),),
        batch_planning_hook=plan_mock_requests,
    )


def plan_mock_requests(
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
        request_template={"method": "POST", "path": f"/mock/{surface.name}/batches"},
        batching_policy=RequestBatchingPolicy(),
        family="event_imports" if getattr(reconciled, "import_pages", None) is not None else None,
    )


def submit_mock_destination(
    *,
    surface: DestinationSurface,
    delivery_outcome: str,
    attempted_count: int,
    config: Mapping[str, JSONValue],
    selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
) -> DestinationSubmissionEvidence:
    request_batch_count = _selected_request_batch_count(
        selected_request_plans=selected_request_plans,
        destination_name="Mock",
    )
    outcome = str(config.get("submission_outcome", "") or "")
    handle_value = str(config.get("remote_handle", "") or "")

    if outcome in {
        "transport_failure",
        "auth_failure",
        "schema_failure",
        "rate_limit_failure",
        "submission_failure",
    }:
        category = outcome.removesuffix("_failure")
        if category == "rate_limit":
            failure_category: PreAcceptanceFailureCategory = "rate_limit"
        elif category in {"transport", "auth", "schema", "submission"}:
            failure_category = category  # type: ignore[assignment]
        else:  # pragma: no cover - protected by the outcome set above.
            failure_category = "submission"
        return DestinationSubmissionEvidence(
            status="pre_acceptance_failure",
            attempted_count=attempted_count,
            pre_acceptance_failure_count=attempted_count,
            pre_acceptance_failure_category=failure_category,
            request_batch_count=request_batch_count,
            summary=f"Mock destination produced a pre-acceptance {failure_category} failure.",
        )
    if outcome == "retryable_failure":
        return DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=attempted_count,
            retryable_failure_count=attempted_count,
            request_batch_count=request_batch_count,
            summary="Mock destination produced retryable failure evidence.",
        )
    if outcome == "terminal_record_failure":
        return DestinationSubmissionEvidence(
            status="terminal_record_failure",
            attempted_count=attempted_count,
            terminal_record_failure_count=attempted_count,
            request_batch_count=request_batch_count,
            summary="Mock destination produced terminal record failure evidence.",
        )

    if outcome == "accepted" or (not outcome and delivery_outcome == "accepted"):
        handle = RemoteHandle(kind="mock_import", value=handle_value or "mock_import_accepted")
        return DestinationSubmissionEvidence(
            status="accepted",
            attempted_count=attempted_count,
            accepted_count=attempted_count,
            request_batch_count=request_batch_count,
            receipts=(
                DestinationReceipt(status="accepted", count=attempted_count, remote_handle=handle),
            ),
            remote_handles=(handle,),
            summary="Mock destination accepted submitted work.",
        )

    handle = RemoteHandle(kind="mock_receipt", value=handle_value or "mock_receipt_confirmed")
    return DestinationSubmissionEvidence(
        status="confirmed",
        attempted_count=attempted_count,
        confirmed_count=attempted_count,
        request_batch_count=request_batch_count,
        receipts=(
            DestinationReceipt(status="confirmed", count=attempted_count, remote_handle=handle),
        ),
        remote_handles=(handle,),
        summary="Mock destination confirmed submitted work.",
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


__all__ = ["mock_connector", "mock_surfaces", "plan_mock_requests", "submit_mock_destination"]
