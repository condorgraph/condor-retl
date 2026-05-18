from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

from retl.declarations import DestinationBinding
from retl.destinations.acknowledgements import DestinationReceipt, DestinationSubmissionEvidence
from retl.destinations.http import (
    HttpRequest,
    HttpResponse,
)
from retl.destinations.receipts import (
    RemoteHandlePolicy,
    ResponseClassification,
    ResponseClassificationPolicy,
    classify_response,
)
from retl.destinations.request_batch import (
    DryRunSubmissionPlan,
    RequestBatchingPolicy,
    RequestBatchPlan,
    plan_request_batches,
)
from retl.destinations.surfaces import DestinationSurface
from retl.events.reconcile import EventReconcileEvidence
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl_reference_http.common import (
    join_url,
    public_config,
    reference_http_config,
    transport_from_config,
)


def submit_reference_http_destination(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    delivery_outcome: str,
    attempted_count: int,
    dry_run: bool,
    resolved_auth: object,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
) -> DestinationSubmissionEvidence:
    request_plans = _submission_request_plans(
        binding=binding,
        surface=surface,
        reconciled=reconciled,
        selected_request_plans=selected_request_plans,
    )
    if dry_run:
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=True,
            request_batch_count=request_plans.request_count,
            summary=(
                "Reference HTTP dry run planned "
                f"{request_plans.request_count} request batch(es) for "
                f"{request_plans.record_count} record(s)."
            ),
        )
    request_plan = _single_submission_request_plan(
        plans=request_plans.plans,
        connector_name="Reference HTTP",
    )
    transport = transport_from_config(binding.config)
    if transport is None:
        return DestinationSubmissionEvidence(
            status="pre_acceptance_failure",
            attempted_count=attempted_count,
            dry_run=False,
            pre_acceptance_failure_count=attempted_count,
            pre_acceptance_failure_category="transport",
            request_batch_count=request_plans.request_count,
            summary=(
                "Reference HTTP non-dry-run submission requires injected transport; "
                "no request was sent."
            ),
        )
    if request_plan is None:
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=False,
            request_batch_count=0,
            summary="Reference HTTP submission had no request batch to execute.",
        )

    request = _http_request(
        binding=binding,
        request_plan=request_plan,
        resolved_auth=resolved_auth,
    )
    try:
        response = transport.send(request)
    except Exception as exc:
        return _transport_failure_evidence(
            attempted_count=attempted_count,
            failed_count=request_plan.row_count,
            request_batch_count=request_plans.request_count,
            error=exc,
        )
    classification = classify_reference_http_response(response)
    return _aggregate_submission_evidence(
        [(classification, request_plan.row_count)],
        delivery_outcome=delivery_outcome,
        attempted_count=attempted_count,
        request_batch_count=request_plans.request_count,
    )


def plan_reference_http_requests(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
) -> DryRunSubmissionPlan:
    work = getattr(reconciled, "operation_pages", None) or getattr(reconciled, "import_pages", None)
    if work is None:
        return DryRunSubmissionPlan(
            dry_run=True,
            plans=(),
            record_count=0,
            request_count=0,
            notes=("Reference HTTP work is deferred until reconcile produces pages.",),
        )
    return plan_request_batches(
        sync_name=reconciled.sync_name,
        surface_name=surface.name,
        work=work,
        request_template=surface.request_template,
        batching_policy=RequestBatchingPolicy(
            max_rows=reference_http_config(binding).request_batch_max_rows
        ),
        public_config=public_config(binding.config),
        dry_run=True,
        family="state_operations" if surface.declaration_family == "state" else "event_imports",
    )


def classify_reference_http_response(response: HttpResponse) -> ResponseClassification:
    return classify_response(response, policy=REFERENCE_HTTP_RESPONSE_POLICY)


def reference_http_fixture_response(outcome: str) -> HttpResponse:
    if outcome == "accepted":
        return HttpResponse(status_code=202, json_body={"request_id": "ref_http_accepted"})
    if outcome == "retryable_failure":
        return HttpResponse(
            status_code=503,
            headers={"retry-after": "30"},
            json_body={"error": {"message": "temporary reference HTTP failure"}},
        )
    if outcome == "terminal_record_failure":
        return HttpResponse(
            status_code=409,
            json_body={"error": {"message": "terminal reference HTTP failure"}},
        )
    if outcome == "pre_acceptance_failure":
        return HttpResponse(
            status_code=401,
            json_body={"error": {"message": "authorization: Bearer secret-token"}},
        )
    return HttpResponse(status_code=200, json_body={"request_id": "ref_http_confirmed"})


def _aggregate_submission_evidence(
    classified_batches: list[tuple[ResponseClassification, int]],
    *,
    delivery_outcome: str,
    attempted_count: int,
    request_batch_count: int,
) -> DestinationSubmissionEvidence:
    confirmed_count = _count_outcome(classified_batches, "confirmed")
    accepted_count = _count_outcome(classified_batches, "accepted")
    retryable_count = _count_outcome(classified_batches, "retryable_failure")
    terminal_count = _count_outcome(classified_batches, "terminal_record_failure")
    pre_acceptance_count = _count_outcome(classified_batches, "pre_acceptance_failure")
    receipts = tuple(
        DestinationReceipt(
            status=cast(Literal["confirmed", "accepted"], classification.outcome),
            count=row_count,
            remote_handle=classification.remote_handle,
        )
        for classification, row_count in classified_batches
        if classification.outcome in {"confirmed", "accepted"}
    )
    handles = tuple(
        classification.remote_handle
        for classification, _ in classified_batches
        if classification.remote_handle is not None
    )
    if pre_acceptance_count:
        first_failure = _first_outcome(classified_batches, "pre_acceptance_failure")
        return DestinationSubmissionEvidence(
            status="pre_acceptance_failure",
            attempted_count=attempted_count,
            pre_acceptance_failure_count=pre_acceptance_count,
            pre_acceptance_failure_category=first_failure.pre_acceptance_failure_category,
            confirmed_count=confirmed_count,
            accepted_count=accepted_count,
            receipts=receipts,
            remote_handles=handles,
            request_batch_count=request_batch_count,
            http_status=first_failure.status_code,
            partner_error_code=first_failure.partner_error_code,
            partner_error_subcode=first_failure.partner_error_subcode,
            summary=(
                first_failure.partner_message or "Reference HTTP request failed before acceptance."
            ),
        )
    if retryable_count:
        first_failure = _first_outcome(classified_batches, "retryable_failure")
        return DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=attempted_count,
            confirmed_count=confirmed_count,
            accepted_count=accepted_count,
            retryable_failure_count=retryable_count,
            receipts=receipts,
            remote_handles=handles,
            request_batch_count=request_batch_count,
            http_status=first_failure.status_code,
            partner_error_code=first_failure.partner_error_code,
            partner_error_subcode=first_failure.partner_error_subcode,
            summary=first_failure.partner_message or "Reference HTTP request failed retryably.",
        )
    if terminal_count:
        first_failure = _first_outcome(classified_batches, "terminal_record_failure")
        status: Literal["confirmed", "accepted", "terminal_record_failure"] = (
            _successful_status(accepted_count=accepted_count)
            if confirmed_count or accepted_count
            else "terminal_record_failure"
        )
        return DestinationSubmissionEvidence(
            status=status,
            attempted_count=attempted_count,
            confirmed_count=confirmed_count,
            accepted_count=accepted_count,
            terminal_record_failure_count=terminal_count,
            receipts=receipts,
            remote_handles=handles,
            request_batch_count=request_batch_count,
            http_status=first_failure.status_code,
            partner_error_code=first_failure.partner_error_code,
            partner_error_subcode=first_failure.partner_error_subcode,
            summary=first_failure.partner_message or "Reference HTTP request failed terminally.",
        )
    status = _successful_status(accepted_count=accepted_count)
    if delivery_outcome == "succeeded" and accepted_count:
        summary = (
            "Reference HTTP received acceptance-only evidence for one or more request batches."
        )
    else:
        summary = "Reference HTTP request batches satisfied destination delivery evidence."
    return DestinationSubmissionEvidence(
        status=status,
        attempted_count=attempted_count,
        confirmed_count=confirmed_count,
        accepted_count=accepted_count,
        receipts=receipts,
        remote_handles=handles,
        request_batch_count=request_batch_count,
        summary=summary,
    )


def _count_outcome(
    classified_batches: list[tuple[ResponseClassification, int]],
    outcome: str,
) -> int:
    return sum(
        row_count
        for classification, row_count in classified_batches
        if classification.outcome == outcome
    )


def _first_outcome(
    classified_batches: list[tuple[ResponseClassification, int]],
    outcome: str,
) -> ResponseClassification:
    for classification, _ in classified_batches:
        if classification.outcome == outcome:
            return classification
    raise ValueError(f"Reference HTTP did not classify outcome `{outcome}`.")


def _successful_status(*, accepted_count: int) -> Literal["confirmed", "accepted"]:
    return "confirmed" if accepted_count == 0 else "accepted"


def _transport_failure_evidence(
    *,
    attempted_count: int,
    failed_count: int,
    request_batch_count: int,
    error: Exception,
) -> DestinationSubmissionEvidence:
    return DestinationSubmissionEvidence(
        status="pre_acceptance_failure",
        attempted_count=attempted_count,
        dry_run=False,
        pre_acceptance_failure_count=failed_count,
        pre_acceptance_failure_category="transport",
        request_batch_count=request_batch_count,
        summary=f"Reference HTTP transport failed before response: {type(error).__name__}.",
    )


def _http_request(
    *,
    binding: DestinationBinding,
    request_plan: RequestBatchPlan,
    resolved_auth: object,
) -> HttpRequest:
    request = request_plan.request
    config = reference_http_config(binding)
    auth_headers = getattr(resolved_auth, "headers", {})
    if not isinstance(auth_headers, Mapping):
        auth_headers = {}
    return HttpRequest(
        method=request.method,
        url=join_url(config, request.path),
        query=request.query,
        headers={**dict(request.headers), **cast(Mapping[str, str], auth_headers)},
        json_body=request.json_body,
    )


REFERENCE_HTTP_RESPONSE_POLICY = ResponseClassificationPolicy(
    error_code_prefix="reference_http",
    terminal_statuses=frozenset({409}),
    remote_handle=RemoteHandlePolicy(kind="reference_http_request", value_path=("request_id",)),
)


@dataclass(frozen=True)
class _SubmissionRequestPlans:
    plans: tuple[RequestBatchPlan, ...]
    request_count: int
    record_count: int


def _submission_request_plans(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    selected_request_plans: tuple[RequestBatchPlan, ...] | None,
) -> _SubmissionRequestPlans:
    if selected_request_plans is not None:
        return _SubmissionRequestPlans(
            plans=selected_request_plans,
            request_count=len(selected_request_plans),
            record_count=sum(request.row_count for request in selected_request_plans),
        )
    plan = plan_reference_http_requests(binding=binding, surface=surface, reconciled=reconciled)
    return _SubmissionRequestPlans(
        plans=plan.plans,
        request_count=plan.request_count,
        record_count=plan.record_count,
    )


def _single_submission_request_plan(
    *,
    plans: tuple[RequestBatchPlan, ...],
    connector_name: str,
) -> RequestBatchPlan | None:
    if len(plans) > 1:
        raise ValueError(
            f"{connector_name} submission hooks require exactly one selected request batch."
        )
    return plans[0] if plans else None


__all__ = [
    "REFERENCE_HTTP_RESPONSE_POLICY",
    "classify_reference_http_response",
    "plan_reference_http_requests",
    "reference_http_fixture_response",
    "submit_reference_http_destination",
]
