from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

import pyarrow as pa

from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.acknowledgements import DestinationReceipt, DestinationSubmissionEvidence
from retl.destinations.http import HttpRequest, HttpResponse, HttpTransport
from retl.destinations.receipts import (
    RemoteHandlePolicy,
    ResponseClassification,
    ResponseClassificationPolicy,
    classify_response,
)
from retl.destinations.request_batch import (
    DestinationWorkRecord,
    DryRunSubmissionPlan,
    RequestBatchContext,
    RequestBatchingPolicy,
    RequestBatchPlan,
    plan_request_batches,
)
from retl.destinations.surfaces import DestinationSurface
from retl.destinations.targets import (
    RemoteTarget,
    TargetResolutionError,
    TargetResolutionFailure,
    registry_key,
)
from retl.errors import DeclarationValidationError
from retl.events.reconcile import EventReconcileEvidence
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl_bing_ads.common import (
    BING_ADS_API_VERSION,
    MAX_CUSTOMER_LIST_ITEMS_PER_REQUEST,
    BingAdsConfig,
    bing_ads_config,
    bing_ads_headers,
    bing_ads_partner_error_detail,
    bing_ads_partner_message,
    hashed_customer_list_item,
    join_url,
    transport_from_config,
)

CUSTOMER_LISTS_SURFACE = "customer_lists"
CUSTOMER_LIST_TYPE = "CustomerList"
CUSTOMER_LIST_ACTIONS = {
    "upsert": "Add",
    "remove": "Remove",
}
CUSTOMER_LIST_ITEM_SUBTYPES = {
    "email": "Email",
    "phone_e164": "Phone",
    "mobile_advertising_id": "MobileAdvertisingId",
}

BING_ADS_RESPONSE_POLICY = ResponseClassificationPolicy(
    error_code_prefix="bing_ads",
    partner_message=lambda response: bing_ads_partner_message(cast(HttpResponse, response)),
    partner_error_detail=lambda response: bing_ads_partner_error_detail(
        cast(HttpResponse, response)
    ),
    remote_handle=RemoteHandlePolicy(
        kind="bing_ads_tracking_id",
        value=lambda response: _header(cast(HttpResponse, response).headers, "TrackingId"),
    ),
    default_retry_after_seconds=60,
)


@dataclass(frozen=True)
class _SubmissionRequestPlans:
    plans: tuple[RequestBatchPlan, ...]
    request_count: int
    record_count: int


@dataclass(frozen=True)
class BingAdsCustomerListTargetClient:
    binding: DestinationBinding
    surface: DestinationSurface
    resolved_auth: object

    def find_target(self, logical_target: str) -> RemoteTarget | None:
        if self.surface.name != CUSTOMER_LISTS_SURFACE:
            return None
        config = bing_ads_config(self.binding)
        transport = _target_transport(self.binding)
        response = transport.send(
            HttpRequest(
                method="POST",
                url=join_url(
                    config,
                    f"/CampaignManagement/{config.api_version}/Audiences/QueryByIds",
                ),
                headers=bing_ads_headers(config=config, resolved_auth=self.resolved_auth),
                json_body={
                    "AudienceIds": None,
                    "Type": CUSTOMER_LIST_TYPE,
                    "ReturnAdditionalFields": None,
                },
            )
        )
        _raise_for_target_response(response, action="find", logical_target=logical_target)
        for audience in _audience_rows(response):
            if audience.get("Name") != logical_target:
                continue
            if audience.get("Type") != CUSTOMER_LIST_TYPE:
                raise DeclarationValidationError(
                    "Bing Ads audience "
                    f"`{logical_target}` exists with unsupported type `{audience.get('Type')}`."
                )
            remote_id = audience.get("Id")
            if remote_id is not None and str(remote_id).strip():
                return RemoteTarget(
                    remote_id=str(remote_id),
                    display_name=logical_target,
                    metadata=_audience_metadata(audience),
                )
        return None

    def create_target(self, logical_target: str, *, display_name: str) -> RemoteTarget:
        if self.surface.name != CUSTOMER_LISTS_SURFACE:
            raise DeclarationValidationError(
                "Bing Ads managed target creation is only supported for Customer Lists."
            )
        config = bing_ads_config(self.binding)
        transport = _target_transport(self.binding)
        response = transport.send(
            HttpRequest(
                method="POST",
                url=join_url(config, f"/CampaignManagement/{config.api_version}/Audiences"),
                headers=bing_ads_headers(config=config, resolved_auth=self.resolved_auth),
                json_body={
                    "Audiences": [
                        {
                            "Description": f"Managed by RETL for `{logical_target}`.",
                            "MembershipDuration": config.membership_duration,
                            "Name": display_name,
                            "ParentId": _target_parent_id(config),
                            "Scope": config.target_scope,
                            "Type": CUSTOMER_LIST_TYPE,
                        }
                    ]
                },
            )
        )
        _raise_for_target_response(response, action="create", logical_target=logical_target)
        remote_id = _created_audience_id(response)
        if remote_id is None:
            raise DeclarationValidationError("Bing Ads CustomerList creation did not return an id.")
        return RemoteTarget(
            remote_id=remote_id,
            display_name=display_name,
            metadata={
                "type": CUSTOMER_LIST_TYPE,
                "scope": config.target_scope,
                "membership_duration": config.membership_duration,
            },
        )


def bing_ads_managed_target_client(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    resolved_auth: object,
) -> BingAdsCustomerListTargetClient | None:
    if surface.name != CUSTOMER_LISTS_SURFACE:
        return None
    return BingAdsCustomerListTargetClient(
        binding=binding,
        surface=surface,
        resolved_auth=resolved_auth,
    )


def plan_bing_ads_requests(
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
            notes=("Bing Ads work is deferred until reconcile produces pages.",),
        )
    config = bing_ads_config(binding)
    return plan_request_batches(
        sync_name=reconciled.sync_name,
        surface_name=surface.name,
        work=work,
        request_template={
            "method": "POST",
            "path": f"/CampaignManagement/{BING_ADS_API_VERSION}/CustomerListUserData/Apply",
        },
        batching_policy=RequestBatchingPolicy(max_rows=MAX_CUSTOMER_LIST_ITEMS_PER_REQUEST),
        public_config={
            "api_version": config.api_version,
            "accept_customer_match_terms": config.accept_customer_match_terms,
        },
        dry_run=True,
        body_hook=_customer_list_body,
        request_item_counts=_customer_list_request_item_counts,
        family="state_operations",
        partition_key=_customer_list_partition,
        record_hook=lambda record: _with_remote_target(
            record,
            binding=binding,
            surface=surface,
        ),
    )


def submit_bing_ads_destination(
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
                f"Bing Ads dry run planned {request_plans.request_count} request batch(es) for "
                f"{request_plans.record_count} record(s)."
            ),
        )

    request_plan = _single_submission_request_plan(
        plans=request_plans.plans,
        connector_name="Bing Ads",
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
            summary="Bing Ads submission requires an HTTP transport; no request was sent.",
        )
    if request_plan is None:
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=False,
            request_batch_count=0,
            summary="Bing Ads submission had no request batch to execute.",
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
    classification = classify_bing_ads_response(response)
    return _aggregate_submission_evidence(
        [(classification, request_plan.row_count)],
        delivery_outcome=delivery_outcome,
        attempted_count=attempted_count,
        request_batch_count=request_plans.request_count,
    )


def classify_bing_ads_response(response: HttpResponse) -> ResponseClassification:
    if _partial_errors(response):
        return ResponseClassification(
            outcome="terminal_record_failure",
            status_code=response.status_code,
            error_code="bing_ads_terminal",
            partner_error_code=_partial_error_code(response),
            partner_message=bing_ads_partner_message(response),
            partner_error_detail=bing_ads_partner_error_detail(response),
        )
    return classify_response(response, policy=BING_ADS_RESPONSE_POLICY)


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
    plan = plan_bing_ads_requests(binding=binding, surface=surface, reconciled=reconciled)
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


def _http_request(
    *,
    binding: DestinationBinding,
    request_plan: RequestBatchPlan,
    resolved_auth: object,
) -> HttpRequest:
    config = bing_ads_config(binding)
    request = request_plan.request
    return HttpRequest(
        method=request.method,
        url=join_url(config, request.path),
        query=request.query,
        headers={
            **dict(request.headers),
            **dict(bing_ads_headers(config=config, resolved_auth=resolved_auth)),
        },
        json_body=request.json_body,
    )


def _target_transport(binding: DestinationBinding) -> HttpTransport:
    transport = transport_from_config(binding.config)
    if transport is None:
        raise DeclarationValidationError("Bing Ads managed targets require an HTTP transport.")
    return transport


def _raise_for_target_response(
    response: HttpResponse,
    *,
    action: str,
    logical_target: str,
) -> None:
    classification = classify_bing_ads_response(response)
    if classification.outcome in {"confirmed", "accepted"}:
        return
    summary = classification.partner_message or f"Bing Ads CustomerList {action} failed."
    raise TargetResolutionError(
        TargetResolutionFailure(
            logical_target=logical_target,
            action=f"{action}_target",
            category=_target_failure_category(classification),
            http_status=classification.status_code,
            partner_error_code=classification.partner_error_code,
            partner_error_subcode=classification.partner_error_subcode,
            partner_error_detail=classification.partner_error_detail,
            summary=summary,
        )
    )


def _target_failure_category(classification: ResponseClassification) -> str:
    if classification.pre_acceptance_failure_category is not None:
        return classification.pre_acceptance_failure_category
    if classification.outcome == "retryable_failure":
        return "rate_limit" if classification.status_code == 429 else "submission"
    if classification.outcome == "terminal_record_failure":
        return "target"
    return "submission"


def _customer_list_body(context: RequestBatchContext) -> JSONValue:
    records = context.records
    item_subtype = _single_item_subtype(records)
    if context.target is None:
        raise ValueError("Bing Ads CustomerList records require a target audience id.")
    return {
        "CustomerListUserData": {
            "AcceptCustomerMatchTerm": context.public_config["accept_customer_match_terms"],
            "ActionType": CUSTOMER_LIST_ACTIONS[cast(str, context.operation)],
            "AudienceId": context.target,
            "CustomerListItems": [
                item for record in records for item in _customer_list_items(record, item_subtype)
            ],
            "CustomerListItemSubType": item_subtype,
        }
    }


def _customer_list_request_item_counts(page: pa.RecordBatch) -> pa.Array:
    identifiers_column = _identifiers_column(page)
    if identifiers_column is None:
        return pa.array([0] * page.num_rows, type=pa.int64())
    return pa.array(
        [
            _customer_list_item_count(_identifier_items_from_value(scalar.as_py()))
            for scalar in identifiers_column
        ],
        type=pa.int64(),
    )


def _customer_list_partition(record: DestinationWorkRecord) -> object:
    subtype = _preferred_identifier_subtype(_identifier_items(record))
    return (record.target, record.operation, subtype)


def _single_item_subtype(records: tuple[DestinationWorkRecord, ...]) -> str:
    subtypes = {_preferred_identifier_subtype(_identifier_items(record)) for record in records}
    if len(subtypes) != 1:
        raise ValueError("Bing Ads request batches must contain one CustomerListItemSubType.")
    return next(iter(subtypes))


def _preferred_identifier_subtype(identifiers: tuple[Mapping[str, object], ...]) -> str:
    for identifier_type in ("email", "phone_e164", "mobile_advertising_id"):
        if _values_for_identifier_type(identifiers, identifier_type):
            return CUSTOMER_LIST_ITEM_SUBTYPES[identifier_type]
    raise ValueError("Bing Ads CustomerList records require at least one accepted identifier.")


def _customer_list_items(
    record: DestinationWorkRecord,
    item_subtype: str,
) -> list[str]:
    identifier_type = _identifier_type_for_subtype(item_subtype)
    return [
        hashed_customer_list_item(identifier_type, value)
        for value in _values_for_identifier_type(_identifier_items(record), identifier_type)
    ]


def _customer_list_item_count(identifiers: tuple[Mapping[str, object], ...]) -> int:
    try:
        subtype = _preferred_identifier_subtype(identifiers)
    except ValueError:
        return 0
    return len(_customer_list_items_from_identifiers(identifiers, subtype))


def _customer_list_items_from_identifiers(
    identifiers: tuple[Mapping[str, object], ...],
    item_subtype: str,
) -> list[str]:
    identifier_type = _identifier_type_for_subtype(item_subtype)
    return [
        hashed_customer_list_item(identifier_type, value)
        for value in _values_for_identifier_type(identifiers, identifier_type)
    ]


def _values_for_identifier_type(
    identifiers: tuple[Mapping[str, object], ...],
    identifier_type: str,
) -> list[str]:
    values: list[str] = []
    for identifier in identifiers:
        if identifier.get("type") != identifier_type:
            continue
        raw_value = identifier.get("value")
        if isinstance(raw_value, str) and raw_value.strip():
            values.append(raw_value)
    return values


def _identifier_type_for_subtype(item_subtype: str) -> str:
    for identifier_type, subtype in CUSTOMER_LIST_ITEM_SUBTYPES.items():
        if subtype == item_subtype:
            return identifier_type
    raise ValueError(f"Unsupported Bing Ads CustomerListItemSubType `{item_subtype}`.")


def _identifiers_column(page: pa.RecordBatch) -> pa.Array | None:
    for field_name in ("identifiers", "identifier_values", "identifiers_json"):
        index = page.schema.get_field_index(field_name)
        if index >= 0:
            return page.column(index)
    return None


def _identifier_items(record: DestinationWorkRecord) -> tuple[Mapping[str, object], ...]:
    return _identifier_items_from_value(record.identifiers)


def _identifier_items_from_value(value: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, str) and value.strip():
        try:
            return _identifier_items_from_value(json.loads(value))
        except json.JSONDecodeError:
            return ()
    if not isinstance(value, list | tuple):
        return ()
    return tuple(cast(Mapping[str, object], item) for item in value if isinstance(item, Mapping))


def _with_remote_target(
    record: DestinationWorkRecord,
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
) -> DestinationWorkRecord:
    if record.target is None:
        raise ValueError("Bing Ads CustomerList records require a Target audience id.")
    return DestinationWorkRecord(
        operation=record.operation,
        record_identity=record.record_identity,
        identifiers=record.identifiers,
        payload=record.payload,
        key=record.key,
        collect_id=record.collect_id,
        sequence_order=record.sequence_order,
        target=_remote_target_id(record.target, binding=binding, surface=surface.name),
        occurred_at=record.occurred_at,
        payload_fingerprint=record.payload_fingerprint,
        source_position=record.source_position,
        raw=record.raw,
    )


def _remote_target_id(
    logical_target: str,
    *,
    binding: DestinationBinding,
    surface: str,
) -> str:
    for mapping in binding.target_mappings:
        if mapping.logical_target == logical_target and (
            mapping.surface is None or mapping.surface == surface
        ):
            return mapping.remote.remote_id
    if binding.target_registry is not None:
        record = binding.target_registry.get(
            registry_key(binding=binding, surface=surface, logical_target=logical_target)
        )
        if record is not None:
            return record.remote.remote_id
    return logical_target


def _audience_rows(response: HttpResponse) -> tuple[Mapping[str, object], ...]:
    data = response.json_body.get("Audiences")
    if not isinstance(data, list):
        return ()
    return tuple(cast(Mapping[str, object], item) for item in data if isinstance(item, Mapping))


def _partial_errors(response: HttpResponse) -> tuple[Mapping[str, object], ...]:
    errors = response.json_body.get("PartialErrors")
    if not isinstance(errors, list):
        return ()
    return tuple(cast(Mapping[str, object], item) for item in errors if isinstance(item, Mapping))


def _partial_error_code(response: HttpResponse) -> str | None:
    errors = _partial_errors(response)
    if not errors:
        return None
    error_code = errors[0].get("ErrorCode")
    if isinstance(error_code, str) and error_code.strip():
        return error_code
    code = errors[0].get("Code")
    return str(code) if code is not None else None


def _audience_metadata(audience: Mapping[str, object]) -> Mapping[str, JSONValue]:
    metadata: dict[str, JSONValue] = {"type": CUSTOMER_LIST_TYPE}
    scope = audience.get("Scope")
    if isinstance(scope, str) and scope.strip():
        metadata["scope"] = scope
    duration = audience.get("MembershipDuration")
    if isinstance(duration, int) and not isinstance(duration, bool):
        metadata["membership_duration"] = duration
    return metadata


def _created_audience_id(response: HttpResponse) -> str | None:
    audience_ids = response.json_body.get("AudienceIds")
    if not isinstance(audience_ids, list) or not audience_ids:
        return None
    first = audience_ids[0]
    return str(first) if first is not None and str(first).strip() else None


def _target_parent_id(config: BingAdsConfig) -> str:
    if config.target_scope == "Customer":
        return config.customer_id
    return config.customer_account_id


def _header(headers: Mapping[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name.lower() and value.strip():
            return value
    return None


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
            partner_error_detail=first_failure.partner_error_detail,
            summary=first_failure.partner_message or "Bing Ads request failed before acceptance.",
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
            partner_error_detail=first_failure.partner_error_detail,
            summary=first_failure.partner_message or "Bing Ads request failed retryably.",
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
            partner_error_detail=first_failure.partner_error_detail,
            summary=first_failure.partner_message or "Bing Ads request failed terminally.",
        )
    status = _successful_status(accepted_count=accepted_count)
    summary = "Bing Ads request batches satisfied destination delivery evidence."
    if delivery_outcome == "succeeded" and accepted_count:
        summary = "Bing Ads received acceptance-only evidence for one or more request batches."
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
        summary=f"Bing Ads transport failed before response: {type(error).__name__}.",
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
    raise ValueError(f"Bing Ads did not classify outcome `{outcome}`.")


def _successful_status(*, accepted_count: int) -> Literal["confirmed", "accepted"]:
    return "confirmed" if accepted_count == 0 else "accepted"


__all__ = [
    "BingAdsCustomerListTargetClient",
    "bing_ads_managed_target_client",
    "classify_bing_ads_response",
    "plan_bing_ads_requests",
    "submit_bing_ads_destination",
]
