from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, cast

import pyarrow as pa

from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.acknowledgements import DestinationReceipt, DestinationSubmissionEvidence
from retl.destinations.http import HttpRequest, HttpResponse, HttpTransport
from retl.destinations.identifiers import hash_or_preserve_sha256_hex
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
from retl.destinations.targets import RemoteTarget, registry_key
from retl.errors import DeclarationValidationError
from retl.events.reconcile import EventReconcileEvidence
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl_meta.common import (
    MAX_CUSTOM_AUDIENCE_ROWS_PER_REQUEST,
    MAX_EVENT_ROWS_PER_REQUEST,
    join_url,
    meta_config,
    meta_partner_error_detail,
    meta_partner_message,
    transport_from_config,
)

CUSTOM_AUDIENCES_SURFACE = "custom_audiences"
EVENTS_SURFACE = "events"
CUSTOM_AUDIENCE_SUBTYPE = "CUSTOM"
DEFAULT_CUSTOMER_FILE_SOURCE = "USER_PROVIDED_ONLY"
CUSTOMER_FILE_SOURCES = frozenset(
    {
        "USER_PROVIDED_ONLY",
        "PARTNER_PROVIDED_ONLY",
        "BOTH_USER_AND_PARTNER_PROVIDED",
    }
)

META_RESPONSE_POLICY = ResponseClassificationPolicy(
    error_code_prefix="meta",
    partner_message=lambda response: meta_partner_message(cast(HttpResponse, response)),
    partner_error_detail=lambda response: meta_partner_error_detail(cast(HttpResponse, response)),
    remote_handle=RemoteHandlePolicy(kind="meta_request", value_path=("request_id",)),
    default_retry_after_seconds=60,
)


@dataclass(frozen=True)
class _SubmissionRequestPlans:
    plans: tuple[RequestBatchPlan, ...]
    request_count: int
    record_count: int


@dataclass(frozen=True)
class MetaCustomAudienceTargetClient:
    binding: DestinationBinding
    surface: DestinationSurface
    resolved_auth: object

    def find_target(self, logical_target: str) -> RemoteTarget | None:
        if self.surface.name != CUSTOM_AUDIENCES_SURFACE:
            return None
        config = meta_config(self.binding)
        transport = _target_transport(self.binding)
        url = join_url(
            config,
            f"/{config.api_version}/{config.normalized_ad_account_id}/customaudiences",
        )
        next_url: str | None = url
        query: Mapping[str, str] = {
            "fields": "id,name,subtype",
            "limit": "20",
        }
        seen_pages = 0
        while next_url is not None and seen_pages < 100:
            seen_pages += 1
            response = transport.send(
                HttpRequest(
                    method="GET",
                    url=next_url,
                    query=query,
                    headers=_auth_headers(self.resolved_auth),
                )
            )
            _raise_for_target_response(response, action="find")
            for audience in _audience_rows(response):
                if audience.get("name") == logical_target:
                    subtype = audience.get("subtype")
                    if subtype != CUSTOM_AUDIENCE_SUBTYPE:
                        raise DeclarationValidationError(
                            "Meta Custom Audience "
                            f"`{logical_target}` exists with unsupported subtype `{subtype}`."
                        )
                    remote_id = audience.get("id")
                    if isinstance(remote_id, str) and remote_id.strip():
                        return RemoteTarget(
                            remote_id=remote_id,
                            display_name=logical_target,
                            metadata=_audience_metadata(audience),
                        )
            next_url = _next_page_url(response)
            query = {}
        return None

    def create_target(self, logical_target: str, *, display_name: str) -> RemoteTarget:
        if self.surface.name != CUSTOM_AUDIENCES_SURFACE:
            raise DeclarationValidationError(
                "Meta managed target creation is only supported for Custom Audiences."
            )
        config = meta_config(self.binding)
        transport = _target_transport(self.binding)
        customer_file_source = _customer_file_source(self.binding)
        response = transport.send(
            HttpRequest(
                method="POST",
                url=join_url(
                    config,
                    f"/{config.api_version}/{config.normalized_ad_account_id}/customaudiences",
                ),
                headers=_auth_headers(self.resolved_auth),
                json_body={
                    "name": display_name,
                    "subtype": CUSTOM_AUDIENCE_SUBTYPE,
                    "description": f"Managed by RETL for `{logical_target}`.",
                    "customer_file_source": customer_file_source,
                },
            )
        )
        _raise_for_target_response(response, action="create")
        remote_id = response.json_body.get("id")
        if not isinstance(remote_id, str) or not remote_id.strip():
            raise DeclarationValidationError("Meta Custom Audience creation did not return `id`.")
        return RemoteTarget(
            remote_id=remote_id,
            display_name=display_name,
            metadata={
                "subtype": CUSTOM_AUDIENCE_SUBTYPE,
                "customer_file_source": customer_file_source,
            },
        )


def meta_managed_target_client(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    resolved_auth: object,
) -> MetaCustomAudienceTargetClient | None:
    if surface.name != CUSTOM_AUDIENCES_SURFACE:
        return None
    return MetaCustomAudienceTargetClient(
        binding=binding,
        surface=surface,
        resolved_auth=resolved_auth,
    )


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
    plan = plan_meta_requests(binding=binding, surface=surface, reconciled=reconciled)
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


def submit_meta_destination(
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
                f"Meta dry run planned {request_plans.request_count} request batch(es) for "
                f"{request_plans.record_count} record(s)."
            ),
        )

    request_plan = _single_submission_request_plan(
        plans=request_plans.plans,
        connector_name="Meta",
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
            summary="Meta submission requires an HTTP transport; no request was sent.",
        )
    if request_plan is None:
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=False,
            request_batch_count=0,
            summary="Meta submission had no request batch to execute.",
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
    classification = classify_meta_response(response)
    return _aggregate_submission_evidence(
        [(classification, request_plan.row_count)],
        delivery_outcome=delivery_outcome,
        attempted_count=attempted_count,
        request_batch_count=request_plans.request_count,
    )


def plan_meta_requests(
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
            notes=("Meta work is deferred until reconcile produces pages.",),
        )
    config = meta_config(binding)
    if surface.name == CUSTOM_AUDIENCES_SURFACE:
        return plan_request_batches(
            sync_name=reconciled.sync_name,
            surface_name=surface.name,
            work=work,
            request_template={
                "method": "{{ http_method }}",
                "path": "/{{ config.api_version }}/{{ target }}/users",
            },
            batching_policy=RequestBatchingPolicy(max_rows=MAX_CUSTOM_AUDIENCE_ROWS_PER_REQUEST),
            public_config={"api_version": config.api_version},
            dry_run=True,
            body_hook=_custom_audience_body,
            request_item_counts=_custom_audience_request_item_counts,
            family="state_operations",
            partition_key=lambda record: (record.target, record.operation),
            record_hook=lambda record: _with_remote_target(
                record,
                binding=binding,
                surface=surface,
            ),
        )
    return plan_request_batches(
        sync_name=reconciled.sync_name,
        surface_name=surface.name,
        work=work,
        request_template={
            "method": "POST",
            "path": "/{{ config.api_version }}/{{ target }}/events",
        },
        batching_policy=RequestBatchingPolicy(max_rows=MAX_EVENT_ROWS_PER_REQUEST),
        public_config={
            "api_version": config.api_version,
            **_event_public_config(binding),
        },
        dry_run=True,
        body_hook=_events_body,
        family="event_imports",
        partition_key=_event_pixel_id,
        record_hook=lambda record: _with_event_route(record, binding=binding),
    )


def classify_meta_response(response: HttpResponse) -> ResponseClassification:
    return classify_response(response, policy=META_RESPONSE_POLICY)


def _http_request(
    *,
    binding: DestinationBinding,
    request_plan: RequestBatchPlan,
    resolved_auth: object,
) -> HttpRequest:
    config = meta_config(binding)
    auth_headers = getattr(resolved_auth, "headers", {})
    if not isinstance(auth_headers, Mapping):
        auth_headers = {}
    request = request_plan.request
    return HttpRequest(
        method=request.method,
        url=join_url(config, request.path),
        query=request.query,
        headers={**dict(request.headers), **cast(Mapping[str, str], auth_headers)},
        json_body=request.json_body,
    )


def _target_transport(binding: DestinationBinding) -> HttpTransport:
    transport = transport_from_config(binding.config)
    if transport is None:
        raise DeclarationValidationError("Meta managed targets require an HTTP transport.")
    return transport


def _auth_headers(resolved_auth: object) -> Mapping[str, str]:
    headers = getattr(resolved_auth, "headers", {})
    if not isinstance(headers, Mapping):
        return {}
    return {str(key): str(value) for key, value in headers.items()}


def _raise_for_target_response(response: HttpResponse, *, action: str) -> None:
    classification = classify_meta_response(response)
    if classification.outcome in {"confirmed", "accepted"}:
        return
    detail = classification.partner_message or f"Meta Custom Audience {action} failed."
    raise DeclarationValidationError(detail)


def _audience_rows(response: HttpResponse) -> tuple[Mapping[str, object], ...]:
    data = response.json_body.get("data")
    if not isinstance(data, list):
        return ()
    rows: list[Mapping[str, object]] = []
    for item in data:
        if isinstance(item, Mapping):
            rows.append(cast(Mapping[str, object], item))
    return tuple(rows)


def _audience_metadata(audience: Mapping[str, object]) -> Mapping[str, JSONValue]:
    metadata: dict[str, JSONValue] = {}
    subtype = audience.get("subtype")
    if isinstance(subtype, str) and subtype.strip():
        metadata["subtype"] = subtype
    return metadata


def _next_page_url(response: HttpResponse) -> str | None:
    paging = response.json_body.get("paging")
    if not isinstance(paging, Mapping):
        return None
    next_url = paging.get("next")
    return next_url if isinstance(next_url, str) and next_url.strip() else None


def _customer_file_source(binding: DestinationBinding) -> str:
    raw_value = binding.config.get("custom_audience_customer_file_source")
    if raw_value is None:
        return DEFAULT_CUSTOMER_FILE_SOURCE
    if not isinstance(raw_value, str):
        raise DeclarationValidationError(
            "Meta config `custom_audience_customer_file_source` must be a string."
        )
    value = raw_value.strip()
    if value not in CUSTOMER_FILE_SOURCES:
        supported = ", ".join(sorted(CUSTOMER_FILE_SOURCES))
        raise DeclarationValidationError(
            f"Meta config `custom_audience_customer_file_source` must be one of: {supported}."
        )
    return value


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
            summary=first_failure.partner_message or "Meta request failed before acceptance.",
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
            summary=first_failure.partner_message or "Meta request failed retryably.",
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
            summary=first_failure.partner_message or "Meta request failed terminally.",
        )
    status = _successful_status(accepted_count=accepted_count)
    summary = "Meta request batches satisfied destination delivery evidence."
    if delivery_outcome == "succeeded" and accepted_count:
        summary = "Meta received acceptance-only evidence for one or more request batches."
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
        summary=f"Meta transport failed before response: {type(error).__name__}.",
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
    raise ValueError(f"Meta did not classify outcome `{outcome}`.")


def _successful_status(*, accepted_count: int) -> Literal["confirmed", "accepted"]:
    return "confirmed" if accepted_count == 0 else "accepted"


def _with_remote_target(
    record: DestinationWorkRecord,
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
) -> DestinationWorkRecord:
    if record.target is None:
        raise ValueError("Meta Custom Audiences records require a Target audience id.")
    remote_id = _remote_target_id(record.target, binding=binding, surface=surface.name)
    return DestinationWorkRecord(
        operation=record.operation,
        record_identity=record.record_identity,
        identifiers=record.identifiers,
        payload=record.payload,
        key=record.key,
        collect_id=record.collect_id,
        sequence_order=record.sequence_order,
        target=remote_id,
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


def _custom_audience_body(context: RequestBatchContext) -> JSONValue:
    schema = _audience_schema(context.records)
    data = [row for record in context.records for row in _audience_data_rows(record, schema=schema)]
    return {
        "payload": {
            "schema": schema,
            "data": data,
        }
    }


def _audience_schema(records: tuple[DestinationWorkRecord, ...]) -> list[str]:
    schema: list[str] = []
    for record in records:
        for identifier in _identifier_items(record):
            meta_name = _meta_identifier_name(identifier.get("type"))
            if meta_name is not None and meta_name not in schema:
                schema.append(meta_name)
    if not schema:
        raise ValueError("Meta Custom Audiences records require at least one accepted identifier.")
    return schema


def _custom_audience_request_item_counts(page: pa.RecordBatch) -> pa.Array:
    identifiers_column = _identifiers_column(page)
    if identifiers_column is None:
        return pa.array([0] * page.num_rows, type=pa.int64())
    return pa.array(
        [
            _audience_request_item_count_from_identifiers(
                _identifier_items_from_value(scalar.as_py())
            )
            for scalar in identifiers_column
        ],
        type=pa.int64(),
    )


def _identifiers_column(page: pa.RecordBatch) -> pa.Array | None:
    for field_name in ("identifiers", "identifier_values", "identifiers_json"):
        index = page.schema.get_field_index(field_name)
        if index >= 0:
            return page.column(index)
    return None


def _audience_data_rows(
    record: DestinationWorkRecord,
    *,
    schema: list[str],
) -> list[list[str | None]]:
    values_by_name = _audience_values_by_name(_identifier_items(record))
    if _audience_values_are_scalar_pairable(values_by_name):
        return [[values_by_name.get(name, [None])[0] for name in schema]]

    rows: list[list[str | None]] = []
    for name in schema:
        column_index = schema.index(name)
        for value in values_by_name.get(name, []):
            row: list[str | None] = [None] * len(schema)
            row[column_index] = value
            rows.append(row)
    return rows


def _audience_values_by_name(
    identifiers: tuple[Mapping[str, object], ...],
) -> dict[str, list[str]]:
    values_by_name: dict[str, list[str]] = {}
    for identifier in identifiers:
        meta_name = _meta_identifier_name(identifier.get("type"))
        raw_value = identifier.get("value")
        if meta_name is None or not isinstance(raw_value, str) or not raw_value.strip():
            continue
        values_by_name.setdefault(meta_name, []).append(
            _meta_identifier_value(meta_name, raw_value)
        )
    return values_by_name


def _audience_request_item_count_from_identifiers(
    identifiers: tuple[Mapping[str, object], ...],
) -> int:
    return _audience_request_item_count(_audience_values_by_name(identifiers))


def _audience_request_item_count(values_by_name: Mapping[str, list[str]]) -> int:
    if not values_by_name:
        return 0
    if _audience_values_are_scalar_pairable(values_by_name):
        return 1
    return sum(len(values) for values in values_by_name.values())


def _audience_values_are_scalar_pairable(values_by_name: Mapping[str, list[str]]) -> bool:
    return bool(values_by_name) and all(len(values) == 1 for values in values_by_name.values())


def _meta_identifier_name(identifier_type: object) -> str | None:
    if identifier_type == "email":
        return "EMAIL"
    if identifier_type == "phone_e164":
        return "PHONE"
    if identifier_type == "mobile_advertising_id":
        return "MADID"
    return None


def _meta_identifier_value(meta_name: str, value: str) -> str:
    if meta_name in {"EMAIL", "PHONE"}:
        return hash_or_preserve_sha256_hex(value, normalizer=_lowercase_identifier)
    return value.strip()


def _events_body(context: RequestBatchContext) -> JSONValue:
    body: dict[str, JSONValue] = {
        "data": [
            {
                "event_name": _event_name(record),
                "event_id": record.record_identity,
                "event_time": _event_time(record),
                "action_source": _event_action_source(context.public_config),
                "user_data": _event_user_data(record),
                "custom_data": _event_custom_data(record),
                **_event_top_level_payload(record),
            }
            for record in context.records
        ]
    }
    test_event_code = context.public_config.get("test_event_code")
    if isinstance(test_event_code, str) and test_event_code:
        body["test_event_code"] = test_event_code
    return body


def _with_event_route(
    record: DestinationWorkRecord,
    *,
    binding: DestinationBinding,
) -> DestinationWorkRecord:
    return DestinationWorkRecord(
        operation=record.operation,
        record_identity=record.record_identity,
        identifiers=record.identifiers,
        payload=record.payload,
        key=record.key,
        collect_id=record.collect_id,
        sequence_order=record.sequence_order,
        target=_event_route(record, binding=binding),
        occurred_at=record.occurred_at,
        payload_fingerprint=record.payload_fingerprint,
        source_position=record.source_position,
        raw=record.raw,
    )


def _event_route(record: DestinationWorkRecord, *, binding: DestinationBinding) -> str:
    event_name = _event_name(record)
    routes = binding.config.get("event_routes")
    if isinstance(routes, Mapping):
        route = routes.get(event_name)
        if isinstance(route, str) and route.strip():
            return route.strip()
    pixel_id = binding.config.get("pixel_id")
    if isinstance(pixel_id, str) and pixel_id.strip():
        return pixel_id.strip()
    raise ValueError(
        f"Meta Events require `pixel_id` or an `event_routes` entry for event `{event_name}`."
    )


def _event_public_config(binding: DestinationBinding) -> Mapping[str, JSONValue]:
    values: dict[str, JSONValue] = {}
    for key in ("action_source", "test_event_code"):
        value = binding.config.get(key)
        if isinstance(value, str) and value.strip():
            values[key] = value.strip()
    return values


def _event_pixel_id(record: DestinationWorkRecord) -> str:
    if record.target is None:
        raise ValueError("Meta Events records require a routed pixel id.")
    return record.target


def _event_name(record: DestinationWorkRecord) -> str:
    for value in (
        record.raw.get("event_name"),
        record.raw.get("event_type"),
        _payload_value(record, "event_name"),
        _payload_value(record, "event_type"),
    ):
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise ValueError("Meta Events records require `event_name` or `event_type`.")


def _event_time(record: DestinationWorkRecord) -> int:
    if record.occurred_at is None:
        raise ValueError("Meta Events records require `occurred_at`.")
    try:
        parsed = datetime.fromisoformat(record.occurred_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Meta Events require RFC3339 `occurred_at`.") from exc
    if parsed.tzinfo is None:
        raise ValueError("Meta Events require timezone-aware `occurred_at`.")
    return int(parsed.astimezone(timezone.utc).timestamp())


def _event_action_source(config: Mapping[str, JSONValue]) -> str:
    value = config.get("action_source")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Meta Events require non-empty `action_source` config.")
    return value.strip()


def _event_user_data(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    user_data: dict[str, JSONValue] = {}
    for identifier in _identifier_items(record):
        raw_type = identifier.get("type")
        raw_value = identifier.get("value")
        if not isinstance(raw_value, str) or not raw_value.strip():
            continue
        if raw_type == "email":
            _append_user_data(
                user_data,
                "em",
                hash_or_preserve_sha256_hex(raw_value, normalizer=_lowercase_identifier),
            )
        elif raw_type == "phone_e164":
            _append_user_data(
                user_data,
                "ph",
                hash_or_preserve_sha256_hex(raw_value, normalizer=_lowercase_identifier),
            )
        elif raw_type == "external_id":
            _append_user_data(user_data, "external_id", hash_or_preserve_sha256_hex(raw_value))
    for key in ("fbc", "fbp", "client_ip_address", "client_user_agent"):
        value = _payload_value(record, key)
        if isinstance(value, str) and value.strip():
            user_data[key] = value.strip()
    if not user_data:
        raise ValueError("Meta Events require at least one user_data parameter.")
    return user_data


def _append_user_data(user_data: dict[str, JSONValue], key: str, value: str) -> None:
    current = user_data.get(key)
    if isinstance(current, list):
        current.append(value)
        return
    user_data[key] = [value]


def _lowercase_identifier(value: str) -> str:
    return value.lower()


def _event_custom_data(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    payload = _payload_mapping(record)
    custom_data = payload.get("custom_data")
    data = (
        dict(cast(Mapping[str, JSONValue], custom_data)) if isinstance(custom_data, Mapping) else {}
    )
    for key, value in payload.items():
        if key not in {
            "event_name",
            "event_type",
            "action_source",
            "custom_data",
            "fbc",
            "fbp",
            "client_ip_address",
            "client_user_agent",
            "event_source_url",
            "opt_out",
        }:
            data.setdefault(key, value)
    return data


def _event_top_level_payload(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    payload = _payload_mapping(record)
    return {key: payload[key] for key in ("event_source_url", "opt_out") if key in payload}


def _payload_value(record: DestinationWorkRecord, key: str) -> JSONValue | None:
    return _payload_mapping(record).get(key)


def _payload_mapping(record: DestinationWorkRecord) -> Mapping[str, JSONValue]:
    if isinstance(record.payload, Mapping):
        return cast(Mapping[str, JSONValue], record.payload)
    return {}


def _identifier_items(record: DestinationWorkRecord) -> tuple[Mapping[str, object], ...]:
    return _identifier_items_from_value(record.identifiers)


def _identifier_items_from_value(value: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, str):
        try:
            value = cast(object, _plain_json(json.loads(value)))
        except json.JSONDecodeError:
            return ()
    if isinstance(value, list | tuple):
        return tuple(item for item in value if isinstance(item, Mapping))
    return ()


def _plain_json(value: object) -> object:
    if hasattr(value, "as_py"):
        return _plain_json(value.as_py())
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_plain_json(item) for item in value]
    return value


__all__ = [
    "CUSTOM_AUDIENCES_SURFACE",
    "EVENTS_SURFACE",
    "MetaCustomAudienceTargetClient",
    "classify_meta_response",
    "meta_managed_target_client",
    "plan_meta_requests",
    "submit_meta_destination",
]
