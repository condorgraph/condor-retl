from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

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
from retl.destinations.targets import RemoteTarget, registry_key
from retl.errors import DeclarationValidationError
from retl.events.reconcile import EventReconcileEvidence
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl_klaviyo.common import (
    MAX_LIST_RELATIONSHIP_PROFILES_PER_REQUEST,
    MAX_PROFILE_IMPORT_PAYLOAD_BYTES,
    MAX_PROFILES_PER_REQUEST,
    join_url,
    klaviyo_config,
    klaviyo_partner_error_detail,
    klaviyo_partner_message,
    transport_from_config,
)

PROFILES_SURFACE = "profiles"
LIST_MEMBERSHIPS_SURFACE = "list_memberships"
LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE = "list_memberships_by_profile_id"
_PROFILE_TOP_LEVEL_ATTRIBUTES = frozenset(
    {
        "email",
        "phone_number",
        "external_id",
        "anonymous_id",
        "first_name",
        "last_name",
        "organization",
        "locale",
        "title",
        "image",
        "location",
        "properties",
    }
)

KLAVIYO_RESPONSE_POLICY = ResponseClassificationPolicy(
    error_code_prefix="klaviyo",
    accepted_statuses=frozenset({202}),
    schema_failure_statuses=frozenset({400, 413}),
    rate_limit_statuses=frozenset({429}),
    partner_message=lambda response: klaviyo_partner_message(cast(HttpResponse, response)),
    partner_error_detail=lambda response: klaviyo_partner_error_detail(
        cast(HttpResponse, response)
    ),
    remote_handle=RemoteHandlePolicy(
        kind="klaviyo_profile_bulk_import_job", value_path=("data", "id")
    ),
    default_retry_after_seconds=60,
)


@dataclass(frozen=True)
class _SubmissionRequestPlans:
    plans: tuple[RequestBatchPlan, ...]
    request_count: int
    record_count: int


@dataclass(frozen=True)
class KlaviyoListTargetClient:
    binding: DestinationBinding
    surface: DestinationSurface
    resolved_auth: object

    def find_target(self, logical_target: str) -> RemoteTarget | None:
        if self.surface.name not in {
            LIST_MEMBERSHIPS_SURFACE,
            LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE,
        }:
            return None
        config = klaviyo_config(self.binding)
        transport = _target_transport(self.binding)
        next_url: str | None = join_url(config, "/api/lists")
        query: Mapping[str, str] = {
            "fields[list]": "name",
            "page[size]": "10",
        }
        seen_pages = 0
        while next_url is not None and seen_pages < 100:
            seen_pages += 1
            response = transport.send(
                HttpRequest(
                    method="GET",
                    url=next_url,
                    query=query,
                    headers=_klaviyo_headers(
                        self.resolved_auth,
                        revision=config.api_revision,
                    ),
                )
            )
            _raise_for_target_response(response, action="find")
            for list_row in _list_rows(response):
                attributes = list_row.get("attributes")
                if not isinstance(attributes, Mapping):
                    continue
                if attributes.get("name") != logical_target:
                    continue
                remote_id = list_row.get("id")
                if isinstance(remote_id, str) and remote_id.strip():
                    return RemoteTarget(
                        remote_id=remote_id,
                        display_name=logical_target,
                        metadata={"kind": "klaviyo_list"},
                    )
            next_url = _next_link(response)
            query = {}
        return None

    def create_target(self, logical_target: str, *, display_name: str) -> RemoteTarget:
        if self.surface.name not in {
            LIST_MEMBERSHIPS_SURFACE,
            LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE,
        }:
            raise DeclarationValidationError(
                "Klaviyo managed target creation is only supported for list surfaces."
            )
        config = klaviyo_config(self.binding)
        transport = _target_transport(self.binding)
        response = transport.send(
            HttpRequest(
                method="POST",
                url=join_url(config, "/api/lists"),
                headers=_klaviyo_headers(self.resolved_auth, revision=config.api_revision),
                json_body={
                    "data": {
                        "type": "list",
                        "attributes": {
                            "name": display_name,
                        },
                    },
                },
            )
        )
        _raise_for_target_response(response, action="create")
        data = response.json_body.get("data")
        remote_id = data.get("id") if isinstance(data, Mapping) else None
        if not isinstance(remote_id, str) or not remote_id.strip():
            raise DeclarationValidationError("Klaviyo List creation did not return `id`.")
        return RemoteTarget(
            remote_id=remote_id,
            display_name=display_name,
            metadata={"kind": "klaviyo_list"},
        )


def klaviyo_managed_target_client(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    resolved_auth: object,
) -> KlaviyoListTargetClient | None:
    if surface.name not in {
        LIST_MEMBERSHIPS_SURFACE,
        LIST_MEMBERSHIPS_BY_PROFILE_ID_SURFACE,
    }:
        return None
    return KlaviyoListTargetClient(
        binding=binding,
        surface=surface,
        resolved_auth=resolved_auth,
    )


def plan_klaviyo_requests(
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
            notes=("Klaviyo work is deferred until reconcile produces pages.",),
        )
    config = klaviyo_config(binding)
    if surface.name in {PROFILES_SURFACE, LIST_MEMBERSHIPS_SURFACE}:
        return plan_request_batches(
            sync_name=reconciled.sync_name,
            surface_name=surface.name,
            work=work,
            request_template={
                "method": "POST",
                "path": "/api/profile-bulk-import-jobs",
                "headers": {
                    "accept": "application/vnd.api+json",
                    "content-type": "application/vnd.api+json",
                    "revision": "{{ config.api_revision }}",
                },
            },
            batching_policy=RequestBatchingPolicy(
                max_rows=MAX_PROFILES_PER_REQUEST,
                max_bytes=MAX_PROFILE_IMPORT_PAYLOAD_BYTES,
            ),
            public_config={"api_revision": config.api_revision},
            dry_run=True,
            body_hook=_profiles_body,
            family="state_operations",
            partition_key=lambda record: (record.operation, record.target),
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
            "method": "{{ http_method }}",
            "path": "/api/lists/{{ target }}/relationships/profiles",
            "headers": {
                "accept": "application/vnd.api+json",
                "content-type": "application/vnd.api+json",
                "revision": "{{ config.api_revision }}",
            },
        },
        batching_policy=RequestBatchingPolicy(max_rows=MAX_LIST_RELATIONSHIP_PROFILES_PER_REQUEST),
        public_config={"api_revision": config.api_revision},
        dry_run=True,
        body_hook=_list_relationship_body,
        family="state_operations",
        partition_key=lambda record: (record.operation, record.target),
        record_hook=lambda record: _with_remote_target(
            record,
            binding=binding,
            surface=surface,
        ),
    )


def submit_klaviyo_destination(
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
                f"Klaviyo dry run planned {request_plans.request_count} bulk import job(s) "
                f"for {request_plans.record_count} profile(s)."
            ),
        )

    request_plan = _single_submission_request_plan(
        plans=request_plans.plans,
        connector_name="Klaviyo",
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
            summary="Klaviyo submission requires an HTTP transport; no request was sent.",
        )
    if request_plan is None:
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=False,
            request_batch_count=0,
            summary="Klaviyo submission had no request batch to execute.",
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
    classification = classify_klaviyo_response(response)
    return _aggregate_submission_evidence(
        [(classification, request_plan.row_count)],
        delivery_outcome=delivery_outcome,
        attempted_count=attempted_count,
        request_batch_count=request_plans.request_count,
    )


def classify_klaviyo_response(response: HttpResponse) -> ResponseClassification:
    return classify_response(response, policy=KLAVIYO_RESPONSE_POLICY)


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
    plan = plan_klaviyo_requests(binding=binding, surface=surface, reconciled=reconciled)
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
    config = klaviyo_config(binding)
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
        raise DeclarationValidationError("Klaviyo managed targets require an HTTP transport.")
    return transport


def _klaviyo_headers(resolved_auth: object, *, revision: str) -> Mapping[str, str]:
    return {
        "accept": "application/vnd.api+json",
        "content-type": "application/vnd.api+json",
        "revision": revision,
        **_auth_headers(resolved_auth),
    }


def _auth_headers(resolved_auth: object) -> Mapping[str, str]:
    headers = getattr(resolved_auth, "headers", {})
    if not isinstance(headers, Mapping):
        return {}
    return {str(key): str(value) for key, value in headers.items()}


def _raise_for_target_response(response: HttpResponse, *, action: str) -> None:
    classification = classify_klaviyo_response(response)
    if classification.outcome in {"confirmed", "accepted"}:
        return
    detail = classification.partner_message or f"Klaviyo List {action} failed."
    raise DeclarationValidationError(detail)


def _list_rows(response: HttpResponse) -> tuple[Mapping[str, object], ...]:
    data = response.json_body.get("data")
    if not isinstance(data, list):
        return ()
    rows: list[Mapping[str, object]] = []
    for item in data:
        if isinstance(item, Mapping):
            rows.append(cast(Mapping[str, object], item))
    return tuple(rows)


def _next_link(response: HttpResponse) -> str | None:
    links = response.json_body.get("links")
    if not isinstance(links, Mapping):
        return None
    next_url = links.get("next")
    return next_url if isinstance(next_url, str) and next_url.strip() else None


def _profiles_body(context: RequestBatchContext) -> JSONValue:
    profiles = [_profile_object(record) for record in context.records]
    data: dict[str, JSONValue] = {
        "type": "profile-bulk-import-job",
        "attributes": {
            "profiles": {
                "data": profiles,
            },
        },
    }
    if context.surface_name == LIST_MEMBERSHIPS_SURFACE:
        if context.target is None:
            raise ValueError("Klaviyo list membership imports require a Target list id.")
        data["relationships"] = {
            "lists": {
                "data": [
                    {
                        "type": "list",
                        "id": context.target,
                    }
                ]
            }
        }
    body: dict[str, JSONValue] = {"data": data}
    return body


def _profile_object(record: DestinationWorkRecord) -> JSONValue:
    if record.operation != "upsert":
        raise ValueError("Klaviyo bulk profile import supports only upsert operations.")
    attributes = _profile_attributes(record)
    profile: dict[str, JSONValue] = {
        "type": "profile",
        "attributes": attributes,
    }
    profile_id = _klaviyo_profile_id(record)
    if profile_id is not None:
        profile["id"] = profile_id
    return profile


def _profile_attributes(record: DestinationWorkRecord) -> JSONValue:
    attributes = _identifier_attributes(record)
    payload = record.payload if isinstance(record.payload, Mapping) else {}
    properties: dict[str, JSONValue] = {}

    for key, value in payload.items():
        if not isinstance(key, str) or value is None:
            continue
        if key == "properties" and isinstance(value, Mapping):
            properties.update(_non_null_mapping(value))
            continue
        if key in {"email", "phone_number", "external_id"}:
            continue
        if key in _PROFILE_TOP_LEVEL_ATTRIBUTES:
            attributes[key] = _clean_json_value(value)
        else:
            properties[key] = _clean_json_value(value)

    if properties:
        existing = attributes.get("properties")
        if isinstance(existing, Mapping):
            merged = {**dict(existing), **properties}
            attributes["properties"] = cast(JSONValue, merged)
        else:
            attributes["properties"] = properties
    if not any(field in attributes for field in ("email", "phone_number", "external_id")):
        if _klaviyo_profile_id(record) is None:
            raise ValueError(
                "Klaviyo bulk profile import records require email, phone_e164, "
                "external_id, or klaviyo_profile_id."
            )
    return cast(JSONValue, attributes)


def _identifier_attributes(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    attributes: dict[str, JSONValue] = {}
    for identifier in _identifier_items(record):
        identifier_type = identifier.get("type")
        raw_value = identifier.get("value")
        if not isinstance(raw_value, str) or not raw_value.strip():
            continue
        value = raw_value.strip()
        if identifier_type == "email":
            attributes.setdefault("email", value)
        elif identifier_type == "phone_e164":
            attributes.setdefault("phone_number", value)
        elif identifier_type == "external_id":
            attributes.setdefault("external_id", value)
    return attributes


def _identifier_items(record: DestinationWorkRecord) -> tuple[Mapping[str, object], ...]:
    raw_identifiers = record.identifiers
    if not isinstance(raw_identifiers, list | tuple):
        return ()
    items: list[Mapping[str, object]] = []
    for item in raw_identifiers:
        if isinstance(item, Mapping):
            items.append(cast(Mapping[str, object], item))
    return tuple(items)


def _list_relationship_body(context: RequestBatchContext) -> JSONValue:
    return {
        "data": [
            {
                "type": "profile",
                "id": _required_klaviyo_profile_id(record),
            }
            for record in context.records
        ]
    }


def _with_remote_target(
    record: DestinationWorkRecord,
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
) -> DestinationWorkRecord:
    if surface.target_mode == "unsupported":
        return record
    if record.target is None:
        raise ValueError(f"Klaviyo `{surface.name}` records require a Target list id.")
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


def _klaviyo_profile_id(record: DestinationWorkRecord) -> str | None:
    for identifier in _identifier_items(record):
        if identifier.get("type") == "klaviyo_profile_id":
            raw_value = identifier.get("value")
            if isinstance(raw_value, str) and raw_value.strip():
                return raw_value.strip()
    return None


def _required_klaviyo_profile_id(record: DestinationWorkRecord) -> str:
    profile_id = _klaviyo_profile_id(record)
    if profile_id is None:
        raise ValueError("Klaviyo list membership add/remove records require `klaviyo_profile_id`.")
    return profile_id


def _non_null_mapping(value: Mapping[str, object]) -> dict[str, JSONValue]:
    cleaned: dict[str, JSONValue] = {}
    for key, item in value.items():
        if isinstance(key, str) and item is not None:
            cleaned[key] = _clean_json_value(cast(JSONValue, item))
    return cleaned


def _clean_json_value(value: JSONValue) -> JSONValue:
    if isinstance(value, Mapping):
        return cast(JSONValue, _non_null_mapping(value))
    if isinstance(value, list):
        return [_clean_json_value(item) for item in value if item is not None]
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
            summary=first_failure.partner_message or "Klaviyo request failed before acceptance.",
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
            summary=first_failure.partner_message or "Klaviyo request failed retryably.",
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
            summary=first_failure.partner_message or "Klaviyo request failed terminally.",
        )
    status = _successful_status(accepted_count=accepted_count)
    summary = "Klaviyo bulk profile import jobs were accepted."
    if delivery_outcome == "succeeded" and accepted_count:
        summary = "Klaviyo returned acceptance-only evidence for one or more import jobs."
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
        summary=f"Klaviyo transport failed before response: {type(error).__name__}.",
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
    raise ValueError(f"Klaviyo did not classify outcome `{outcome}`.")


def _successful_status(*, accepted_count: int) -> Literal["confirmed", "accepted"]:
    return "confirmed" if accepted_count == 0 else "accepted"
