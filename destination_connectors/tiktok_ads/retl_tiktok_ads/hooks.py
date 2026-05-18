from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

import pyarrow as pa

from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.acknowledgements import (
    DestinationReceipt,
    DestinationSubmissionEvidence,
    RemoteHandle,
)
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
from retl_tiktok_ads.common import (
    MAX_CUSTOM_AUDIENCE_FILE_ROWS_PER_REQUEST,
    TIKTOK_ADS_API_VERSION,
    RequestsTikTokAdsTransport,
    join_url,
    tiktok_ads_config,
    tiktok_identifier_value,
    tiktok_partner_error_detail,
    tiktok_partner_message,
    transport_from_config,
)

CUSTOM_AUDIENCES_SURFACE = "custom_audiences"
DMP_UPDATE_ACTIONS = {
    "upsert": "APPEND",
    "remove": "REMOVE",
}
IDENTIFIER_ID_TYPES = {
    "email": "EMAIL_SHA256",
    "phone_e164": "PHONE_SHA256",
}

TIKTOK_ADS_RESPONSE_POLICY = ResponseClassificationPolicy(
    error_code_prefix="tiktok_ads",
    accepted_statuses=frozenset({200}),
    partner_message=lambda response: tiktok_partner_message(cast(HttpResponse, response)),
    partner_error_detail=lambda response: tiktok_partner_error_detail(cast(HttpResponse, response)),
    remote_handle=RemoteHandlePolicy(kind="tiktok_request", value_path=("request_id",)),
    default_retry_after_seconds=60,
)


@dataclass(frozen=True)
class _SubmissionRequestPlans:
    plans: tuple[RequestBatchPlan, ...]
    request_count: int
    record_count: int


@dataclass(frozen=True)
class TikTokAdsCustomAudienceTargetClient:
    binding: DestinationBinding
    surface: DestinationSurface
    resolved_auth: object

    def find_target(self, logical_target: str) -> RemoteTarget | None:
        if self.surface.name != CUSTOM_AUDIENCES_SURFACE:
            return None
        config = tiktok_ads_config(self.binding)
        transport = _target_transport(self.binding)
        page = 1
        while page <= 100:
            response = transport.send(
                HttpRequest(
                    method="GET",
                    url=join_url(
                        config,
                        f"/open_api/{config.api_version}/dmp/custom_audience/list/",
                    ),
                    query={
                        "advertiser_id": config.advertiser_id,
                        "page": str(page),
                        "page_size": "100",
                    },
                    headers=_tiktok_headers(self.resolved_auth),
                )
            )
            _raise_for_target_response(response, action="find")
            for audience in _audience_rows(response):
                if audience.get("custom_audience_name") != logical_target:
                    continue
                remote_id = audience.get("custom_audience_id")
                if remote_id is not None and str(remote_id).strip():
                    return RemoteTarget(
                        remote_id=str(remote_id),
                        display_name=logical_target,
                        metadata=_audience_metadata(audience),
                    )
            if not _has_next_page(response, page=page):
                return None
            page += 1
        return None

    def create_target(self, logical_target: str, *, display_name: str) -> RemoteTarget:
        if self.surface.name != CUSTOM_AUDIENCES_SURFACE:
            raise DeclarationValidationError(
                "TikTok Ads managed target creation is only supported for Custom Audiences."
            )
        config = tiktok_ads_config(self.binding)
        transport = _target_transport(self.binding)
        calculate_type = "EMAIL_SHA256"
        file_path = _upload_audience_file(
            transport=transport,
            config=config,
            resolved_auth=self.resolved_auth,
            calculate_type=calculate_type,
            values=(_managed_target_seed_identifier(logical_target),),
            filename=f"retl_tiktok_ads_seed_{logical_target}.txt",
        )
        response = transport.send(
            HttpRequest(
                method="POST",
                url=join_url(
                    config,
                    f"/open_api/{config.api_version}/dmp/custom_audience/create/",
                ),
                headers={
                    "Content-Type": "application/json",
                    **_tiktok_headers(self.resolved_auth),
                },
                json_body={
                    "custom_audience_name": display_name,
                    "advertiser_id": config.advertiser_id,
                    "file_paths": [file_path],
                    "calculate_type": calculate_type,
                },
            )
        )
        _raise_for_target_response(response, action="create")
        remote_id = _created_audience_id(response)
        if remote_id is None:
            created = self._find_created_target(
                logical_target=logical_target, display_name=display_name
            )
            if created is not None:
                return created
            raise DeclarationValidationError(
                "TikTok Ads Custom Audience creation did not return an id."
            )
        return RemoteTarget(
            remote_id=remote_id,
            display_name=display_name,
            metadata={
                "kind": "tiktok_custom_audience",
                "id_type": config.mobile_advertising_id_type,
            },
        )

    def _find_created_target(
        self, *, logical_target: str, display_name: str
    ) -> RemoteTarget | None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            target = self.find_target(display_name)
            if target is not None:
                return target
            if display_name != logical_target:
                target = self.find_target(logical_target)
                if target is not None:
                    return target
            time.sleep(2)
        return None


def tiktok_ads_managed_target_client(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    resolved_auth: object,
) -> TikTokAdsCustomAudienceTargetClient | None:
    if surface.name != CUSTOM_AUDIENCES_SURFACE:
        return None
    return TikTokAdsCustomAudienceTargetClient(
        binding=binding,
        surface=surface,
        resolved_auth=resolved_auth,
    )


def plan_tiktok_ads_requests(
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
            notes=("TikTok Ads work is deferred until reconcile produces pages.",),
        )
    config = tiktok_ads_config(binding)
    return plan_request_batches(
        sync_name=reconciled.sync_name,
        surface_name=surface.name,
        work=work,
        request_template={
            "method": "POST",
            "path": f"/open_api/{TIKTOK_ADS_API_VERSION}/dmp/custom_audience/update/",
            "headers": {"Content-Type": "application/json"},
        },
        batching_policy=RequestBatchingPolicy(max_rows=MAX_CUSTOM_AUDIENCE_FILE_ROWS_PER_REQUEST),
        public_config={
            "advertiser_id": config.advertiser_id,
            "api_version": config.api_version,
            "mobile_advertising_id_type": config.mobile_advertising_id_type,
        },
        dry_run=True,
        body_hook=_dmp_update_body,
        request_item_counts=_dmp_update_request_item_counts,
        family="state_operations",
        partition_key=_dmp_update_partition,
        record_hook=lambda record: _with_remote_target(
            record,
            binding=binding,
            surface=surface,
        ),
    )


def submit_tiktok_ads_destination(
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
                f"TikTok Ads dry run planned {request_plans.request_count} DMP file update "
                f"request batch(es) for {request_plans.record_count} record(s)."
            ),
        )

    request_plan = _single_submission_request_plan(
        plans=request_plans.plans,
        connector_name="TikTok Ads",
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
            summary="TikTok Ads submission requires an HTTP transport; no request was sent.",
        )
    if request_plan is None:
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=False,
            request_batch_count=0,
            summary="TikTok Ads submission had no request batch to execute.",
        )

    try:
        classification = _submit_dmp_update(
            binding=binding,
            transport=transport,
            request_plan=request_plan,
            resolved_auth=resolved_auth,
        )
    except Exception as exc:
        return _transport_failure_evidence(
            attempted_count=attempted_count,
            failed_count=request_plan.row_count,
            request_batch_count=request_plans.request_count,
            error=exc,
        )
    return _aggregate_submission_evidence(
        [(classification, request_plan.row_count)],
        delivery_outcome=delivery_outcome,
        attempted_count=attempted_count,
        request_batch_count=request_plans.request_count,
    )


def classify_tiktok_ads_response(response: HttpResponse) -> ResponseClassification:
    if response.status_code == 200 and response.json_body.get("code") in (0, "0", None):
        return ResponseClassification(
            outcome="accepted",
            status_code=response.status_code,
            remote_handle=_remote_handle(response),
            partner_message=tiktok_partner_message(response),
            partner_error_detail=tiktok_partner_error_detail(response),
        )
    if response.status_code in range(200, 300) and response.json_body.get("code") not in (
        0,
        "0",
        None,
    ):
        return ResponseClassification(
            outcome="terminal_record_failure",
            status_code=response.status_code,
            error_code="tiktok_ads_terminal",
            partner_error_code=str(response.json_body.get("code")),
            partner_message=tiktok_partner_message(response),
            partner_error_detail=tiktok_partner_error_detail(response),
        )
    return classify_response(response, policy=TIKTOK_ADS_RESPONSE_POLICY)


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
    plan = plan_tiktok_ads_requests(binding=binding, surface=surface, reconciled=reconciled)
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
    config = tiktok_ads_config(binding)
    request = request_plan.request
    return HttpRequest(
        method=request.method,
        url=join_url(config, request.path),
        query=request.query,
        headers={**dict(request.headers), **dict(_tiktok_headers(resolved_auth))},
        json_body=request.json_body,
    )


def _submit_dmp_update(
    *,
    binding: DestinationBinding,
    transport: HttpTransport,
    request_plan: RequestBatchPlan,
    resolved_auth: object,
) -> ResponseClassification:
    config = tiktok_ads_config(binding)
    body = request_plan.request.json_body
    if not isinstance(body, Mapping):
        raise ValueError("TikTok Ads DMP update plans require a JSON object body.")
    calculate_type = _required_body_string(body, "calculate_type")
    values = _required_body_string_list(body, "identifiers")
    file_path = _upload_audience_file(
        transport=transport,
        config=config,
        resolved_auth=resolved_auth,
        calculate_type=calculate_type,
        values=values,
        filename=f"retl_tiktok_ads_{request_plan.batch_id}.txt",
    )
    update_body = {
        "advertiser_id": _required_body_string(body, "advertiser_id"),
        "custom_audience_id": _required_body_string(body, "custom_audience_id"),
        "file_paths": [file_path],
        "calculate_type": calculate_type,
        "action": _required_body_string(body, "action"),
    }
    response = transport.send(
        HttpRequest(
            method=request_plan.request.method,
            url=join_url(config, request_plan.request.path),
            headers={**dict(request_plan.request.headers), **dict(_tiktok_headers(resolved_auth))},
            json_body=update_body,
        )
    )
    return classify_tiktok_ads_response(response)


def _upload_audience_file(
    *,
    transport: HttpTransport,
    config: object,
    resolved_auth: object,
    calculate_type: str,
    values: Sequence[str],
    filename: str,
) -> str:
    from retl_tiktok_ads.common import TikTokAdsConfig

    if not isinstance(config, TikTokAdsConfig):
        raise TypeError("TikTok Ads file upload requires TikTokAdsConfig.")
    content = ("\n".join(values) + "\n").encode("utf-8")
    upload = getattr(transport, "upload_audience_file", None)
    if not callable(upload):
        upload = RequestsTikTokAdsTransport().upload_audience_file
    response = upload(
        url=join_url(config, f"/open_api/{config.api_version}/dmp/custom_audience/file/upload/"),
        headers=_tiktok_headers(resolved_auth),
        advertiser_id=config.advertiser_id,
        calculate_type=calculate_type,
        filename=filename,
        content=content,
    )
    classification = classify_tiktok_ads_response(response)
    if classification.outcome not in {"confirmed", "accepted"}:
        detail = classification.partner_message or "TikTok Ads audience file upload failed."
        raise DeclarationValidationError(detail)
    file_path = _uploaded_file_path(response)
    if file_path is None:
        raise DeclarationValidationError(
            "TikTok Ads audience file upload did not return file path."
        )
    return file_path


def _target_transport(binding: DestinationBinding) -> HttpTransport:
    transport = transport_from_config(binding.config)
    if transport is None:
        raise DeclarationValidationError("TikTok Ads managed targets require an HTTP transport.")
    return transport


def _required_body_string(body: Mapping[str, object], field_name: str) -> str:
    value = body.get(field_name)
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"TikTok Ads DMP request body requires `{field_name}`.")


def _required_body_string_list(body: Mapping[str, object], field_name: str) -> tuple[str, ...]:
    value = body.get(field_name)
    if not isinstance(value, list | tuple):
        raise ValueError(f"TikTok Ads DMP request body requires `{field_name}` list.")
    values = tuple(item.strip() for item in value if isinstance(item, str) and item.strip())
    if not values:
        raise ValueError(f"TikTok Ads DMP request body requires non-empty `{field_name}` list.")
    return values


def _tiktok_headers(resolved_auth: object) -> Mapping[str, str]:
    headers = getattr(resolved_auth, "headers", {})
    if not isinstance(headers, Mapping):
        return {}
    return {str(key): str(value) for key, value in headers.items()}


def _raise_for_target_response(response: HttpResponse, *, action: str) -> None:
    classification = classify_tiktok_ads_response(response)
    if classification.outcome in {"confirmed", "accepted"}:
        return
    detail = classification.partner_message or f"TikTok Ads Custom Audience {action} failed."
    raise DeclarationValidationError(detail)


def _dmp_update_body(context: RequestBatchContext) -> JSONValue:
    config = context.public_config
    action = _context_action(context)
    items = [
        (id_type, value)
        for record in context.records
        for id_type, value in _dmp_update_items(
            record,
            mobile_id_type=cast(str, config["mobile_advertising_id_type"]),
        )
    ]
    calculate_types = {id_type for id_type, _ in items}
    if len(calculate_types) != 1:
        raise ValueError("TikTok Ads DMP file batches must contain one calculate_type.")
    return {
        "advertiser_id": config["advertiser_id"],
        "custom_audience_id": context.target,
        "calculate_type": next(iter(calculate_types)),
        "action": action,
        "identifiers": [value for _, value in items],
    }


def _context_action(context: RequestBatchContext) -> str:
    if context.operation == "upsert":
        return DMP_UPDATE_ACTIONS["upsert"]
    if context.operation == "remove":
        return DMP_UPDATE_ACTIONS["remove"]
    raise ValueError("TikTok Ads DMP file batches must contain one operation.")


def _dmp_update_request_item_counts(page: pa.RecordBatch) -> pa.Array:
    identifiers_column = _identifiers_column(page)
    if identifiers_column is None:
        return pa.array([0] * page.num_rows, type=pa.int64())
    return pa.array(
        [
            _dmp_update_item_count(_identifier_items_from_value(scalar.as_py()))
            for scalar in identifiers_column
        ],
        type=pa.int64(),
    )


def _dmp_update_partition(record: DestinationWorkRecord) -> object:
    id_type = _preferred_id_type(_identifier_items(record), mobile_id_type="MAID_SHA256")
    return (record.target, record.operation, id_type)


def _dmp_update_items(
    record: DestinationWorkRecord,
    *,
    mobile_id_type: str,
) -> list[tuple[str, str]]:
    if record.target is None:
        raise ValueError("TikTok Ads Custom Audience records require a Target audience id.")
    id_type = _preferred_id_type(_identifier_items(record), mobile_id_type=mobile_id_type)
    identifier_type = _identifier_type_for_id_type(id_type)
    return [
        (id_type, tiktok_identifier_value(identifier_type, value))
        for value in _values_for_identifier_type(_identifier_items(record), identifier_type)
    ]


def _dmp_update_item_count(identifiers: tuple[Mapping[str, object], ...]) -> int:
    try:
        id_type = _preferred_id_type(identifiers, mobile_id_type="MAID_SHA256")
    except ValueError:
        return 0
    identifier_type = _identifier_type_for_id_type(id_type)
    return len(_values_for_identifier_type(identifiers, identifier_type))


def _preferred_id_type(
    identifiers: tuple[Mapping[str, object], ...],
    *,
    mobile_id_type: str,
) -> str:
    for identifier_type in ("email", "phone_e164", "mobile_advertising_id"):
        if _values_for_identifier_type(identifiers, identifier_type):
            if identifier_type == "mobile_advertising_id":
                return mobile_id_type
            return IDENTIFIER_ID_TYPES[identifier_type]
    raise ValueError("TikTok Ads Custom Audience records require at least one accepted identifier.")


def _identifier_type_for_id_type(id_type: str) -> str:
    if id_type == "EMAIL_SHA256":
        return "email"
    if id_type == "PHONE_SHA256":
        return "phone_e164"
    if id_type in {"IDFA_SHA256", "GAID_SHA256", "MAID_SHA256"}:
        return "mobile_advertising_id"
    raise ValueError(f"Unsupported TikTok Ads DMP calculate_type `{id_type}`.")


def _managed_target_seed_identifier(logical_target: str) -> str:
    return tiktok_identifier_value(
        "email",
        f"retl-tiktok-ads-managed-target-{logical_target}@example.test",
    )


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
        raise ValueError("TikTok Ads Custom Audience records require a Target audience id.")
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
    data = response.json_body.get("data")
    if isinstance(data, Mapping):
        for key in ("list", "audiences", "custom_audiences"):
            rows = data.get(key)
            if isinstance(rows, list):
                return tuple(
                    cast(Mapping[str, object], item) for item in rows if isinstance(item, Mapping)
                )
    return ()


def _has_next_page(response: HttpResponse, *, page: int) -> bool:
    data = response.json_body.get("data")
    if not isinstance(data, Mapping):
        return False
    page_info = data.get("page_info")
    if not isinstance(page_info, Mapping):
        return False
    total_page = page_info.get("total_page")
    if isinstance(total_page, int) and not isinstance(total_page, bool):
        return page < total_page
    return False


def _audience_metadata(audience: Mapping[str, object]) -> Mapping[str, JSONValue]:
    metadata: dict[str, JSONValue] = {"kind": "tiktok_custom_audience"}
    for key in ("audience_type", "audience_sub_type", "status", "is_creator"):
        value = audience.get(key)
        if isinstance(value, str | bool | int | float):
            metadata[key] = cast(JSONValue, value)
    return metadata


def _created_audience_id(response: HttpResponse) -> str | None:
    data = response.json_body.get("data")
    if isinstance(data, Mapping):
        for key in ("custom_audience_id", "audience_id"):
            value = data.get(key)
            if value is not None and str(value).strip():
                return str(value)
    for key in ("custom_audience_id", "audience_id"):
        value = response.json_body.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def _uploaded_file_path(response: HttpResponse) -> str | None:
    data = response.json_body.get("data")
    if isinstance(data, Mapping):
        for key in ("file_path", "file_paths", "path"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, list) and value:
                first = value[0]
                if isinstance(first, str) and first.strip():
                    return first.strip()
        files = data.get("files")
        if isinstance(files, list) and files:
            first = files[0]
            if isinstance(first, Mapping):
                value = first.get("file_path") or first.get("path")
                if isinstance(value, str) and value.strip():
                    return value.strip()
    for key in ("file_path", "path"):
        value = response.json_body.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _remote_handle(response: HttpResponse) -> RemoteHandle | None:
    request_id = response.json_body.get("request_id")
    if isinstance(request_id, str) and request_id.strip():
        return RemoteHandle(kind="tiktok_request", value=request_id)
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
            partner_error_detail=first_failure.partner_error_detail,
            summary=first_failure.partner_message or "TikTok Ads request failed before acceptance.",
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
            partner_error_detail=first_failure.partner_error_detail,
            summary=first_failure.partner_message or "TikTok Ads request failed retryably.",
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
            partner_error_detail=first_failure.partner_error_detail,
            summary=first_failure.partner_message or "TikTok Ads request failed terminally.",
        )
    return DestinationSubmissionEvidence(
        status=_successful_status(accepted_count=accepted_count),
        attempted_count=attempted_count,
        confirmed_count=confirmed_count,
        accepted_count=accepted_count,
        receipts=receipts,
        remote_handles=handles,
        request_batch_count=request_batch_count,
        summary=(
            "TikTok Ads DMP Custom Audience file requests were accepted."
            if delivery_outcome == "accepted"
            else "TikTok Ads returned acceptance-only evidence."
        ),
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
        summary=f"TikTok Ads transport failed before response: {type(error).__name__}.",
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
    raise ValueError(f"No TikTok Ads response classification with outcome `{outcome}`.")


def _successful_status(*, accepted_count: int) -> Literal["confirmed", "accepted"]:
    return "accepted" if accepted_count else "confirmed"
