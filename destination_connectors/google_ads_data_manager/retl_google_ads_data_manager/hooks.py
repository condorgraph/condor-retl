from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from time import sleep
from typing import Literal, cast

from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.acknowledgements import (
    DestinationReceipt,
    DestinationSubmissionEvidence,
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
from retl_google_ads_data_manager.common import (
    DATA_MANAGER_API_VERSION,
    GOOGLE_ADS_ACCOUNT_TYPE,
    MAX_AUDIENCE_MEMBERS_PER_REQUEST,
    MAX_EVENTS_PER_REQUEST,
    MAX_USER_IDENTIFIERS_PER_MEMBER,
    GoogleAdsDataManagerConfig,
    google_ads_data_manager_config,
    google_ads_data_manager_partner_error_detail,
    google_ads_data_manager_partner_message,
    hashed_or_normalized_email,
    hashed_or_normalized_phone,
    join_url,
    normalize_consent_status,
    sha256_hex,
    transport_from_config,
)

CUSTOMER_MATCH_SURFACE = "customer_match"
CUSTOMER_MATCH_CONTACT_ID_SURFACE = "customer_match_contact_id"
EVENTS_SURFACE = "events"
CONTACT_ID_UPLOAD_KEY_TYPE = "CONTACT_ID"
CONTACT_ID_MEMBERSHIP_DURATION_DAYS = 540
CONTACT_ID_MEMBERSHIP_DURATION = f"{CONTACT_ID_MEMBERSHIP_DURATION_DAYS * 24 * 60 * 60}s"
CONTACT_ID_DATA_SOURCE_TYPE = "DATA_SOURCE_TYPE_FIRST_PARTY"

GOOGLE_ADS_DATA_MANAGER_RESPONSE_POLICY = ResponseClassificationPolicy(
    error_code_prefix="google_ads_data_manager",
    confirmed_statuses=frozenset(),
    accepted_statuses=frozenset(range(200, 300)),
    schema_failure_statuses=frozenset({400}),
    partner_message=lambda response: google_ads_data_manager_partner_message(
        cast(HttpResponse, response)
    ),
    partner_error_detail=lambda response: google_ads_data_manager_partner_error_detail(
        cast(HttpResponse, response)
    ),
    remote_handle=RemoteHandlePolicy(kind="data_manager_request", value_path=("requestId",)),
    default_retry_after_seconds=60,
)


@dataclass(frozen=True)
class _SubmissionRequestPlans:
    plans: tuple[RequestBatchPlan, ...]
    request_count: int
    record_count: int


@dataclass(frozen=True)
class GoogleAdsDataManagerContactListTargetClient:
    binding: DestinationBinding
    surface: DestinationSurface
    resolved_auth: object

    def find_target(self, logical_target: str) -> RemoteTarget | None:
        if self.surface.name != CUSTOMER_MATCH_CONTACT_ID_SURFACE:
            return None
        config = google_ads_data_manager_config(self.binding)
        transport = _target_transport(self.binding)
        page_token: str | None = None
        seen_pages = 0
        while seen_pages < 100:
            seen_pages += 1
            query: dict[str, str] = {
                "pageSize": "100",
                "filter": _contact_list_filter(logical_target),
            }
            if page_token:
                query["pageToken"] = page_token
            response = transport.send(
                HttpRequest(
                    method="GET",
                    url=_user_lists_url(config),
                    query=query,
                    headers=_target_headers(config=config, resolved_auth=self.resolved_auth),
                )
            )
            _raise_for_target_response(response, action="find")
            for user_list in _user_list_rows(response):
                if not _is_contact_id_user_list(user_list):
                    continue
                if user_list.get("displayName") != logical_target:
                    continue
                remote = _remote_target_from_user_list(user_list, display_name=logical_target)
                if remote is not None:
                    return remote
            raw_next_page_token = response.json_body.get("nextPageToken")
            if not isinstance(raw_next_page_token, str) or not raw_next_page_token.strip():
                return None
            page_token = raw_next_page_token.strip()
        return None

    def create_target(self, logical_target: str, *, display_name: str) -> RemoteTarget:
        if self.surface.name != CUSTOMER_MATCH_CONTACT_ID_SURFACE:
            raise DeclarationValidationError(
                "Google Ads Data Manager managed target creation is only supported "
                "for Customer Match CONTACT_ID lists."
            )
        config = google_ads_data_manager_config(self.binding)
        transport = _target_transport(self.binding)
        response = transport.send(
            HttpRequest(
                method="POST",
                url=_user_lists_url(config),
                headers=_target_headers(config=config, resolved_auth=self.resolved_auth),
                json_body={
                    "displayName": display_name,
                    "description": f"Managed by RETL for `{logical_target}`.",
                    "membershipStatus": "OPEN",
                    "membershipDuration": CONTACT_ID_MEMBERSHIP_DURATION,
                    "integrationCode": _integration_code(
                        self.binding,
                        self.surface,
                        logical_target,
                    ),
                    "ingestedUserListInfo": {
                        "uploadKeyTypes": [CONTACT_ID_UPLOAD_KEY_TYPE],
                        "contactIdInfo": {
                            "dataSourceType": CONTACT_ID_DATA_SOURCE_TYPE,
                        },
                    },
                },
            )
        )
        _raise_for_target_response(response, action="create")
        remote = _remote_target_from_user_list(
            cast(Mapping[str, JSONValue], response.json_body),
            display_name=display_name,
        )
        if remote is None:
            raise DeclarationValidationError(
                "Google Ads Data Manager UserList creation did not return `id`."
            )
        return remote


def google_ads_data_manager_managed_target_client(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    resolved_auth: object,
) -> GoogleAdsDataManagerContactListTargetClient | None:
    if surface.name != CUSTOMER_MATCH_CONTACT_ID_SURFACE:
        return None
    return GoogleAdsDataManagerContactListTargetClient(
        binding=binding,
        surface=surface,
        resolved_auth=resolved_auth,
    )


def submit_google_ads_data_manager_destination(
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
                "Google Ads Data Manager dry run planned "
                f"{request_plans.request_count} request batch(es) for "
                f"{request_plans.record_count} record(s)."
            ),
        )

    request_plan = _single_submission_request_plan(
        plans=request_plans.plans,
        connector_name="Google Ads Data Manager",
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
                "Google Ads Data Manager submission requires an HTTP transport; "
                "no request was sent."
            ),
        )
    if request_plan is None:
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=False,
            request_batch_count=0,
            summary="Google Ads Data Manager submission had no request batch to execute.",
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
    classification = classify_google_ads_data_manager_response(response)
    if surface.name == EVENTS_SURFACE and classification.outcome == "accepted":
        classification = _poll_event_request_status(
            binding=binding,
            transport=transport,
            resolved_auth=resolved_auth,
            classification=classification,
        )
    return _aggregate_submission_evidence(
        [(classification, request_plan.row_count)],
        delivery_outcome=delivery_outcome,
        attempted_count=attempted_count,
        request_batch_count=request_plans.request_count,
    )


def plan_google_ads_data_manager_requests(
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
            notes=("Google Ads Data Manager work is deferred until reconcile produces pages.",),
        )
    config = google_ads_data_manager_config(binding)
    if surface.name == EVENTS_SURFACE:
        return plan_request_batches(
            sync_name=reconciled.sync_name,
            surface_name=surface.name,
            work=work,
            request_template={
                "method": "POST",
                "path": f"/{DATA_MANAGER_API_VERSION}/events:ingest",
            },
            batching_policy=RequestBatchingPolicy(max_rows=MAX_EVENTS_PER_REQUEST),
            public_config=_public_config(config),
            dry_run=True,
            body_hook=_events_body,
            family="event_imports",
        )
    plan = plan_request_batches(
        sync_name=reconciled.sync_name,
        surface_name=surface.name,
        work=work,
        request_template={
            "method": "POST",
            "path": f"/{DATA_MANAGER_API_VERSION}/audienceMembers:{{{{ operation }}}}",
        },
        batching_policy=RequestBatchingPolicy(max_rows=MAX_AUDIENCE_MEMBERS_PER_REQUEST),
        public_config=_public_config(config),
        dry_run=True,
        body_hook=_customer_match_body,
        family="state_operations",
        partition_key=lambda record: (record.target, record.operation),
        record_hook=lambda record: _with_remote_target(
            record,
            binding=binding,
            surface=surface,
        ),
    )
    return _with_customer_match_methods(plan)


def classify_google_ads_data_manager_response(response: HttpResponse) -> ResponseClassification:
    return classify_response(response, policy=GOOGLE_ADS_DATA_MANAGER_RESPONSE_POLICY)


def _poll_event_request_status(
    *,
    binding: DestinationBinding,
    transport: HttpTransport,
    resolved_auth: object,
    classification: ResponseClassification,
) -> ResponseClassification:
    handle = classification.remote_handle
    if handle is None:
        return classification
    config = google_ads_data_manager_config(binding)
    initial_backoff = config.request_status_poll_interval_seconds
    timeout = config.request_status_poll_timeout_seconds
    if initial_backoff <= 0 or timeout <= 0:
        return classification

    elapsed = 0.0
    last_response: HttpResponse | None = None
    for poll_at in _request_status_poll_schedule(
        initial_backoff_seconds=initial_backoff,
        timeout_seconds=timeout,
    ):
        wait_seconds = poll_at - elapsed
        if wait_seconds > 0:
            sleep(wait_seconds)
        elapsed = poll_at
        request = _request_status_request(
            binding=binding,
            request_id=handle.value,
            resolved_auth=resolved_auth,
        )
        try:
            response = transport.send(request)
        except Exception:
            return classification
        last_response = response
        if response.status_code == 404:
            continue
        if response.status_code == 200:
            return _classification_from_event_status_response(
                response=response,
                fallback=classification,
            )
        if response.status_code in {408, 425, 429, 599, *range(500, 600)}:
            return ResponseClassification(
                outcome="accepted",
                status_code=response.status_code,
                partner_message=google_ads_data_manager_partner_message(response),
                partner_error_detail=google_ads_data_manager_partner_error_detail(response),
                remote_handle=handle,
            )
        return classification

    if last_response is not None and last_response.status_code == 404:
        return ResponseClassification(
            outcome="accepted",
            status_code=classification.status_code,
            partner_message=(
                "Google Ads Data Manager request status was not visible within "
                f"{timeout:g} seconds."
            ),
            partner_error_detail=(
                "requestStatus returned 404 NOT_FOUND during the event ingest "
                "visibility polling window."
            ),
            remote_handle=handle,
        )
    return classification


def _request_status_poll_schedule(
    *,
    initial_backoff_seconds: float,
    timeout_seconds: float,
) -> tuple[float, ...]:
    poll_at = initial_backoff_seconds
    schedule: list[float] = []
    while poll_at < timeout_seconds:
        schedule.append(poll_at)
        poll_at *= 2
    if not schedule or schedule[-1] < timeout_seconds:
        schedule.append(timeout_seconds)
    return tuple(schedule)


def _request_status_request(
    *,
    binding: DestinationBinding,
    request_id: str,
    resolved_auth: object,
) -> HttpRequest:
    config = google_ads_data_manager_config(binding)
    auth_headers = getattr(resolved_auth, "headers", {})
    if not isinstance(auth_headers, Mapping):
        auth_headers = {}
    return HttpRequest(
        method="GET",
        url=join_url(config, f"/{DATA_MANAGER_API_VERSION}/requestStatus:retrieve"),
        query={"requestId": request_id},
        headers=cast(Mapping[str, str], auth_headers),
    )


def _classification_from_event_status_response(
    *,
    response: HttpResponse,
    fallback: ResponseClassification,
) -> ResponseClassification:
    statuses = tuple(_request_status_values(response))
    if statuses and all(status == "SUCCESS" for status in statuses):
        return ResponseClassification(
            outcome="accepted",
            status_code=200,
            partner_message="Google Ads Data Manager request status succeeded.",
            remote_handle=fallback.remote_handle,
        )
    if any(status == "FAILED" for status in statuses):
        return ResponseClassification(
            outcome="terminal_record_failure",
            status_code=200,
            partner_message="Google Ads Data Manager request status failed.",
            partner_error_detail=google_ads_data_manager_partner_error_detail(response),
            remote_handle=fallback.remote_handle,
        )
    return ResponseClassification(
        outcome="accepted",
        status_code=200,
        partner_message="Google Ads Data Manager request status is processing.",
        remote_handle=fallback.remote_handle,
    )


def _request_status_values(response: HttpResponse) -> tuple[str, ...]:
    raw_statuses = response.json_body.get("requestStatusPerDestination")
    if not isinstance(raw_statuses, list):
        return ()
    values: list[str] = []
    for item in raw_statuses:
        if isinstance(item, Mapping):
            value = item.get("requestStatus")
            if isinstance(value, str) and value.strip():
                values.append(value.strip().upper())
    return tuple(values)


def _with_customer_match_methods(plan: DryRunSubmissionPlan) -> DryRunSubmissionPlan:
    plans = tuple(
        replace(
            request_plan,
            request=replace(
                request_plan.request,
                path=(
                    f"/{DATA_MANAGER_API_VERSION}/audienceMembers:remove"
                    if request_plan.operation == "remove"
                    else f"/{DATA_MANAGER_API_VERSION}/audienceMembers:ingest"
                ),
            ),
        )
        for request_plan in plan.plans
    )
    return DryRunSubmissionPlan(
        dry_run=plan.dry_run,
        plans=plans,
        record_count=plan.record_count,
        request_count=plan.request_count,
        notes=plan.notes,
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
    plan = plan_google_ads_data_manager_requests(
        binding=binding,
        surface=surface,
        reconciled=reconciled,
    )
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
    config = google_ads_data_manager_config(binding)
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


def _aggregate_submission_evidence(
    classified_batches: list[tuple[ResponseClassification, int]],
    *,
    delivery_outcome: str,
    attempted_count: int,
    request_batch_count: int,
) -> DestinationSubmissionEvidence:
    accepted_count = _count_outcome(classified_batches, "accepted")
    retryable_count = _count_outcome(classified_batches, "retryable_failure")
    terminal_count = _count_outcome(classified_batches, "terminal_record_failure")
    pre_acceptance_count = _count_outcome(classified_batches, "pre_acceptance_failure")
    receipts = tuple(
        DestinationReceipt(
            status="accepted",
            count=row_count,
            remote_handle=classification.remote_handle,
        )
        for classification, row_count in classified_batches
        if classification.outcome == "accepted"
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
            accepted_count=accepted_count,
            pre_acceptance_failure_count=pre_acceptance_count,
            pre_acceptance_failure_category=first_failure.pre_acceptance_failure_category,
            receipts=receipts,
            remote_handles=handles,
            request_batch_count=request_batch_count,
            http_status=first_failure.status_code,
            partner_error_code=first_failure.partner_error_code,
            partner_error_subcode=first_failure.partner_error_subcode,
            partner_error_detail=first_failure.partner_error_detail,
            summary=(
                first_failure.partner_message
                or "Google Ads Data Manager request failed before acceptance."
            ),
        )
    if retryable_count:
        first_failure = _first_outcome(classified_batches, "retryable_failure")
        return DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=attempted_count,
            accepted_count=accepted_count,
            retryable_failure_count=retryable_count,
            receipts=receipts,
            remote_handles=handles,
            request_batch_count=request_batch_count,
            http_status=first_failure.status_code,
            partner_error_code=first_failure.partner_error_code,
            partner_error_subcode=first_failure.partner_error_subcode,
            partner_error_detail=first_failure.partner_error_detail,
            summary=(
                first_failure.partner_message or "Google Ads Data Manager request failed retryably."
            ),
        )
    if terminal_count:
        first_failure = _first_outcome(classified_batches, "terminal_record_failure")
        status: Literal["accepted", "terminal_record_failure"] = (
            "accepted" if accepted_count else "terminal_record_failure"
        )
        return DestinationSubmissionEvidence(
            status=status,
            attempted_count=attempted_count,
            accepted_count=accepted_count,
            terminal_record_failure_count=terminal_count,
            receipts=receipts,
            remote_handles=handles,
            request_batch_count=request_batch_count,
            http_status=first_failure.status_code,
            partner_error_code=first_failure.partner_error_code,
            partner_error_subcode=first_failure.partner_error_subcode,
            partner_error_detail=first_failure.partner_error_detail,
            summary=(
                first_failure.partner_message
                or "Google Ads Data Manager request failed terminally."
            ),
        )
    first_accepted = _first_outcome(classified_batches, "accepted")
    summary = (
        first_accepted.partner_message
        or "Google Ads Data Manager accepted request batches for asynchronous processing."
    )
    if delivery_outcome == "succeeded":
        summary = "Google Ads Data Manager returns acceptance-only request evidence."
    return DestinationSubmissionEvidence(
        status="accepted",
        attempted_count=attempted_count,
        accepted_count=accepted_count,
        receipts=receipts,
        remote_handles=handles,
        request_batch_count=request_batch_count,
        http_status=first_accepted.status_code,
        partner_error_detail=first_accepted.partner_error_detail,
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
        summary=(
            f"Google Ads Data Manager transport failed before response: {type(error).__name__}."
        ),
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
    raise ValueError(f"Google Ads Data Manager did not classify outcome `{outcome}`.")


def _target_transport(binding: DestinationBinding) -> HttpTransport:
    transport = transport_from_config(binding.config)
    if transport is None:
        raise DeclarationValidationError(
            "Google Ads Data Manager managed targets require an HTTP transport."
        )
    return transport


def _target_headers(
    *,
    config: GoogleAdsDataManagerConfig,
    resolved_auth: object,
) -> dict[str, str]:
    headers = dict(_auth_headers(resolved_auth))
    if config.login_account_id is not None:
        headers["login-account"] = _account_resource(
            account_type=config.login_account_type,
            account_id=config.login_account_id,
        )
    if config.linked_account_id is not None and config.linked_account_type is not None:
        headers["linked-account"] = _account_resource(
            account_type=config.linked_account_type,
            account_id=config.linked_account_id,
        )
    return headers


def _auth_headers(resolved_auth: object) -> Mapping[str, str]:
    auth_headers = getattr(resolved_auth, "headers", {})
    if not isinstance(auth_headers, Mapping):
        return {}
    return cast(Mapping[str, str], auth_headers)


def _user_lists_parent(config: GoogleAdsDataManagerConfig) -> str:
    return _account_resource(
        account_type=config.operating_account_type,
        account_id=config.operating_account_id,
    )


def _user_lists_url(config: GoogleAdsDataManagerConfig) -> str:
    return join_url(config, f"/{DATA_MANAGER_API_VERSION}/{_user_lists_parent(config)}/userLists")


def _account_resource(*, account_type: str, account_id: str) -> str:
    return f"accountTypes/{account_type}/accounts/{_bare_account_id(account_id)}"


def _bare_account_id(account_id: str) -> str:
    normalized = account_id.strip()
    if normalized.startswith("customers/"):
        return normalized.removeprefix("customers/")
    return normalized


def _contact_list_filter(logical_target: str) -> str:
    escaped_target = logical_target.replace("\\", "\\\\").replace('"', '\\"')
    return (
        f'display_name = "{escaped_target}" AND '
        f'ingested_user_list_info.upload_key_types = "{CONTACT_ID_UPLOAD_KEY_TYPE}"'
    )


def _integration_code(
    binding: DestinationBinding,
    surface: DestinationSurface,
    logical_target: str,
) -> str:
    return f"retl:{binding.binding_name}:{surface.name}:{sha256_hex(logical_target)[:24]}"


def _user_list_rows(response: HttpResponse) -> tuple[Mapping[str, JSONValue], ...]:
    raw_rows = response.json_body.get("userLists")
    if not isinstance(raw_rows, list):
        return ()
    return tuple(cast(Mapping[str, JSONValue], row) for row in raw_rows if isinstance(row, Mapping))


def _is_contact_id_user_list(user_list: Mapping[str, JSONValue]) -> bool:
    info = user_list.get("ingestedUserListInfo")
    if not isinstance(info, Mapping):
        return False
    upload_key_types = info.get("uploadKeyTypes")
    return isinstance(upload_key_types, list) and CONTACT_ID_UPLOAD_KEY_TYPE in upload_key_types


def _remote_target_from_user_list(
    user_list: Mapping[str, JSONValue],
    *,
    display_name: str,
) -> RemoteTarget | None:
    remote_id = user_list.get("id")
    if not isinstance(remote_id, str) or not remote_id.strip():
        name = user_list.get("name")
        if isinstance(name, str) and "/userLists/" in name:
            remote_id = name.rsplit("/userLists/", 1)[1]
    if not isinstance(remote_id, str) or not remote_id.strip():
        return None
    metadata: dict[str, JSONValue] = {
        "kind": "google_ads_data_manager_user_list",
        "upload_key_type": CONTACT_ID_UPLOAD_KEY_TYPE,
    }
    integration_code = user_list.get("integrationCode")
    if isinstance(integration_code, str) and integration_code.strip():
        metadata["integration_code"] = integration_code
    return RemoteTarget(
        remote_id=remote_id.strip(),
        display_name=display_name,
        metadata=metadata,
    )


def _raise_for_target_response(response: HttpResponse, *, action: str) -> None:
    if 200 <= response.status_code < 300:
        return
    detail = google_ads_data_manager_partner_error_detail(response)
    message = google_ads_data_manager_partner_message(response)
    summary = message or f"Google Ads Data Manager UserList {action} request failed."
    if detail:
        summary = f"{summary} {detail}"
    raise DeclarationValidationError(summary)


def _with_remote_target(
    record: DestinationWorkRecord,
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
) -> DestinationWorkRecord:
    if record.target is None:
        raise ValueError("Google Ads Customer Match records require a Target audience id.")
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


def _customer_match_body(context: RequestBatchContext) -> JSONValue:
    config = _config_from_public(context.public_config)
    audience_members = [_audience_member(record) for record in context.records]
    body: dict[str, JSONValue] = {
        "destinations": [_destination(config, context)],
        "audienceMembers": audience_members,
    }
    if _has_user_data(audience_members):
        body["encoding"] = config.encoding
    if context.operation == "upsert":
        _require_terms_accepted(config, audience_members)
        body["termsOfService"] = {"customerMatchTermsOfServiceStatus": "ACCEPTED"}
        consent = _request_consent(config)
        if consent:
            body["consent"] = consent
    return body


def _destination(
    config: GoogleAdsDataManagerConfig,
    context: RequestBatchContext,
) -> dict[str, JSONValue]:
    product_destination_id = context.target or config.event_destination_id
    if product_destination_id is None:
        raise ValueError(
            "Google Ads Data Manager requests require a Target or `event_destination_id` config."
        )
    destination: dict[str, JSONValue] = {
        "operatingAccount": {
            "accountId": config.operating_account_id,
            "accountType": config.operating_account_type,
        },
        "productDestinationId": product_destination_id,
    }
    if config.login_account_id is not None:
        destination["loginAccount"] = {
            "accountId": config.login_account_id,
            "accountType": config.login_account_type,
        }
    if config.linked_account_id is not None and config.linked_account_type is not None:
        destination["linkedAccount"] = {
            "accountId": config.linked_account_id,
            "accountType": config.linked_account_type,
        }
    return destination


def _events_body(context: RequestBatchContext) -> JSONValue:
    config = _config_from_public(context.public_config)
    return {
        "destinations": [_destination(config, context)],
        "encoding": config.encoding,
        "events": [_event(record) for record in context.records],
    }


def _event(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    payload = _payload_mapping(record)
    event: dict[str, JSONValue] = {
        "transactionId": _event_transaction_id(record),
        "eventTimestamp": _event_timestamp(record),
        "eventName": _event_name(record),
    }
    for source_key, output_key in (
        ("last_updated_timestamp", "lastUpdatedTimestamp"),
        ("lastUpdatedTimestamp", "lastUpdatedTimestamp"),
        ("currency", "currency"),
        ("event_source", "eventSource"),
        ("eventSource", "eventSource"),
        ("client_id", "clientId"),
        ("clientId", "clientId"),
        ("user_id", "userId"),
        ("userId", "userId"),
        ("app_instance_id", "appInstanceId"),
        ("appInstanceId", "appInstanceId"),
        ("conversion_value", "conversionValue"),
        ("conversionValue", "conversionValue"),
    ):
        value = payload.get(source_key)
        if _present_json(value):
            event[output_key] = cast(JSONValue, value)
    for source_key, output_key in (
        ("consent", "consent"),
        ("ad_identifiers", "adIdentifiers"),
        ("adIdentifiers", "adIdentifiers"),
        ("event_device_info", "eventDeviceInfo"),
        ("eventDeviceInfo", "eventDeviceInfo"),
        ("cart_data", "cartData"),
        ("cartData", "cartData"),
        ("custom_variables", "customVariables"),
        ("customVariables", "customVariables"),
        ("experimental_fields", "experimentalFields"),
        ("experimentalFields", "experimentalFields"),
        ("user_properties", "userProperties"),
        ("userProperties", "userProperties"),
        ("additional_event_parameters", "additionalEventParameters"),
        ("additionalEventParameters", "additionalEventParameters"),
        ("third_party_user_data", "thirdPartyUserData"),
        ("thirdPartyUserData", "thirdPartyUserData"),
        ("event_location", "eventLocation"),
        ("eventLocation", "eventLocation"),
    ):
        value = payload.get(source_key)
        if _present_json(value):
            event[output_key] = cast(JSONValue, value)
    user_data = _event_user_data(record)
    if user_data:
        event["userData"] = user_data
    external_user_id = _event_external_user_id(record)
    if external_user_id is not None and "userId" not in event:
        event["userId"] = external_user_id
    ad_identifiers = _event_ad_identifiers(record)
    if ad_identifiers:
        existing = event.get("adIdentifiers")
        merged = (
            dict(cast(Mapping[str, JSONValue], existing)) if isinstance(existing, Mapping) else {}
        )
        merged.update(ad_identifiers)
        event["adIdentifiers"] = merged
    device_info = _event_device_info(record)
    if device_info:
        existing = event.get("eventDeviceInfo")
        merged = (
            dict(cast(Mapping[str, JSONValue], existing)) if isinstance(existing, Mapping) else {}
        )
        merged.update(device_info)
        event["eventDeviceInfo"] = merged
    return event


def _event_transaction_id(record: DestinationWorkRecord) -> str:
    for value in (
        _payload_value(record, "transactionId"),
        _payload_value(record, "transaction_id"),
        _mapping_value(record.key, "event_id"),
        _mapping_value(record.key, "transaction_id"),
        record.record_identity,
    ):
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise ValueError("Google Data Manager Events require `event_id`.")


def _event_timestamp(record: DestinationWorkRecord) -> str:
    for value in (
        _payload_value(record, "eventTimestamp"),
        _payload_value(record, "event_timestamp"),
        record.occurred_at,
    ):
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise ValueError("Google Data Manager Events require `occurred_at`.")


def _event_name(record: DestinationWorkRecord) -> str:
    for value in (
        record.raw.get("event_name"),
        record.raw.get("eventName"),
        _payload_value(record, "event_name"),
        _payload_value(record, "eventName"),
    ):
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise ValueError("Google Data Manager Events require `event_name`.")


def _event_user_data(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    payload = _payload_mapping(record)
    raw_user_data = payload.get("userData", payload.get("user_data"))
    user_data = (
        dict(cast(Mapping[str, JSONValue], raw_user_data))
        if isinstance(raw_user_data, Mapping)
        else {}
    )
    user_identifiers = list(
        cast(list[JSONValue], user_data.get("userIdentifiers", []))
        if isinstance(user_data.get("userIdentifiers"), list)
        else []
    )
    for identifier in _identifier_items(record):
        raw_type = identifier.get("type")
        raw_value = identifier.get("value")
        if not isinstance(raw_value, str) or not raw_value.strip():
            continue
        if raw_type == "email":
            user_identifiers.append({"emailAddress": hashed_or_normalized_email(raw_value)})
        elif raw_type == "phone_e164":
            user_identifiers.append({"phoneNumber": hashed_or_normalized_phone(raw_value)})
    if user_identifiers:
        user_data["userIdentifiers"] = user_identifiers
    return user_data


def _event_external_user_id(record: DestinationWorkRecord) -> str | None:
    for identifier in _identifier_items(record):
        if identifier.get("type") == "external_id":
            raw_value = identifier.get("value")
            if isinstance(raw_value, str) and raw_value.strip():
                return raw_value.strip()
    return None


def _event_ad_identifiers(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    values: dict[str, JSONValue] = {}
    for source_key, output_key in (
        ("session_attributes", "sessionAttributes"),
        ("sessionAttributes", "sessionAttributes"),
        ("gclid", "gclid"),
        ("gbraid", "gbraid"),
        ("wbraid", "wbraid"),
        ("mobile_device_id", "mobileDeviceId"),
        ("mobileDeviceId", "mobileDeviceId"),
    ):
        value = _payload_value(record, source_key)
        if isinstance(value, str) and value.strip():
            values[output_key] = value.strip()
    return values


def _event_device_info(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    values: dict[str, JSONValue] = {}
    for source_key, output_key in (
        ("user_agent", "userAgent"),
        ("userAgent", "userAgent"),
        ("ip_address", "ipAddress"),
        ("ipAddress", "ipAddress"),
        ("language_code", "languageCode"),
        ("languageCode", "languageCode"),
    ):
        value = _payload_value(record, source_key)
        if isinstance(value, str) and value.strip():
            values[output_key] = value.strip()
    return values


def _audience_member(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    identifiers = _identifier_items(record)
    user_identifiers: list[dict[str, JSONValue]] = []
    mobile_ids: list[str] = []
    user_id: str | None = None
    for identifier in identifiers:
        raw_type = identifier.get("type")
        raw_value = identifier.get("value")
        if raw_type == "email" and isinstance(raw_value, str) and raw_value.strip():
            user_identifiers.append({"emailAddress": hashed_or_normalized_email(raw_value)})
        elif raw_type == "phone_e164" and isinstance(raw_value, str) and raw_value.strip():
            user_identifiers.append({"phoneNumber": hashed_or_normalized_phone(raw_value)})
        elif raw_type == "address" and isinstance(raw_value, Mapping):
            user_identifiers.append({"address": _address_identifier(raw_value)})
        elif (
            raw_type == "mobile_advertising_id" and isinstance(raw_value, str) and raw_value.strip()
        ):
            mobile_ids.append(raw_value.strip())
        elif raw_type == "external_id" and isinstance(raw_value, str) and raw_value.strip():
            if user_id is not None:
                raise ValueError("Google Ads Customer Match accepts one external_id per record.")
            user_id = raw_value.strip()

    data_kinds = sum(bool(value) for value in (user_identifiers, mobile_ids, user_id))
    if data_kinds != 1:
        raise ValueError(
            "Google Ads Customer Match records require exactly one identifier family: "
            "UserData, MobileData, or UserIdData."
        )
    member: dict[str, JSONValue]
    if user_identifiers:
        if len(user_identifiers) > MAX_USER_IDENTIFIERS_PER_MEMBER:
            raise ValueError(
                "Google Ads Customer Match UserData records can contain at most "
                f"{MAX_USER_IDENTIFIERS_PER_MEMBER} user identifiers."
            )
        member = {"userData": {"userIdentifiers": user_identifiers}}
    elif mobile_ids:
        if len(mobile_ids) > MAX_USER_IDENTIFIERS_PER_MEMBER:
            raise ValueError(
                "Google Ads Customer Match MobileData records can contain at most "
                f"{MAX_USER_IDENTIFIERS_PER_MEMBER} mobile ids."
            )
        member = {"mobileData": {"mobileIds": mobile_ids}}
    elif user_id is not None:
        member = {"userIdData": {"userId": user_id}}
    else:
        raise ValueError("Google Ads Customer Match records require an accepted identifier.")

    consent = _member_consent(record)
    if consent:
        member["consent"] = consent
    return member


def _address_identifier(raw_value: Mapping[object, object]) -> dict[str, JSONValue]:
    address: dict[str, JSONValue] = {}
    for source_key, output_key in (
        ("given_name", "givenName"),
        ("family_name", "familyName"),
        ("region_code", "regionCode"),
        ("postal_code", "postalCode"),
    ):
        value = raw_value.get(source_key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                "Google Ads Customer Match address identifiers require "
                "`given_name`, `family_name`, `region_code`, and `postal_code`."
            )
        stripped = value.strip()
        if source_key in {"given_name", "family_name"}:
            address[output_key] = sha256_hex(stripped.lower())
        else:
            address[output_key] = stripped
    return address


def _member_consent(record: DestinationWorkRecord) -> dict[str, JSONValue]:
    payload = _payload_mapping(record)
    consent: dict[str, JSONValue] = {}
    for payload_key, output_key in (
        ("ad_user_data_consent", "adUserData"),
        ("ad_personalization_consent", "adPersonalization"),
    ):
        value = payload.get(payload_key)
        if isinstance(value, str) and value.strip():
            try:
                normalized = normalize_consent_status(value)
            except ValueError as exc:
                raise ValueError(
                    "Google Ads Data Manager member consent values must be "
                    "`CONSENT_GRANTED`, `CONSENT_DENIED`, or `CONSENT_STATUS_UNSPECIFIED`."
                ) from exc
            consent[output_key] = normalized
    return consent


def _request_consent(config: GoogleAdsDataManagerConfig) -> dict[str, JSONValue]:
    consent: dict[str, JSONValue] = {}
    if config.ad_user_data_consent is not None:
        consent["adUserData"] = config.ad_user_data_consent
    if config.ad_personalization_consent is not None:
        consent["adPersonalization"] = config.ad_personalization_consent
    return consent


def _require_terms_accepted(
    config: GoogleAdsDataManagerConfig,
    audience_members: list[dict[str, JSONValue]],
) -> None:
    requires_terms = any(
        "userData" in member or "mobileData" in member for member in audience_members
    )
    if requires_terms and not config.customer_match_terms_accepted:
        raise DeclarationValidationError(
            "Google Ads Customer Match UserData and MobileData uploads require "
            "`customer_match_terms_accepted=True` in destination config."
        )


def _has_user_data(audience_members: list[dict[str, JSONValue]]) -> bool:
    return any("userData" in member for member in audience_members)


def _public_config(config: GoogleAdsDataManagerConfig) -> Mapping[str, JSONValue]:
    values: dict[str, JSONValue] = {
        "operating_account_id": config.operating_account_id,
        "operating_account_type": config.operating_account_type,
        "login_account_type": config.login_account_type,
        "encoding": config.encoding,
        "customer_match_terms_accepted": config.customer_match_terms_accepted,
    }
    for key, value in (
        ("login_account_id", config.login_account_id),
        ("linked_account_id", config.linked_account_id),
        ("linked_account_type", config.linked_account_type),
        ("event_destination_id", config.event_destination_id),
        ("ad_user_data_consent", config.ad_user_data_consent),
        ("ad_personalization_consent", config.ad_personalization_consent),
    ):
        if value is not None:
            values[key] = value
    return values


def _config_from_public(config: Mapping[str, JSONValue]) -> GoogleAdsDataManagerConfig:
    return GoogleAdsDataManagerConfig(
        operating_account_id=cast(str, config["operating_account_id"]),
        operating_account_type=cast(
            str, config.get("operating_account_type", GOOGLE_ADS_ACCOUNT_TYPE)
        ),
        login_account_id=cast(str | None, config.get("login_account_id")),
        login_account_type=cast(str, config.get("login_account_type", GOOGLE_ADS_ACCOUNT_TYPE)),
        linked_account_id=cast(str | None, config.get("linked_account_id")),
        linked_account_type=cast(str | None, config.get("linked_account_type")),
        event_destination_id=cast(str | None, config.get("event_destination_id")),
        encoding=cast(str, config.get("encoding", "HEX")),
        customer_match_terms_accepted=config.get("customer_match_terms_accepted") is True,
        ad_user_data_consent=cast(str | None, config.get("ad_user_data_consent")),
        ad_personalization_consent=cast(str | None, config.get("ad_personalization_consent")),
    )


def _payload_mapping(record: DestinationWorkRecord) -> Mapping[str, JSONValue]:
    if isinstance(record.payload, Mapping):
        return cast(Mapping[str, JSONValue], record.payload)
    return {}


def _payload_value(record: DestinationWorkRecord, key: str) -> JSONValue | None:
    return _payload_mapping(record).get(key)


def _mapping_value(value: JSONValue, key: str) -> JSONValue | None:
    if isinstance(value, Mapping):
        return cast(JSONValue | None, value.get(key))
    return None


def _present_json(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _identifier_items(record: DestinationWorkRecord) -> tuple[Mapping[str, object], ...]:
    if isinstance(record.identifiers, list | tuple):
        return tuple(item for item in record.identifiers if isinstance(item, Mapping))
    return ()


__all__ = [
    "CUSTOMER_MATCH_CONTACT_ID_SURFACE",
    "CUSTOMER_MATCH_SURFACE",
    "EVENTS_SURFACE",
    "classify_google_ads_data_manager_response",
    "google_ads_data_manager_managed_target_client",
    "plan_google_ads_data_manager_requests",
    "submit_google_ads_data_manager_destination",
]
