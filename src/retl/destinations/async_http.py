from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol, TypeAlias
from urllib import parse

from retl.declarations import JSONValue
from retl.destinations.acknowledgements import RemoteHandle
from retl.destinations.http import is_sensitive_evidence_name
from retl.destinations.receipts import (
    DestinationHttpResponse,
    ResponseClassification,
    ResponseClassificationPolicy,
    classify_response,
    retry_after_seconds,
    sanitize_partner_message,
)
from retl.destinations.surfaces import DeliveryOutcome

PollOutcome: TypeAlias = Literal[
    "pending",
    "confirmed",
    "retryable_failure",
    "terminal_failure",
]

_DEFAULT_PENDING_VALUES = ("pending", "running", "in_progress", "accepted", "queued")
_DEFAULT_CONFIRMED_VALUES = (
    "confirmed",
    "succeeded",
    "success",
    "complete",
    "done",
)
_DEFAULT_RETRYABLE_VALUES = ("retryable", "failed_retryable", "rate_limited")
_DEFAULT_TERMINAL_VALUES = ("terminal", "failed_terminal", "failed", "error", "invalid")


@dataclass(frozen=True)
class DestinationHttpRequest:
    """Minimal assumed active HTTP request shape until `destinations.http` lands."""

    method: str
    url: str
    path: str
    headers: Mapping[str, str] = field(default_factory=dict)
    query: Mapping[str, str] = field(default_factory=dict)
    json_body: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        method = self.method.upper()
        if method not in {"DELETE", "GET", "PATCH", "POST", "PUT"}:
            raise ValueError(f"Unsupported HTTP method `{self.method}`.")
        if not self.url.strip():
            raise ValueError("HTTP request `url` must be a non-empty string.")
        if not self.path.strip():
            raise ValueError("HTTP request `path` must be a non-empty string.")
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "headers", dict(self.headers))
        object.__setattr__(self, "query", dict(self.query))


class DestinationHttpTransport(Protocol):
    def send(self, request: DestinationHttpRequest) -> DestinationHttpResponse: ...


@dataclass(frozen=True)
class AsyncSubmitResult:
    classification: ResponseClassification
    remote_handle: RemoteHandle | None
    resume_payload: Mapping[str, JSONValue]


@dataclass(frozen=True)
class AsyncPollRequestSpec:
    method: str = "GET"
    path: str = "/jobs/{remote_handle_value}"
    query: Mapping[str, str] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        method = self.method.upper()
        if method not in {"DELETE", "GET", "PATCH", "POST", "PUT"}:
            raise ValueError(f"Unsupported async poll HTTP method `{self.method}`.")
        if not self.path.strip():
            raise ValueError("Async poll request path must be a non-empty string.")
        _reject_auth_material(self.headers, field_name="headers")
        _reject_auth_material(self.query, field_name="query")
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "headers", dict(self.headers))
        object.__setattr__(self, "query", dict(self.query))


@dataclass(frozen=True)
class AsyncPollClassificationPolicy:
    status_path: Sequence[str] = ("status",)
    pending_values: Sequence[str] = _DEFAULT_PENDING_VALUES
    confirmed_values: Sequence[str] = _DEFAULT_CONFIRMED_VALUES
    retryable_failure_values: Sequence[str] = _DEFAULT_RETRYABLE_VALUES
    terminal_failure_values: Sequence[str] = _DEFAULT_TERMINAL_VALUES
    partner_message_path: Sequence[str] = ("error", "message")
    retryable_statuses: frozenset[int] = field(
        default_factory=lambda: frozenset({429, *range(500, 600), 599})
    )
    default_retry_after_seconds: int | None = None

    def __post_init__(self) -> None:
        for label in ("status_path", "partner_message_path"):
            path = tuple(getattr(self, label))
            if not path or any(not part.strip() for part in path):
                raise ValueError(f"Async poll policy `{label}` must contain non-empty parts.")
            object.__setattr__(self, label, path)
        for label in (
            "pending_values",
            "confirmed_values",
            "retryable_failure_values",
            "terminal_failure_values",
        ):
            values = tuple(str(value).lower() for value in getattr(self, label))
            if any(not value.strip() for value in values):
                raise ValueError(f"Async poll policy `{label}` cannot contain empty values.")
            object.__setattr__(self, label, values)
        if self.default_retry_after_seconds is not None and self.default_retry_after_seconds < 1:
            raise ValueError("Async poll default retry-after seconds must be positive.")


@dataclass(frozen=True)
class AsyncPollClassification:
    outcome: PollOutcome
    remote_handle: RemoteHandle
    raw_status: str | None = None
    partner_message: str | None = None
    retry_after_seconds: int | None = None


@dataclass(frozen=True)
class AsyncFinalizationDecision:
    delivery_outcome: DeliveryOutcome
    progress_outcome: Literal["accepted", "succeeded"] | None
    progress_allowed: bool
    reason: str


def classify_async_submit_response(
    response: DestinationHttpResponse,
    *,
    policy: ResponseClassificationPolicy,
) -> AsyncSubmitResult:
    classification = classify_response(response, policy=policy)
    handle = classification.remote_handle
    resume_payload: Mapping[str, JSONValue] = {}
    if handle is not None:
        resume_payload = {
            "remote_handle_kind": handle.kind,
            "remote_handle_value": handle.value,
        }
        if classification.retry_after_seconds is not None:
            resume_payload = {
                **resume_payload,
                "retry_after_seconds": classification.retry_after_seconds,
            }
    return AsyncSubmitResult(
        classification=classification,
        remote_handle=handle,
        resume_payload=resume_payload,
    )


def render_poll_request(
    *,
    base_url: str,
    remote_handle: RemoteHandle,
    resume_payload: Mapping[str, JSONValue] | None = None,
    spec: AsyncPollRequestSpec | None = None,
) -> DestinationHttpRequest:
    spec = spec or AsyncPollRequestSpec()
    payload = dict(resume_payload or {})
    template_values = {
        "remote_handle_kind": remote_handle.kind,
        "remote_handle_value": remote_handle.value,
        **{key: str(value) for key, value in payload.items() if _format_value(value) is not None},
    }
    path = spec.path.format(**template_values)
    query = {key: value.format(**template_values) for key, value in spec.query.items()}
    return DestinationHttpRequest(
        method=spec.method,
        url=_join_http_url(base_url, path, query),
        path=path,
        headers=spec.headers,
        query=query,
    )


def classify_poll_response(
    response: DestinationHttpResponse,
    *,
    remote_handle: RemoteHandle,
    policy: AsyncPollClassificationPolicy | None = None,
) -> AsyncPollClassification:
    policy = policy or AsyncPollClassificationPolicy()
    retry_after = retry_after_seconds(response) or policy.default_retry_after_seconds
    if response.status_code in policy.retryable_statuses:
        return AsyncPollClassification(
            outcome="retryable_failure",
            remote_handle=remote_handle,
            partner_message=(
                _poll_message(response, policy=policy) or "Async poll failed retryably."
            ),
            retry_after_seconds=retry_after,
        )
    raw_value = _string_at_path(response.json_body, policy.status_path)
    if raw_value is None:
        return AsyncPollClassification(
            outcome="retryable_failure",
            remote_handle=remote_handle,
            partner_message="Async poll response did not include status.",
            retry_after_seconds=retry_after,
        )
    normalized = raw_value.lower()
    if normalized in policy.confirmed_values:
        outcome: PollOutcome = "confirmed"
    elif normalized in policy.pending_values:
        outcome = "pending"
    elif normalized in policy.retryable_failure_values:
        outcome = "retryable_failure"
    elif normalized in policy.terminal_failure_values:
        outcome = "terminal_failure"
    else:
        outcome = "retryable_failure"
    return AsyncPollClassification(
        outcome=outcome,
        remote_handle=remote_handle,
        raw_status=raw_value,
        partner_message=_poll_message(response, policy=policy),
        retry_after_seconds=retry_after if outcome in {"pending", "retryable_failure"} else None,
    )


def finalize_async_progress(
    *,
    delivery_outcome: DeliveryOutcome,
    submit: ResponseClassification,
    poll: AsyncPollClassification | None = None,
) -> AsyncFinalizationDecision:
    if delivery_outcome == "accepted":
        if submit.outcome in {"accepted", "confirmed"}:
            progress_outcome: Literal["accepted", "succeeded"]
            progress_outcome = "accepted" if submit.outcome == "accepted" else "succeeded"
            return AsyncFinalizationDecision(
                delivery_outcome=delivery_outcome,
                progress_outcome=progress_outcome,
                progress_allowed=True,
                reason="Accepted delivery outcome is satisfied by submit acceptance evidence.",
            )
        return AsyncFinalizationDecision(
            delivery_outcome=delivery_outcome,
            progress_outcome=None,
            progress_allowed=False,
            reason="Accepted delivery outcome requires accepted or succeeded submit evidence.",
        )
    if delivery_outcome != "succeeded":
        raise ValueError("Delivery outcome must be either 'accepted' or 'succeeded'.")
    if submit.outcome == "confirmed" or (poll is not None and poll.outcome == "confirmed"):
        return AsyncFinalizationDecision(
            delivery_outcome=delivery_outcome,
            progress_outcome="succeeded",
            progress_allowed=True,
            reason="Succeeded delivery outcome is satisfied by final async confirmation evidence.",
        )
    return AsyncFinalizationDecision(
        delivery_outcome=delivery_outcome,
        progress_outcome=None,
        progress_allowed=False,
        reason="Succeeded delivery outcome requires final async confirmation evidence.",
    )


def _poll_message(
    response: DestinationHttpResponse,
    *,
    policy: AsyncPollClassificationPolicy,
) -> str | None:
    raw_message = _string_at_path(response.json_body, policy.partner_message_path)
    return sanitize_partner_message(raw_message)


def _string_at_path(payload: Mapping[str, object], path: Sequence[str]) -> str | None:
    current: object | None = payload
    for part in path:
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current if isinstance(current, str) else None


def _join_http_url(base_url: str, path: str, query: Mapping[str, str]) -> str:
    normalized_base = base_url.rstrip("/")
    normalized_path = path if path.startswith("/") else f"/{path}"
    url = f"{normalized_base}{normalized_path}"
    if query:
        return f"{url}?{parse.urlencode(sorted(query.items()))}"
    return url


def _reject_auth_material(values: Mapping[str, str], *, field_name: str) -> None:
    forbidden = sorted(name for name in values if is_sensitive_evidence_name(name))
    if forbidden:
        raise ValueError(f"Async poll request {field_name} must not include auth material.")


def _format_value(value: JSONValue) -> str | None:
    if isinstance(value, str | int | float | bool):
        return str(value)
    return None


__all__ = [
    "AsyncFinalizationDecision",
    "AsyncPollClassification",
    "AsyncPollClassificationPolicy",
    "AsyncPollRequestSpec",
    "AsyncSubmitResult",
    "DestinationHttpRequest",
    "DestinationHttpTransport",
    "PollOutcome",
    "classify_async_submit_response",
    "classify_poll_response",
    "finalize_async_progress",
    "render_poll_request",
]
