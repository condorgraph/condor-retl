from __future__ import annotations

import email.utils
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Protocol, TypeAlias

from retl.destinations.acknowledgements import (
    DestinationReceipt,
    DestinationSubmissionEvidence,
    PreAcceptanceFailureCategory,
    RemoteHandle,
    SubmissionStatus,
)

ResponseOutcome: TypeAlias = Literal[
    "confirmed",
    "accepted",
    "retryable_failure",
    "terminal_record_failure",
    "pre_acceptance_failure",
]

PartnerMessageExtractor = Callable[["DestinationHttpResponse"], str | None]
PartnerErrorDetailExtractor = Callable[["DestinationHttpResponse"], str | None]
RemoteHandleExtractor = Callable[["DestinationHttpResponse"], str | None]
MAX_PARTNER_ERROR_DETAIL = 4096

_DEFAULT_RETRYABLE_STATUSES = frozenset({408, 425, 429, 599, *range(500, 600)})
_SECRET_KEY_PATTERN = (
    r"authorization|access[_-]?token|api[_-]?key|client[_-]?secret|"
    r"private[_-]?key|password|secret|token"
)
_SECRET_PATTERNS = (
    re.compile(rf"(?i)(?P<key>{_SECRET_KEY_PATTERN})\s*[:=]\s*bearer\s+[a-z0-9._~+/=-]+"),
    re.compile(
        rf"(?i)[\"']?(?P<key>{_SECRET_KEY_PATTERN})[\"']?\s*[:=]\s*"
        r"[\"']?[^\s,;}\"]+[\"']?"
    ),
    re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/=-]+"),
)


class DestinationHttpResponse(Protocol):
    """Minimal assumed active HTTP response shape until `destinations.http` lands."""

    @property
    def status_code(self) -> int: ...

    @property
    def headers(self) -> Mapping[str, str]: ...

    @property
    def json_body(self) -> Mapping[str, object]: ...

    @property
    def body_text(self) -> str | None: ...


@dataclass(frozen=True)
class RemoteHandlePolicy:
    kind: str
    value_path: Sequence[str] = ("id",)
    value: RemoteHandleExtractor | None = None

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("Remote Handle policy `kind` must be a non-empty string.")
        path = tuple(self.value_path)
        if not path and self.value is None:
            raise ValueError("Remote Handle policy requires a value path or extractor.")
        if any(not part.strip() for part in path):
            raise ValueError("Remote Handle policy `value_path` cannot contain empty parts.")
        object.__setattr__(self, "value_path", path)


@dataclass(frozen=True)
class ResponseClassificationPolicy:
    error_code_prefix: str
    confirmed_statuses: frozenset[int] = field(default_factory=lambda: frozenset(range(200, 300)))
    accepted_statuses: frozenset[int] = field(default_factory=lambda: frozenset({202}))
    retryable_statuses: frozenset[int] = field(default_factory=lambda: _DEFAULT_RETRYABLE_STATUSES)
    terminal_statuses: frozenset[int] = field(default_factory=frozenset)
    auth_failure_statuses: frozenset[int] = field(
        default_factory=lambda: frozenset({401, 403, 407})
    )
    schema_failure_statuses: frozenset[int] = field(default_factory=frozenset)
    rate_limit_statuses: frozenset[int] = field(default_factory=frozenset)
    partner_message_path: Sequence[str] = ("error", "message")
    partner_message: PartnerMessageExtractor | None = None
    partner_error_detail: PartnerErrorDetailExtractor | None = None
    retry_after_path: Sequence[str] = ("retry_after_seconds",)
    remote_handle: RemoteHandlePolicy | None = None
    default_retry_after_seconds: int | None = None

    def __post_init__(self) -> None:
        if not self.error_code_prefix.strip():
            raise ValueError("Response classification policy requires an error code prefix.")
        for label, path in {
            "partner_message_path": self.partner_message_path,
            "retry_after_path": self.retry_after_path,
        }.items():
            normalized = tuple(path)
            if any(not part.strip() for part in normalized):
                raise ValueError(f"Response classification policy `{label}` has an empty part.")
            object.__setattr__(self, label, normalized)
        if self.default_retry_after_seconds is not None and self.default_retry_after_seconds < 1:
            raise ValueError("Default retry-after seconds must be positive.")


@dataclass(frozen=True)
class ResponseClassification:
    outcome: ResponseOutcome
    status_code: int
    error_code: str | None = None
    partner_error_code: str | None = None
    partner_error_subcode: str | None = None
    partner_message: str | None = None
    partner_error_detail: str | None = None
    retry_after_seconds: int | None = None
    pre_acceptance_failure_category: PreAcceptanceFailureCategory | None = None
    remote_handle: RemoteHandle | None = None

    @property
    def submission_status(self) -> SubmissionStatus:
        return self.outcome


def classify_response(
    response: DestinationHttpResponse,
    *,
    policy: ResponseClassificationPolicy,
) -> ResponseClassification:
    status = response.status_code
    retry_after = retry_after_seconds(response, path=policy.retry_after_path)
    message = _partner_message(response, policy=policy)
    detail = _partner_error_detail(response, policy=policy)
    partner_error_code = _partner_error_code(response)
    partner_error_subcode = _partner_error_subcode(response)
    handle = extract_remote_handle(response, policy=policy.remote_handle)

    if status in policy.accepted_statuses:
        return ResponseClassification(
            outcome="accepted",
            status_code=status,
            partner_error_code=partner_error_code,
            partner_error_subcode=partner_error_subcode,
            partner_message=message,
            partner_error_detail=detail,
            retry_after_seconds=retry_after,
            remote_handle=handle,
        )
    if status in policy.confirmed_statuses:
        return ResponseClassification(
            outcome="confirmed",
            status_code=status,
            partner_error_code=partner_error_code,
            partner_error_subcode=partner_error_subcode,
            partner_message=message,
            partner_error_detail=detail,
            retry_after_seconds=retry_after,
            remote_handle=handle,
        )
    if status in policy.auth_failure_statuses:
        return _pre_acceptance(
            status_code=status,
            category="auth",
            code="auth_failed",
            message=message or "HTTP request was not authorized.",
            retry_after=retry_after,
            policy=policy,
            partner_error_code=partner_error_code,
            partner_error_subcode=partner_error_subcode,
            partner_error_detail=detail,
        )
    if status in policy.schema_failure_statuses:
        return _pre_acceptance(
            status_code=status,
            category="schema",
            code="schema_failed",
            message=message or "HTTP request failed destination schema validation.",
            retry_after=retry_after,
            policy=policy,
            partner_error_code=partner_error_code,
            partner_error_subcode=partner_error_subcode,
            partner_error_detail=detail,
        )
    if status in policy.retryable_statuses:
        return ResponseClassification(
            outcome="retryable_failure",
            status_code=status,
            error_code=_error_code(policy, "retryable"),
            partner_error_code=partner_error_code,
            partner_error_subcode=partner_error_subcode,
            partner_message=message or "HTTP request failed retryably.",
            partner_error_detail=detail,
            retry_after_seconds=retry_after or policy.default_retry_after_seconds,
        )
    if status in policy.rate_limit_statuses:
        return ResponseClassification(
            outcome="retryable_failure",
            status_code=status,
            error_code=_error_code(policy, "retryable"),
            partner_error_code=partner_error_code,
            partner_error_subcode=partner_error_subcode,
            partner_message=message or "HTTP request was rate limited.",
            partner_error_detail=detail,
            retry_after_seconds=retry_after or policy.default_retry_after_seconds,
        )
    if status in policy.terminal_statuses or 400 <= status < 500:
        return ResponseClassification(
            outcome="terminal_record_failure",
            status_code=status,
            error_code=_error_code(policy, "terminal"),
            partner_error_code=partner_error_code,
            partner_error_subcode=partner_error_subcode,
            partner_message=message or "HTTP request failed terminally.",
            partner_error_detail=detail,
        )
    return _pre_acceptance(
        status_code=status,
        category="submission",
        code="submission_failed",
        message=message or "HTTP request did not produce accepted or confirmed evidence.",
        retry_after=retry_after,
        policy=policy,
        partner_error_code=partner_error_code,
        partner_error_subcode=partner_error_subcode,
        partner_error_detail=detail,
    )


def submission_evidence_from_classification(
    classification: ResponseClassification,
    *,
    attempted_count: int,
    dry_run: bool = False,
) -> DestinationSubmissionEvidence:
    if classification.outcome == "confirmed":
        receipts = (
            DestinationReceipt(
                status="confirmed",
                count=attempted_count,
                remote_handle=classification.remote_handle,
            ),
        )
        handles = _remote_handles(classification)
        return DestinationSubmissionEvidence(
            status="confirmed",
            attempted_count=attempted_count,
            dry_run=dry_run,
            confirmed_count=attempted_count,
            receipts=receipts,
            remote_handles=handles,
            http_status=classification.status_code,
            partner_error_code=classification.partner_error_code,
            partner_error_subcode=classification.partner_error_subcode,
            partner_error_detail=classification.partner_error_detail,
            retry_after_seconds=classification.retry_after_seconds,
            summary="Destination response confirmed submitted work.",
        )
    if classification.outcome == "accepted":
        receipts = (
            DestinationReceipt(
                status="accepted",
                count=attempted_count,
                remote_handle=classification.remote_handle,
            ),
        )
        handles = _remote_handles(classification)
        return DestinationSubmissionEvidence(
            status="accepted",
            attempted_count=attempted_count,
            dry_run=dry_run,
            accepted_count=attempted_count,
            receipts=receipts,
            remote_handles=handles,
            http_status=classification.status_code,
            partner_error_code=classification.partner_error_code,
            partner_error_subcode=classification.partner_error_subcode,
            partner_error_detail=classification.partner_error_detail,
            retry_after_seconds=classification.retry_after_seconds,
            summary="Destination response accepted submitted work.",
        )
    if classification.outcome == "retryable_failure":
        return DestinationSubmissionEvidence(
            status="retryable_failure",
            attempted_count=attempted_count,
            dry_run=dry_run,
            retryable_failure_count=attempted_count,
            http_status=classification.status_code,
            partner_error_code=classification.partner_error_code,
            partner_error_subcode=classification.partner_error_subcode,
            partner_error_detail=classification.partner_error_detail,
            retry_after_seconds=classification.retry_after_seconds,
            summary=classification.partner_message or "Destination response was retryable.",
        )
    if classification.outcome == "terminal_record_failure":
        return DestinationSubmissionEvidence(
            status="terminal_record_failure",
            attempted_count=attempted_count,
            dry_run=dry_run,
            terminal_record_failure_count=attempted_count,
            http_status=classification.status_code,
            partner_error_code=classification.partner_error_code,
            partner_error_subcode=classification.partner_error_subcode,
            partner_error_detail=classification.partner_error_detail,
            retry_after_seconds=classification.retry_after_seconds,
            summary=classification.partner_message or "Destination response was terminal.",
        )
    return DestinationSubmissionEvidence(
        status="pre_acceptance_failure",
        attempted_count=attempted_count,
        dry_run=dry_run,
        pre_acceptance_failure_count=attempted_count,
        pre_acceptance_failure_category=classification.pre_acceptance_failure_category,
        http_status=classification.status_code,
        partner_error_code=classification.partner_error_code,
        partner_error_subcode=classification.partner_error_subcode,
        partner_error_detail=classification.partner_error_detail,
        retry_after_seconds=classification.retry_after_seconds,
        summary=classification.partner_message or "Destination response failed before acceptance.",
    )


def sanitize_partner_message(message: str | None, *, max_length: int = 256) -> str | None:
    if message is None:
        return None
    collapsed = " ".join(str(message).split())
    if not collapsed:
        return None
    redacted = collapsed
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub(_redacted_secret_match, redacted)
    return redacted[:max_length]


def sanitize_partner_error_detail(detail: str | None) -> str | None:
    return sanitize_partner_message(detail, max_length=MAX_PARTNER_ERROR_DETAIL)


def retry_after_seconds(
    response: DestinationHttpResponse,
    *,
    path: Sequence[str] = ("retry_after_seconds",),
    now: datetime | None = None,
) -> int | None:
    header_value = _header(response.headers, "retry-after")
    parsed = _parse_retry_after_value(header_value, now=now)
    if parsed is not None:
        return parsed
    body_value = _value_at_path(response.json_body, tuple(path))
    return _positive_int(body_value)


def extract_remote_handle(
    response: DestinationHttpResponse,
    *,
    policy: RemoteHandlePolicy | None,
) -> RemoteHandle | None:
    if policy is None:
        return None
    raw_value = policy.value(response) if policy.value is not None else None
    if raw_value is None:
        value = _value_at_path(response.json_body, policy.value_path)
        raw_value = str(value) if isinstance(value, str | int) else None
    if raw_value is None or not str(raw_value).strip():
        return None
    return RemoteHandle(kind=policy.kind, value=str(raw_value))


def _pre_acceptance(
    *,
    status_code: int,
    category: PreAcceptanceFailureCategory,
    code: str,
    message: str,
    retry_after: int | None,
    policy: ResponseClassificationPolicy,
    partner_error_code: str | None,
    partner_error_subcode: str | None,
    partner_error_detail: str | None,
) -> ResponseClassification:
    return ResponseClassification(
        outcome="pre_acceptance_failure",
        status_code=status_code,
        error_code=_error_code(policy, code),
        partner_error_code=partner_error_code,
        partner_error_subcode=partner_error_subcode,
        partner_message=message,
        partner_error_detail=partner_error_detail or sanitize_partner_error_detail(message),
        retry_after_seconds=retry_after,
        pre_acceptance_failure_category=category,
    )


def _remote_handles(classification: ResponseClassification) -> tuple[RemoteHandle, ...]:
    return (classification.remote_handle,) if classification.remote_handle is not None else ()


def _redacted_secret_match(match: re.Match[str]) -> str:
    key = match.groupdict().get("key")
    if key and key.lower() != "bearer":
        return f"{key}=[redacted]"
    return "[redacted]"


def _partner_message(
    response: DestinationHttpResponse,
    *,
    policy: ResponseClassificationPolicy,
) -> str | None:
    if policy.partner_message is not None:
        return sanitize_partner_message(policy.partner_message(response))
    value = _value_at_path(response.json_body, policy.partner_message_path)
    if isinstance(value, str):
        return sanitize_partner_message(value)
    if response.body_text:
        return sanitize_partner_message(response.body_text)
    return None


def _partner_error_detail(
    response: DestinationHttpResponse,
    *,
    policy: ResponseClassificationPolicy,
) -> str | None:
    if policy.partner_error_detail is None:
        return None
    return sanitize_partner_error_detail(policy.partner_error_detail(response))


def _partner_error_code(response: DestinationHttpResponse) -> str | None:
    return _diagnostic_value(
        _value_at_path(response.json_body, ("error", "code"))
        or _value_at_path(response.json_body, ("code",))
    )


def _partner_error_subcode(response: DestinationHttpResponse) -> str | None:
    return _diagnostic_value(
        _value_at_path(response.json_body, ("error", "error_subcode"))
        or _value_at_path(response.json_body, ("error_subcode",))
        or _value_at_path(response.json_body, ("subcode",))
    )


def _diagnostic_value(value: object | None) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str | int):
        text = str(value).strip()
        return sanitize_partner_message(text, max_length=64)
    return None


def _value_at_path(payload: Mapping[str, object], path: Sequence[str]) -> object | None:
    current: object | None = payload
    for part in path:
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _header(headers: Mapping[str, str], name: str) -> str | None:
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return None


def _parse_retry_after_value(value: str | None, *, now: datetime | None = None) -> int | None:
    if value is None:
        return None
    parsed_int = _positive_int(value)
    if parsed_int is not None:
        return parsed_int
    try:
        parsed_date = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed_date is None:
        return None
    if parsed_date.tzinfo is None:
        parsed_date = parsed_date.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return max(0, int((parsed_date - current).total_seconds()))


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _error_code(policy: ResponseClassificationPolicy, suffix: str) -> str:
    return f"{policy.error_code_prefix}.{suffix}"


__all__ = [
    "DestinationHttpResponse",
    "MAX_PARTNER_ERROR_DETAIL",
    "PartnerErrorDetailExtractor",
    "PartnerMessageExtractor",
    "RemoteHandleExtractor",
    "RemoteHandlePolicy",
    "ResponseClassification",
    "ResponseClassificationPolicy",
    "ResponseOutcome",
    "classify_response",
    "extract_remote_handle",
    "retry_after_seconds",
    "sanitize_partner_error_detail",
    "sanitize_partner_message",
    "submission_evidence_from_classification",
]
