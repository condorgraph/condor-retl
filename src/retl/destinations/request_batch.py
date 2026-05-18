from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Literal, TypeAlias, cast

import pyarrow as pa  # type: ignore[import-untyped]

from retl.artifacts.arrow_ipc import iter_columnar_batches
from retl.artifacts.columnar import ColumnarArtifactRef
from retl.declarations import JSONValue, SecretLiteral, SecretRef
from retl.destinations.http import is_sensitive_evidence_name
from retl.destinations.operations import EventOperation, StateOperation
from retl.stores.contracts import (
    CanonicalKey,
    CanonicalKeyScalar,
    DestinationScanRange,
    EventKeysetScanPosition,
    ScanPosition,
    StateCurrentSnapshotScanPosition,
    StateOrderedWorkScanPosition,
)

JSONMapping: TypeAlias = Mapping[str, JSONValue]
WorkFamily: TypeAlias = Literal["state_operations", "event_imports"]
RequestBodyHook: TypeAlias = Callable[["RequestBatchContext"], JSONValue]
RequestItemCountsHook: TypeAlias = Callable[[pa.RecordBatch], pa.Array]
RequestPartitionHook: TypeAlias = Callable[["DestinationWorkRecord"], object]
RequestRecordHook: TypeAlias = Callable[["DestinationWorkRecord"], "DestinationWorkRecord"]

_PLACEHOLDER = re.compile(r"{{\s*([A-Za-z_][A-Za-z0-9_.]*)\s*}}")
_MAX_BYTES_CHECK_INTERVAL_ROWS = 100
_MAX_REQUEST_ITEM_COUNT = 2**63 - 1


@dataclass(frozen=True)
class RequestBatchingPolicy:
    max_rows: int = 1000
    max_bytes: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.max_rows, int) or isinstance(self.max_rows, bool):
            raise ValueError("Request batching `max_rows` must be an integer greater than 0.")
        if self.max_rows <= 0:
            raise ValueError("Request batching `max_rows` must be greater than 0.")
        if self.max_bytes is not None and (
            not isinstance(self.max_bytes, int) or isinstance(self.max_bytes, bool)
        ):
            raise ValueError("Request batching `max_bytes` must be an integer when provided.")
        if self.max_bytes is not None and self.max_bytes <= 0:
            raise ValueError("Request batching `max_bytes` must be greater than 0 when provided.")


@dataclass(frozen=True)
class RequestTemplate:
    method: JSONValue
    path: JSONValue
    query: JSONMapping = field(default_factory=dict)
    headers: JSONMapping = field(default_factory=dict)
    json_body: JSONValue | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "query", MappingProxyType(dict(self.query)))
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))


@dataclass(frozen=True)
class PlannedHttpRequest:
    method: str
    path: str
    query: Mapping[str, str] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    json_body: JSONValue | None = None

    def __post_init__(self) -> None:
        method = self.method.upper()
        if not method or not method.isalpha():
            raise ValueError("HTTP request method must contain only letters.")
        if "://" in self.path or not self.path.startswith("/"):
            raise ValueError("HTTP request path must be relative and start with `/`.")
        if "?" in self.path:
            raise ValueError("HTTP request path must not include query parameters.")
        _reject_auth_headers(self.headers)
        _reject_auth_query(self.query)
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "query", MappingProxyType(dict(self.query)))
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))


@dataclass(frozen=True)
class DestinationWorkRecord:
    operation: StateOperation | EventOperation
    record_identity: str
    identifiers: JSONValue
    payload: JSONValue
    key: JSONValue
    collect_id: str | None = None
    sequence_order: int | None = None
    target: str | None = None
    occurred_at: str | None = None
    payload_fingerprint: str | None = None
    source_position: ScanPosition | None = None
    source_lower_bound: ScanPosition | None = None
    raw: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.operation not in ("upsert", "remove", "import"):
            raise ValueError(f"Unsupported destination work operation `{self.operation}`.")
        if not self.record_identity.strip():
            raise ValueError("Destination work records require a non-empty record identity.")
        object.__setattr__(self, "raw", MappingProxyType(dict(self.raw)))

    def to_json(self) -> JSONMapping:
        data: dict[str, JSONValue] = {
            "operation": self.operation,
            "record_identity": self.record_identity,
            "identifiers": self.identifiers,
            "payload": self.payload,
            "key": self.key,
        }
        if self.target is not None:
            data["target"] = self.target
        if self.occurred_at is not None:
            data["occurred_at"] = self.occurred_at
        return MappingProxyType(data)


@dataclass(frozen=True)
class RequestBatchContext:
    sync_name: str
    surface_name: str
    family: WorkFamily
    index: int
    operation: StateOperation | EventOperation | Literal["mixed"]
    records: tuple[DestinationWorkRecord, ...]
    public_config: Mapping[str, JSONValue] = field(default_factory=dict)

    @property
    def row_count(self) -> int:
        return len(self.records)

    @property
    def record_identities(self) -> tuple[str, ...]:
        return tuple(record.record_identity for record in self.records)

    @property
    def target(self) -> str | None:
        targets = {record.target for record in self.records if record.target is not None}
        if len(targets) == 1:
            return next(iter(targets))
        return None

    def template_context(self) -> JSONMapping:
        return MappingProxyType(
            {
                "sync": self.sync_name,
                "surface": self.surface_name,
                "family": self.family,
                "index": self.index,
                "operation": self.operation,
                "http_method": _operation_http_method(self.operation),
                "row_count": self.row_count,
                "target": self.target,
                "config": MappingProxyType(dict(self.public_config)),
            }
        )


@dataclass(frozen=True)
class RequestBatchPlan:
    batch_id: str
    family: WorkFamily
    index: int
    row_count: int
    request_item_count: int
    request_item_counts: tuple[int, ...]
    first_collect_id: str | None
    last_collect_id: str | None
    first_sequence_order: int | None
    last_sequence_order: int | None
    source_range: DestinationScanRange | None
    operation: StateOperation | EventOperation | Literal["mixed"]
    record_identities: tuple[str, ...]
    payload_fingerprint: str
    target_request_fingerprint: str
    request: PlannedHttpRequest


@dataclass(frozen=True)
class DryRunSubmissionPlan:
    dry_run: bool
    plans: tuple[RequestBatchPlan, ...]
    record_count: int
    request_count: int
    notes: tuple[str, ...] = ()


def template_from_mapping(template: Mapping[str, JSONValue]) -> RequestTemplate:
    method = template.get("method")
    path = template.get("path")
    if method is None or path is None:
        raise ValueError("Request template requires `method` and `path`.")
    return RequestTemplate(
        method=method,
        path=path,
        query=_mapping_template(template.get("query"), field_name="query"),
        headers=_mapping_template(template.get("headers"), field_name="headers"),
        json_body=template.get("json", template.get("json_body")),
    )


def plan_request_batches(
    *,
    sync_name: str,
    surface_name: str,
    work: object,
    request_template: RequestTemplate | Mapping[str, JSONValue],
    batching_policy: RequestBatchingPolicy | None = None,
    public_config: Mapping[str, JSONValue] | None = None,
    dry_run: bool = True,
    body_hook: RequestBodyHook | None = None,
    request_item_counts: RequestItemCountsHook | None = None,
    family: WorkFamily | None = None,
    partition_key: RequestPartitionHook | None = None,
    record_hook: RequestRecordHook | None = None,
    event_cursor_kind: str | None = None,
    event_primary_key_kind: str | None = None,
) -> DryRunSubmissionPlan:
    """Plan bounded destination request batches from Arrow operation/import pages."""

    config = MappingProxyType(dict(public_config or {}))
    _reject_secret_refs(config, field_name="public_config")
    policy = batching_policy or RequestBatchingPolicy()
    _validate_executable_request_batching_policy(policy)
    template = (
        request_template
        if isinstance(request_template, RequestTemplate)
        else template_from_mapping(request_template)
    )
    inferred_family = family or _infer_work_family(work)
    if inferred_family == "event_imports":
        event_cursor_kind = event_cursor_kind or _event_checkpoint_kind_from_work(
            work, "event_cursor_kind"
        )
        event_primary_key_kind = event_primary_key_kind or _event_checkpoint_kind_from_work(
            work, "event_primary_key_kind"
        )
    plans: list[RequestBatchPlan] = []
    record_count = 0
    next_index = 0
    pending: list[DestinationWorkRecord] = []
    pending_request_item_counts: list[int] = []
    pending_request_item_count = 0
    pending_partition: object = None
    pending_since_byte_check = 0
    previous_source_position: ScanPosition | None = None

    def build_request(
        records: Sequence[DestinationWorkRecord],
    ) -> tuple[
        RequestBatchContext,
        PlannedHttpRequest,
        int,
    ]:
        context = RequestBatchContext(
            sync_name=sync_name,
            surface_name=surface_name,
            family=inferred_family,
            index=next_index,
            operation=_batch_operation(records),
            records=tuple(records),
            public_config=config,
        )
        body = body_hook(context) if body_hook is not None else _default_json_body(context)
        request = render_request(context=context, template=template, body_hook=None, body=body)
        return context, request, _json_body_size_bytes(request.json_body)

    def append_plan(
        records: Sequence[DestinationWorkRecord],
        item_counts: Sequence[int],
        context: RequestBatchContext,
        request: PlannedHttpRequest,
    ) -> None:
        nonlocal next_index
        fingerprint = payload_fingerprint(
            {
                "request": {
                    "method": request.method,
                    "path": request.path,
                    "query": dict(request.query),
                    "headers": dict(request.headers),
                    "json_body": request.json_body,
                },
                "records": tuple(record.to_json() for record in records),
            }
        )
        plans.append(
            RequestBatchPlan(
                batch_id=_request_batch_id(
                    sync_name=sync_name,
                    surface_name=surface_name,
                    family=inferred_family,
                    index=next_index,
                    records=records,
                    payload_fingerprint=fingerprint,
                ),
                family=inferred_family,
                index=next_index,
                row_count=len(records),
                request_item_count=sum(item_counts),
                request_item_counts=tuple(item_counts),
                first_collect_id=records[0].collect_id,
                last_collect_id=records[-1].collect_id,
                first_sequence_order=records[0].sequence_order,
                last_sequence_order=records[-1].sequence_order,
                source_range=_destination_scan_range(records),
                operation=context.operation,
                record_identities=context.record_identities,
                payload_fingerprint=fingerprint,
                target_request_fingerprint=target_request_fingerprint(
                    request=request,
                    record_identities=context.record_identities,
                    row_count=context.row_count,
                ),
                request=request,
            )
        )
        next_index += 1

    def flush_records(
        records: Sequence[DestinationWorkRecord],
        item_counts: Sequence[int],
    ) -> None:
        if not records:
            return
        context, request, _ = build_request(records)
        append_plan(records, item_counts, context, request)

    def largest_fitting_prefix(records: Sequence[DestinationWorkRecord]) -> int:
        if policy.max_bytes is None:
            return len(records)
        low = 1
        high = len(records)
        largest = 0
        while low <= high:
            mid = (low + high) // 2
            _, _, size_bytes = build_request(records[:mid])
            if size_bytes <= policy.max_bytes:
                largest = mid
                low = mid + 1
            else:
                high = mid - 1
        return largest

    def enforce_max_bytes() -> None:
        nonlocal pending, pending_request_item_counts, pending_request_item_count
        nonlocal pending_since_byte_check
        if policy.max_bytes is None or not pending:
            pending_since_byte_check = 0
            return
        while pending:
            context, request, size_bytes = build_request(pending)
            if size_bytes <= policy.max_bytes:
                pending_since_byte_check = 0
                return
            prefix_size = largest_fitting_prefix(pending)
            if prefix_size == 0:
                prefix_size = 1
            flushing = tuple(pending[:prefix_size])
            flushing_counts = tuple(pending_request_item_counts[:prefix_size])
            if prefix_size == len(pending):
                append_plan(flushing, flushing_counts, context, request)
                pending.clear()
                pending_request_item_counts.clear()
                pending_request_item_count = 0
            else:
                flush_records(flushing, flushing_counts)
                del pending[:prefix_size]
                del pending_request_item_counts[:prefix_size]
                pending_request_item_count = sum(pending_request_item_counts)
            pending_since_byte_check = 0

    def flush() -> None:
        nonlocal pending_request_item_count, pending_since_byte_check
        if not pending:
            pending_since_byte_check = 0
            return
        enforce_max_bytes()
        flush_records(tuple(pending), tuple(pending_request_item_counts))
        pending.clear()
        pending_request_item_counts.clear()
        pending_request_item_count = 0
        pending_since_byte_check = 0

    for page in _iter_work_pages(work):
        page_request_item_counts = _request_item_counts_for_page(
            page,
            request_item_counts=request_item_counts,
        )
        for record, record_request_item_count in zip(
            _records_from_page(
                page,
                family=inferred_family,
                event_cursor_kind=event_cursor_kind,
                event_primary_key_kind=event_primary_key_kind,
            ),
            page_request_item_counts,
            strict=True,
        ):
            if inferred_family == "event_imports":
                record = replace(
                    record,
                    source_lower_bound=previous_source_position or record.source_lower_bound,
                )
                previous_source_position = record.source_position
            if record_hook is not None:
                record = record_hook(record)
            record_count += 1
            if record_request_item_count > policy.max_rows:
                raise ValueError(
                    "Destination request item count for record "
                    f"`{record.record_identity}` exceeds request batching `max_rows` "
                    f"({record_request_item_count} > {policy.max_rows})."
                )
            record_partition = (
                partition_key(record) if partition_key is not None else record.operation
            )
            if pending and (
                pending_request_item_count + record_request_item_count > policy.max_rows
                or record_partition != pending_partition
            ):
                flush()
            pending.append(record)
            pending_request_item_counts.append(record_request_item_count)
            pending_request_item_count += record_request_item_count
            pending_partition = record_partition
            pending_since_byte_check += 1
            if (
                policy.max_bytes is not None
                and pending_since_byte_check >= _MAX_BYTES_CHECK_INTERVAL_ROWS
            ):
                enforce_max_bytes()
    flush()
    return DryRunSubmissionPlan(
        dry_run=dry_run,
        plans=tuple(plans),
        record_count=record_count,
        request_count=len(plans),
        notes=(
            "Request batches were planned from bounded Arrow operation/import pages.",
            "Destination JSON was rendered only at the payload batch boundary.",
        ),
    )


def _validate_executable_request_batching_policy(policy: RequestBatchingPolicy) -> None:
    _ = policy


def _request_item_counts_for_page(
    page: pa.RecordBatch,
    *,
    request_item_counts: RequestItemCountsHook | None,
) -> tuple[int, ...]:
    if request_item_counts is None:
        return (1,) * page.num_rows
    counts = request_item_counts(page)
    if not isinstance(counts, pa.Array):
        raise TypeError("Destination request item count hooks must return a pyarrow.Array.")
    if len(counts) != page.num_rows:
        raise ValueError("Destination request item count hooks must return one count per work row.")
    if counts.null_count:
        raise ValueError("Destination request item count hooks must not return null counts.")
    if pa.types.is_boolean(counts.type) or not pa.types.is_integer(counts.type):
        raise TypeError("Destination request item count hooks must return an integer Arrow array.")
    parsed: list[int] = []
    for scalar in counts:
        value = scalar.as_py()
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError("Destination request item counts must be integers.")
        if value < 0:
            raise ValueError("Destination request item counts must be non-negative.")
        if value > _MAX_REQUEST_ITEM_COUNT:
            raise OverflowError("Destination request item counts must fit a signed 64-bit integer.")
        parsed.append(value)
    return tuple(parsed)


def _json_body_size_bytes(value: JSONValue | None) -> int:
    return len(
        json.dumps(
            _canonicalize(value),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )


def payload_fingerprint(values: object) -> str:
    return _digest(values)


def target_request_fingerprint(
    *,
    request: PlannedHttpRequest,
    record_identities: Sequence[str],
    row_count: int,
) -> str:
    return _digest(
        {
            "method": request.method,
            "path": request.path,
            "query": dict(request.query),
            "headers": dict(request.headers),
            "record_identities": tuple(record_identities),
            "row_count": row_count,
        }
    )


def render_request(
    *,
    context: object,
    template: RequestTemplate | Mapping[str, JSONValue],
    body_hook: RequestBodyHook | None = None,
    body: JSONValue | None = None,
) -> PlannedHttpRequest:
    request_template = (
        template if isinstance(template, RequestTemplate) else template_from_mapping(template)
    )
    if not isinstance(context, RequestBatchContext):
        raise TypeError("Request rendering requires RequestBatchContext.")
    template_context = context.template_context()
    rendered_body = body
    if body_hook is not None:
        rendered_body = body_hook(context)
    elif rendered_body is None and request_template.json_body is not None:
        rendered_body = _expand_template(request_template.json_body, template_context)
    return PlannedHttpRequest(
        method=_render_string(request_template.method, template_context, field_name="method"),
        path=_render_string(request_template.path, template_context, field_name="path"),
        query=_render_string_mapping(request_template.query, template_context),
        headers=_render_string_mapping(request_template.headers, template_context),
        json_body=rendered_body,
    )


def _iter_work_pages(work: object) -> Iterator[pa.RecordBatch]:
    if isinstance(work, pa.RecordBatch):
        yield work
        return
    payload = getattr(work, "payload", None)
    if isinstance(payload, pa.RecordBatch):
        yield payload
        return
    if isinstance(work, ColumnarArtifactRef):
        yield from iter_columnar_batches(work)
        return
    iter_record_batches = getattr(work, "iter_record_batches", None)
    if callable(iter_record_batches):
        for batch in iter_record_batches():
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError("Destination work page iterators must yield pyarrow.RecordBatch.")
            yield batch
        return
    batches = getattr(work, "batches", None)
    if batches is not None:
        for batch in batches:
            if not isinstance(batch, pa.RecordBatch):
                raise TypeError("Destination work `batches` must contain pyarrow.RecordBatch.")
            yield batch
        return
    if isinstance(work, Iterable) and not isinstance(work, Mapping | str | bytes):
        for page in work:
            yield from _iter_work_pages(page)
        return
    raise TypeError("Destination request batching requires bounded Arrow work pages.")


def _infer_work_family(work: object) -> WorkFamily:
    family = getattr(work, "family", None)
    if family in ("state_operations", "event_imports"):
        return cast(WorkFamily, family)
    return "state_operations"


def _event_checkpoint_kind_from_work(work: object, field_name: str) -> str | None:
    value = getattr(work, field_name, None)
    if isinstance(value, str) and value:
        return value
    if isinstance(work, Sequence) and not isinstance(work, str | bytes | bytearray):
        for item in work:
            value = getattr(item, field_name, None)
            if isinstance(value, str) and value:
                return value
    return None


def _records_from_page(
    page: pa.RecordBatch,
    *,
    family: WorkFamily,
    event_cursor_kind: str | None,
    event_primary_key_kind: str | None,
) -> Iterator[DestinationWorkRecord]:
    for row in page.to_pylist():
        if not isinstance(row, Mapping):
            raise TypeError("Destination work pages must contain mapping-like rows.")
        normalized = _json_mapping(row)
        operation = _operation_from_row(normalized, family=family)
        record_identity = _record_identity(normalized)
        yield DestinationWorkRecord(
            operation=operation,
            record_identity=record_identity,
            identifiers=_identifier_value(
                normalized.get(
                    "identifiers",
                    normalized.get(
                        "identifier_values",
                        _canonical_json_field(normalized, "identifiers_json", default=()),
                    ),
                )
            ),
            payload=_json_value(
                normalized.get(
                    "payload",
                    _canonical_json_field(normalized, "payload_json", default={}),
                )
            ),
            key=_json_value(
                normalized.get(
                    "key",
                    normalized.get(
                        "state_key",
                        normalized.get(
                            "event_key",
                            _canonical_json_field(normalized, "key_json", default={}),
                        ),
                    ),
                )
            ),
            collect_id=_optional_string(normalized.get("collect_id")),
            sequence_order=_optional_int(normalized.get("sequence_order")),
            target=_optional_string(
                _target_value(
                    normalized.get("target", _canonical_json_field(normalized, "target_json"))
                )
            ),
            occurred_at=_optional_string(
                normalized.get("occurred_at", normalized.get("event_occurred_at"))
            ),
            payload_fingerprint=_optional_string(normalized.get("payload_fingerprint")),
            source_position=_source_position_from_row(
                normalized,
                family=family,
                collect_id=_optional_string(normalized.get("collect_id")),
                sequence_order=_optional_int(normalized.get("sequence_order")),
                event_cursor_kind=event_cursor_kind,
                event_primary_key_kind=event_primary_key_kind,
            ),
            source_lower_bound=_source_lower_bound_from_row(
                normalized,
                family=family,
                event_cursor_kind=event_cursor_kind,
                event_primary_key_kind=event_primary_key_kind,
            ),
            raw=normalized,
        )


def _destination_scan_range(
    records: Sequence[DestinationWorkRecord],
) -> DestinationScanRange | None:
    positions = tuple(record.source_position for record in records)
    if not positions or any(position is None for position in positions):
        return None
    return DestinationScanRange(
        lower_bound_exclusive=records[0].source_lower_bound,
        first_record_position=cast(ScanPosition, positions[0]),
        last_record_position=cast(ScanPosition, positions[-1]),
        upper_bound_inclusive=cast(ScanPosition, positions[-1]),
    )


def _source_lower_bound_from_row(
    row: Mapping[str, JSONValue],
    *,
    family: WorkFamily,
    event_cursor_kind: str | None,
    event_primary_key_kind: str | None,
) -> ScanPosition | None:
    if family != "event_imports":
        return None
    cursor = _optional_string(row.get("event_lower_cursor_value"))
    primary_key = _optional_string(row.get("event_lower_primary_key_value"))
    if cursor is None or primary_key is None:
        return None
    if event_cursor_kind is None or event_primary_key_kind is None:
        raise ValueError("Event request planning requires declaration checkpoint scalar types.")
    return EventKeysetScanPosition(
        cursor_value=_canonical_scalar_from_text(cursor, event_cursor_kind),
        primary_key_value=_canonical_scalar_from_text(primary_key, event_primary_key_kind),
    )


def _source_position_from_row(
    row: Mapping[str, JSONValue],
    *,
    family: WorkFamily,
    collect_id: str | None,
    sequence_order: int | None,
    event_cursor_kind: str | None,
    event_primary_key_kind: str | None,
) -> ScanPosition | None:
    if family == "event_imports":
        cursor = _optional_string(row.get("event_cursor_value"))
        primary_key = _optional_string(row.get("event_primary_key_value"))
        if cursor is None or primary_key is None:
            return None
        if event_cursor_kind is None or event_primary_key_kind is None:
            raise ValueError("Event request planning requires declaration checkpoint scalar types.")
        return EventKeysetScanPosition(
            cursor_value=_canonical_scalar_from_text(cursor, event_cursor_kind),
            primary_key_value=_canonical_scalar_from_text(primary_key, event_primary_key_kind),
        )
    if family != "state_operations":
        return None
    identity = _current_snapshot_identity_string(row.get("identity_json"))
    if identity is not None:
        return StateCurrentSnapshotScanPosition(
            key=CanonicalKey.of(CanonicalKeyScalar.string(identity))
        )
    if collect_id is not None and sequence_order is not None:
        return StateOrderedWorkScanPosition(collect_id=collect_id, sequence_order=sequence_order)
    return None


def _canonical_scalar_from_text(value: str, kind: str) -> CanonicalKeyScalar:
    if kind == "string":
        return CanonicalKeyScalar.string(value)
    if kind == "integer":
        return CanonicalKeyScalar.integer(int(value))
    if kind == "number":
        return CanonicalKeyScalar.number(float(value))
    if kind == "boolean":
        lowered = value.casefold()
        if lowered in {"true", "1"}:
            return CanonicalKeyScalar.boolean(True)
        if lowered in {"false", "0"}:
            return CanonicalKeyScalar.boolean(False)
    raise ValueError("Event checkpoint scalar type is not supported.")


def _current_snapshot_identity_string(value: JSONValue | None) -> str | None:
    if isinstance(value, str):
        return value if value.strip() else None
    if isinstance(value, Mapping | list):
        return json.dumps(
            _plain_json(value),
            sort_keys=True,
            separators=(",", ":"),
        )
    return None


def _operation_from_row(
    row: Mapping[str, JSONValue],
    *,
    family: WorkFamily,
) -> StateOperation | EventOperation:
    value = row.get("operation")
    if value is None and family == "event_imports":
        return "import"
    if value in ("upsert", "remove", "import"):
        return cast(StateOperation | EventOperation, value)
    raise ValueError("Destination work rows require an operation column.")


def _record_identity(row: Mapping[str, JSONValue]) -> str:
    for field_name in (
        "record_identity",
        "state_identity",
        "event_identity",
        "identity",
        "operation_id",
        "event_id",
        "work_id",
    ):
        value = row.get(field_name)
        if isinstance(value, str) and value.strip():
            return value
    fingerprint = row.get("payload_fingerprint")
    if isinstance(fingerprint, str) and fingerprint.strip():
        return fingerprint
    return payload_fingerprint(row)


def _json_mapping(row: Mapping[object, object]) -> Mapping[str, JSONValue]:
    return MappingProxyType({str(key): _json_value(value) for key, value in row.items()})


def _canonical_json_field(
    row: Mapping[str, JSONValue],
    field_name: str,
    *,
    default: JSONValue | None = None,
) -> JSONValue | None:
    if field_name not in row:
        return default
    value = row[field_name]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return value
        return cast(JSONValue, _plain_json(parsed))
    return _json_value(value)


def _target_value(value: object) -> object:
    if isinstance(value, Mapping) and set(value) == {"value"}:
        return value.get("value")
    return value


def _json_value(value: object) -> JSONValue:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped[0] in "[{":
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return value
            return cast(JSONValue, _plain_json(parsed))
        return value
    return cast(JSONValue, _plain_json(value))


def _identifier_value(value: object) -> JSONValue:
    if isinstance(value, str):
        return _json_value(value)
    if hasattr(value, "as_py"):
        return _identifier_value(value.as_py())
    if isinstance(value, Sequence) and not isinstance(value, str):
        return cast(JSONValue, [_json_value(item) for item in value])
    return cast(JSONValue, _plain_json(value))


def _plain_json(value: object) -> object:
    if hasattr(value, "as_py"):
        return _plain_json(value.as_py())
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str):
        return [_plain_json(item) for item in value]
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError("Destination work coordinates must be integers when provided.")


def _batch_operation(
    records: Sequence[DestinationWorkRecord],
) -> StateOperation | EventOperation | Literal["mixed"]:
    operations = {record.operation for record in records}
    if len(operations) == 1:
        return next(iter(operations))
    return "mixed"


def _operation_http_method(
    operation: StateOperation | EventOperation | Literal["mixed"],
) -> str:
    if operation == "remove":
        return "DELETE"
    return "POST"


def _default_json_body(context: RequestBatchContext) -> JSONMapping:
    return MappingProxyType(
        {
            "sync": context.sync_name,
            "surface": context.surface_name,
            "family": context.family,
            "operation": context.operation,
            "records": tuple(record.to_json() for record in context.records),
        }
    )


def _request_batch_id(
    *,
    sync_name: str,
    surface_name: str,
    family: WorkFamily,
    index: int,
    records: Sequence[DestinationWorkRecord],
    payload_fingerprint: str,
) -> str:
    identities = ",".join(record.record_identity for record in records)
    operations = ",".join(record.operation for record in records)
    stable = _digest(
        {
            "sync": sync_name,
            "surface": surface_name,
            "family": family,
            "index": index,
            "identities": identities,
            "operations": operations,
            "payload_fingerprint": payload_fingerprint,
        }
    )[:24]
    return f"{sync_name}:{surface_name}:{family}:{index}:{stable}"


def _mapping_template(value: JSONValue | None, *, field_name: str) -> JSONMapping:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise ValueError(f"Request template `{field_name}` must be a mapping.")
    return cast(JSONMapping, MappingProxyType(dict(value)))


def _render_string(value: JSONValue, context: JSONMapping, *, field_name: str) -> str:
    rendered = _expand_template(value, context)
    if not isinstance(rendered, str):
        raise ValueError(f"Rendered request `{field_name}` must be a string.")
    if not rendered.strip():
        raise ValueError(f"Rendered request `{field_name}` must not be empty.")
    return rendered


def _render_string_mapping(value: JSONMapping, context: JSONMapping) -> Mapping[str, str]:
    rendered: dict[str, str] = {}
    for key, item in value.items():
        expanded = _expand_template(item, context)
        if expanded is None:
            continue
        if isinstance(expanded, bool | int | float | str):
            rendered[key] = str(expanded)
            continue
        raise ValueError("Rendered request query and headers must contain scalar values.")
    return MappingProxyType(rendered)


def _expand_template(value: JSONValue, context: JSONMapping) -> JSONValue:
    _reject_secret_refs(value, field_name="request_template")
    if isinstance(value, str):
        exact = _PLACEHOLDER.fullmatch(value)
        if exact:
            return cast(JSONValue, _resolve_path(exact.group(1), context))
        return _PLACEHOLDER.sub(lambda match: str(_resolve_path(match.group(1), context)), value)
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _expand_template(item, context) for key, item in value.items()}
        )
    if isinstance(value, Sequence) and not isinstance(value, str):
        return tuple(_expand_template(item, context) for item in value)
    return value


def _resolve_path(path: str, context: JSONMapping) -> object:
    current: object = context
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
            continue
        raise ValueError(f"Unknown request template placeholder: {path}.")
    return current


def _reject_secret_refs(value: object, *, field_name: str) -> None:
    if isinstance(value, SecretRef | SecretLiteral):
        raise ValueError(f"`{field_name}` must use public config, not secret-shaped values.")
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_secret_refs(item, field_name=field_name)
    elif isinstance(value, Sequence) and not isinstance(value, str):
        for item in value:
            _reject_secret_refs(item, field_name=field_name)


def _reject_auth_headers(headers: Mapping[str, str]) -> None:
    forbidden = sorted(name for name in headers if is_sensitive_evidence_name(name))
    if forbidden:
        raise ValueError(f"Request template headers must not include auth material: {forbidden}.")


def _reject_auth_query(query: Mapping[str, str]) -> None:
    forbidden = sorted(name for name in query if is_sensitive_evidence_name(name))
    if forbidden:
        raise ValueError(f"Request template query must not include auth material: {forbidden}.")


def _digest(value: object) -> str:
    encoded = json.dumps(
        _canonicalize(value),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _canonicalize(value: object) -> object:
    if isinstance(value, SecretRef | SecretLiteral):
        raise ValueError("Secret-shaped values cannot be fingerprinted.")
    if isinstance(value, Mapping):
        return {str(key): _canonicalize(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str):
        return [_canonicalize(item) for item in value]
    return value


__all__ = [
    "DestinationWorkRecord",
    "DryRunSubmissionPlan",
    "PlannedHttpRequest",
    "RequestBatchContext",
    "RequestBatchPlan",
    "RequestBatchingPolicy",
    "RequestBodyHook",
    "RequestItemCountsHook",
    "RequestPartitionHook",
    "RequestRecordHook",
    "RequestTemplate",
    "payload_fingerprint",
    "plan_request_batches",
    "render_request",
    "target_request_fingerprint",
    "template_from_mapping",
]
