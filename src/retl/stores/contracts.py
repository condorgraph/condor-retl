from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeAlias, cast, runtime_checkable

import pyarrow as pa  # type: ignore[import-untyped]

from retl.collect_identity import is_uuidv7

if TYPE_CHECKING:
    from retl.destinations.targets import TargetRegistryKey, TargetRegistryRecord
    from retl.runtime.recovery import (
        AttemptIdentity,
        AttemptStatus,
        CommitDecisionRecord,
        ReceiptRecord,
        RemoteHandleRecord,
    )

WorkFamily = Literal["state", "event"]
WorkKind = Literal["upsert", "remove", "import"]
StateScanMode = Literal["ordered_work", "current_snapshot"]
CanonicalKeyScalarKind = Literal["null", "boolean", "integer", "number", "string"]
DestinationBatchStatus = Literal[
    "pending",
    "accepted",
    "succeeded",
    "failed",
    "skipped",
]
SqlRelationAccess = Literal["read_only", "read_write"]


@dataclass(frozen=True)
class SqlRelationSpace:
    backend_name: str
    database: str
    schema: str
    access: SqlRelationAccess

    def __post_init__(self) -> None:
        _validate_nonempty_contract_string(self.backend_name, "backend_name")
        _validate_nonempty_contract_string(self.database, "database")
        _validate_nonempty_contract_string(self.schema, "schema")
        if self.access not in {"read_only", "read_write"}:
            raise ValueError("SQL relation space access is not supported.")


@dataclass(frozen=True)
class SqlCollectPlacement:
    source: SqlRelationSpace
    runtime: SqlRelationSpace

    def __post_init__(self) -> None:
        if not isinstance(self.source, SqlRelationSpace):
            raise ValueError("SQL collect placement source must be a SqlRelationSpace.")
        if not isinstance(self.runtime, SqlRelationSpace):
            raise ValueError("SQL collect placement runtime must be a SqlRelationSpace.")
        if self.source.access != "read_only":
            raise ValueError("SQL collect placement source access must be read_only.")
        if self.runtime.access != "read_write":
            raise ValueError("SQL collect placement runtime access must be read_write.")
        if self.source.backend_name != self.runtime.backend_name:
            raise ValueError("SQL collect placement source and runtime backend names must match.")


@dataclass(frozen=True)
class CanonicalKeyScalar:
    kind: CanonicalKeyScalarKind
    value: bool | int | float | str | None

    def __post_init__(self) -> None:
        if self.kind == "null":
            if self.value is not None:
                raise ValueError("null canonical key scalar value must be None.")
            return
        if self.kind == "boolean":
            if not isinstance(self.value, bool):
                raise ValueError("boolean canonical key scalar value must be a bool.")
            return
        if self.kind == "integer":
            if not isinstance(self.value, int) or isinstance(self.value, bool):
                raise ValueError("integer canonical key scalar value must be an int.")
            return
        if self.kind == "number":
            if not isinstance(self.value, float):
                raise ValueError("number canonical key scalar value must be a float.")
            if not math.isfinite(self.value):
                raise ValueError("number canonical key scalar value must be finite.")
            return
        if self.kind == "string":
            if not isinstance(self.value, str):
                raise ValueError("string canonical key scalar value must be a string.")
            return
        raise ValueError(f"unsupported canonical key scalar kind `{self.kind}`.")

    @classmethod
    def null(cls) -> CanonicalKeyScalar:
        return cls(kind="null", value=None)

    @classmethod
    def boolean(cls, value: bool) -> CanonicalKeyScalar:
        return cls(kind="boolean", value=value)

    @classmethod
    def integer(cls, value: int) -> CanonicalKeyScalar:
        return cls(kind="integer", value=value)

    @classmethod
    def number(cls, value: float) -> CanonicalKeyScalar:
        return cls(kind="number", value=value)

    @classmethod
    def string(cls, value: str) -> CanonicalKeyScalar:
        return cls(kind="string", value=value)


@dataclass(frozen=True)
class CanonicalKey:
    parts: tuple[CanonicalKeyScalar, ...]

    def __post_init__(self) -> None:
        if not self.parts:
            raise ValueError("canonical key must contain at least one part.")
        if not all(isinstance(part, CanonicalKeyScalar) for part in self.parts):
            raise ValueError("canonical key parts must be CanonicalKeyScalar values.")

    @classmethod
    def of(cls, *parts: CanonicalKeyScalar) -> CanonicalKey:
        return cls(parts=parts)


@dataclass(frozen=True)
class StateOrderedWorkScanPosition:
    collect_id: str
    sequence_order: int
    family: Literal["state"] = field(default="state", init=False)
    mode: Literal["ordered_work"] = field(default="ordered_work", init=False)

    def __post_init__(self) -> None:
        _validate_collect_id_contract_string(self.collect_id, "collect_id")
        _validate_nonnegative_contract_int(self.sequence_order, "sequence_order")


@dataclass(frozen=True)
class StateCurrentSnapshotScanPosition:
    key: CanonicalKey
    family: Literal["state"] = field(default="state", init=False)
    mode: Literal["current_snapshot"] = field(default="current_snapshot", init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.key, CanonicalKey):
            raise ValueError("current snapshot State scan position key must be a CanonicalKey.")


@dataclass(frozen=True)
class EventKeysetScanPosition:
    cursor_value: CanonicalKeyScalar
    primary_key_value: CanonicalKeyScalar
    family: Literal["event"] = field(default="event", init=False)
    mode: Literal["keyset"] = field(default="keyset", init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.cursor_value, CanonicalKeyScalar):
            raise ValueError("Event scan cursor value must be a CanonicalKeyScalar.")
        if not isinstance(self.primary_key_value, CanonicalKeyScalar):
            raise ValueError("Event scan primary key value must be a CanonicalKeyScalar.")


ScanPosition: TypeAlias = (
    StateOrderedWorkScanPosition | StateCurrentSnapshotScanPosition | EventKeysetScanPosition
)


@dataclass(frozen=True)
class DestinationScanRange:
    upper_bound_inclusive: ScanPosition
    first_record_position: ScanPosition
    last_record_position: ScanPosition
    lower_bound_exclusive: ScanPosition | None = None

    def __post_init__(self) -> None:
        _validate_scan_position(self.upper_bound_inclusive, "upper_bound_inclusive")
        _validate_scan_position(self.first_record_position, "first_record_position")
        _validate_scan_position(self.last_record_position, "last_record_position")
        _validate_optional_scan_position(self.lower_bound_exclusive, "lower_bound_exclusive")
        _validate_same_scan_position_family_and_mode(
            self.first_record_position,
            self.last_record_position,
            "first_record_position",
            "last_record_position",
        )
        _validate_same_scan_position_family_and_mode(
            self.last_record_position,
            self.upper_bound_inclusive,
            "last_record_position",
            "upper_bound_inclusive",
        )
        if self.lower_bound_exclusive is not None:
            _validate_same_scan_position_family_and_mode(
                self.lower_bound_exclusive,
                self.first_record_position,
                "lower_bound_exclusive",
                "first_record_position",
            )
            if compare_scan_positions(self.lower_bound_exclusive, self.first_record_position) >= 0:
                raise ValueError("lower_bound_exclusive must be before first_record_position.")
        if compare_scan_positions(self.first_record_position, self.last_record_position) > 0:
            raise ValueError(
                "first_record_position must be before or equal to last_record_position."
            )
        if compare_scan_positions(self.last_record_position, self.upper_bound_inclusive) > 0:
            raise ValueError(
                "last_record_position must be before or equal to upper_bound_inclusive."
            )

    @property
    def family(self) -> WorkFamily:
        return self.upper_bound_inclusive.family


def compare_scan_positions(left: ScanPosition, right: ScanPosition) -> int:
    _validate_same_scan_position_family_and_mode(left, right, "left", "right")
    left_key = _scan_position_comparison_key(left)
    right_key = _scan_position_comparison_key(right)
    return (left_key > right_key) - (left_key < right_key)


def scan_position_to_jsonable(position: ScanPosition) -> dict[str, Any]:
    _validate_scan_position(position, "position")
    if isinstance(position, StateOrderedWorkScanPosition):
        return {
            "collect_id": position.collect_id,
            "family": "state",
            "mode": "ordered_work",
            "sequence_order": position.sequence_order,
        }
    if isinstance(position, StateCurrentSnapshotScanPosition):
        return {
            "family": "state",
            "key": canonical_key_to_jsonable(position.key),
            "mode": "current_snapshot",
        }
    return {
        "cursor_value": canonical_key_scalar_to_jsonable(position.cursor_value),
        "family": "event",
        "mode": "keyset",
        "primary_key_value": canonical_key_scalar_to_jsonable(position.primary_key_value),
    }


def scan_position_from_jsonable(value: Mapping[str, Any]) -> ScanPosition:
    if not isinstance(value, Mapping):
        raise ValueError("scan position must be a mapping.")
    family = value.get("family")
    mode = value.get("mode")
    if family == "state" and mode == "ordered_work":
        if "collect_id" not in value or "sequence_order" not in value:
            raise ValueError(
                "ordered_work State scan positions require collect_id and sequence_order."
            )
        return StateOrderedWorkScanPosition(
            collect_id=_contract_string_from_jsonable(value["collect_id"], "collect_id"),
            sequence_order=_contract_int_from_jsonable(value["sequence_order"], "sequence_order"),
        )
    if family == "state" and mode == "current_snapshot":
        raw_key = value.get("key")
        if not isinstance(raw_key, Mapping):
            raise ValueError("current_snapshot State scan positions require a structured key.")
        return StateCurrentSnapshotScanPosition(key=canonical_key_from_jsonable(raw_key))
    if family == "event" and mode == "keyset":
        cursor_value = value.get("cursor_value")
        primary_key_value = value.get("primary_key_value")
        if not isinstance(cursor_value, Mapping) or not isinstance(primary_key_value, Mapping):
            raise ValueError("Event scan positions require cursor_value and primary_key_value.")
        return EventKeysetScanPosition(
            cursor_value=canonical_key_scalar_from_jsonable(cursor_value),
            primary_key_value=canonical_key_scalar_from_jsonable(primary_key_value),
        )
    raise ValueError("scan position family and mode are not supported.")


def canonical_key_to_jsonable(key: CanonicalKey) -> dict[str, Any]:
    if not isinstance(key, CanonicalKey):
        raise ValueError("canonical key must be a CanonicalKey.")
    return {"parts": [canonical_key_scalar_to_jsonable(part) for part in key.parts]}


def canonical_key_from_jsonable(value: Mapping[str, Any]) -> CanonicalKey:
    parts = value.get("parts")
    if not isinstance(parts, list):
        raise ValueError("canonical key must contain a parts list.")
    return CanonicalKey(parts=tuple(canonical_key_scalar_from_jsonable(part) for part in parts))


def canonical_key_scalar_to_jsonable(scalar: CanonicalKeyScalar) -> dict[str, Any]:
    if not isinstance(scalar, CanonicalKeyScalar):
        raise ValueError("canonical key scalar must be a CanonicalKeyScalar.")
    return {"kind": scalar.kind, "value": scalar.value}


def canonical_key_scalar_from_jsonable(value: Mapping[str, Any]) -> CanonicalKeyScalar:
    if not isinstance(value, Mapping):
        raise ValueError("canonical key scalar must be a mapping.")
    kind = value.get("kind")
    if kind not in {"null", "boolean", "integer", "number", "string"}:
        raise ValueError("canonical key scalar kind is not supported.")
    return CanonicalKeyScalar(
        kind=cast(CanonicalKeyScalarKind, kind),
        value=cast(bool | int | float | str | None, value.get("value")),
    )


def destination_scan_range_to_jsonable(scan_range: DestinationScanRange) -> dict[str, Any]:
    if not isinstance(scan_range, DestinationScanRange):
        raise ValueError("destination scan range must be a DestinationScanRange.")
    return {
        "first_record_position": scan_position_to_jsonable(scan_range.first_record_position),
        "last_record_position": scan_position_to_jsonable(scan_range.last_record_position),
        "lower_bound_exclusive": (
            scan_position_to_jsonable(scan_range.lower_bound_exclusive)
            if scan_range.lower_bound_exclusive is not None
            else None
        ),
        "upper_bound_inclusive": scan_position_to_jsonable(scan_range.upper_bound_inclusive),
    }


def destination_scan_range_from_jsonable(value: Mapping[str, Any]) -> DestinationScanRange:
    if not isinstance(value, Mapping):
        raise ValueError("destination scan range must be a mapping.")
    first_record_position = value.get("first_record_position")
    last_record_position = value.get("last_record_position")
    upper_bound_inclusive = value.get("upper_bound_inclusive")
    lower_bound_exclusive = value.get("lower_bound_exclusive")
    if (
        not isinstance(first_record_position, Mapping)
        or not isinstance(last_record_position, Mapping)
        or not isinstance(upper_bound_inclusive, Mapping)
    ):
        raise ValueError("destination scan range requires first, last, and upper scan positions.")
    if lower_bound_exclusive is not None and not isinstance(lower_bound_exclusive, Mapping):
        raise ValueError("destination scan range lower bound must be a scan position.")
    return DestinationScanRange(
        lower_bound_exclusive=(
            scan_position_from_jsonable(lower_bound_exclusive)
            if lower_bound_exclusive is not None
            else None
        ),
        first_record_position=scan_position_from_jsonable(first_record_position),
        last_record_position=scan_position_from_jsonable(last_record_position),
        upper_bound_inclusive=scan_position_from_jsonable(upper_bound_inclusive),
    )


def sql_relation_space_to_jsonable(space: SqlRelationSpace) -> dict[str, Any]:
    if not isinstance(space, SqlRelationSpace):
        raise ValueError("SQL relation space must be a SqlRelationSpace.")
    return {
        "access": space.access,
        "backend_name": space.backend_name,
        "database": space.database,
        "schema": space.schema,
    }


def sql_relation_space_from_jsonable(value: Mapping[str, Any]) -> SqlRelationSpace:
    if not isinstance(value, Mapping):
        raise ValueError("SQL relation space must be a mapping.")
    access = value.get("access")
    if access not in {"read_only", "read_write"}:
        raise ValueError("SQL relation space access is not supported.")
    return SqlRelationSpace(
        access=cast(SqlRelationAccess, access),
        backend_name=_contract_string_from_jsonable(value.get("backend_name"), "backend_name"),
        database=_contract_string_from_jsonable(value.get("database"), "database"),
        schema=_contract_string_from_jsonable(value.get("schema"), "schema"),
    )


def sql_collect_placement_to_jsonable(placement: SqlCollectPlacement) -> dict[str, Any]:
    if not isinstance(placement, SqlCollectPlacement):
        raise ValueError("SQL collect placement must be a SqlCollectPlacement.")
    return {
        "runtime": sql_relation_space_to_jsonable(placement.runtime),
        "source": sql_relation_space_to_jsonable(placement.source),
    }


def sql_collect_placement_from_jsonable(value: Mapping[str, Any]) -> SqlCollectPlacement:
    if not isinstance(value, Mapping):
        raise ValueError("SQL collect placement must be a mapping.")
    source = value.get("source")
    runtime = value.get("runtime")
    if not isinstance(source, Mapping) or not isinstance(runtime, Mapping):
        raise ValueError("SQL collect placement requires source and runtime relation spaces.")
    return SqlCollectPlacement(
        source=sql_relation_space_from_jsonable(source),
        runtime=sql_relation_space_from_jsonable(runtime),
    )


def _validate_scan_position(position: object, field_name: str) -> None:
    if not isinstance(
        position,
        (
            EventKeysetScanPosition,
            StateCurrentSnapshotScanPosition,
            StateOrderedWorkScanPosition,
        ),
    ):
        raise ValueError(f"{field_name} must be a ScanPosition.")


def _validate_optional_scan_position(position: object, field_name: str) -> None:
    if position is not None:
        _validate_scan_position(position, field_name)


def _validate_source_relation_space(
    source_space: object,
    *,
    handle_backend: str,
    handle_name: str,
) -> None:
    if not isinstance(source_space, SqlRelationSpace):
        raise ValueError(f"{handle_name} source_space must be a SqlRelationSpace.")
    if source_space.access != "read_only":
        raise ValueError(f"{handle_name} source_space access must be read_only.")
    if handle_backend != source_space.backend_name:
        raise ValueError(f"{handle_name} backend must match source_space.backend_name.")


def _validate_same_scan_position_family_and_mode(
    left: ScanPosition,
    right: ScanPosition,
    left_name: str,
    right_name: str,
) -> None:
    _validate_scan_position(left, left_name)
    _validate_scan_position(right, right_name)
    if left.family != right.family:
        raise ValueError(f"{left_name} and {right_name} scan position families cannot be mixed.")
    if left.mode != right.mode:
        raise ValueError(f"{left_name} and {right_name} scan position modes cannot be mixed.")


def _scan_position_comparison_key(
    position: ScanPosition,
) -> tuple[object, ...]:
    if isinstance(position, StateOrderedWorkScanPosition):
        return (position.collect_id, position.sequence_order)
    if isinstance(position, StateCurrentSnapshotScanPosition):
        return _canonical_key_comparison_key(position.key)
    return (
        _canonical_key_scalar_comparison_key(position.cursor_value),
        _canonical_key_scalar_comparison_key(position.primary_key_value),
    )


def _canonical_key_comparison_key(key: CanonicalKey) -> tuple[object, ...]:
    return tuple(_canonical_key_scalar_comparison_key(part) for part in key.parts)


def _canonical_key_scalar_comparison_key(scalar: CanonicalKeyScalar) -> tuple[int, object]:
    kind_order = {
        "null": 0,
        "boolean": 1,
        "integer": 2,
        "number": 3,
        "string": 4,
    }
    if scalar.kind == "null":
        return (kind_order[scalar.kind], "")
    return (kind_order[scalar.kind], scalar.value)


def _contract_int_from_jsonable(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer.")
    return value


def _contract_string_from_jsonable(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string.")
    return value


def _validate_nonempty_contract_string(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")


def _validate_collect_id_contract_string(value: object, field_name: str) -> None:
    if not is_uuidv7(value):
        raise ValueError(f"{field_name} must be a UUIDv7 string.")


def _validate_positive_contract_int(value: object, field_name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer.")


def _validate_nonnegative_contract_int(value: object, field_name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field_name} must be a nonnegative integer.")


DestinationBatchCompletionState = Literal[
    "unresolved",
    "resolved",
]


@dataclass(frozen=True)
class OrderedWorkInput:
    collect_id: str
    family: WorkFamily
    kind: WorkKind
    declaration_name: str
    key: Mapping[str, Any] = field(default_factory=dict)
    target: Mapping[str, Any] | None = None
    identifiers: tuple[Mapping[str, Any], ...] = ()
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderedWorkRow:
    work_id: str
    collect_id: str
    sequence_order: int
    family: WorkFamily
    kind: WorkKind
    declaration_name: str
    key: Mapping[str, Any]
    target: Mapping[str, Any] | None
    identifiers: tuple[Mapping[str, Any], ...]
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class PendingWorkCursor:
    token: str
    collect_id: str
    sequence_order: int


@dataclass(frozen=True)
class EventSourceCursor:
    position: EventKeysetScanPosition


@dataclass(frozen=True)
class PendingWorkPage:
    payload: pa.RecordBatch
    row_count: int
    first_collect_id: str | None = None
    last_collect_id: str | None = None
    first_sequence_order: int | None = None
    last_sequence_order: int | None = None
    complete_through_collect_id: str | None = None
    next_cursor: PendingWorkCursor | EventSourceCursor | None = None


@dataclass(frozen=True)
class StateCurrentCursor:
    token: str
    identity: str
    position: StateCurrentSnapshotScanPosition


@dataclass(frozen=True)
class StateCurrentPage:
    payload: pa.RecordBatch
    row_count: int
    collect_id: str | None = None
    first_collect_id: str | None = None
    last_collect_id: str | None = None
    first_sequence_order: int | None = None
    last_sequence_order: int | None = None
    next_cursor: StateCurrentCursor | None = None


@dataclass(frozen=True)
class DestinationProgressScope:
    sync_name: str
    destination_name: str
    surface: str
    family: WorkFamily
    declaration_name: str


@dataclass(frozen=True)
class DestinationProgress:
    scope: DestinationProgressScope
    position: ScanPosition | None = None


@dataclass(frozen=True)
class DestinationProgressUpdate:
    scope: DestinationProgressScope
    before: ScanPosition | None
    after: ScanPosition | None
    advanced: bool


@dataclass(frozen=True)
class DestinationBatchIdentity:
    scope: DestinationProgressScope
    declaration_version_id: str
    source_range: DestinationScanRange | None = None
    source_page_index: int | None = None
    reconcile_page_index: int | None = None
    first_collect_id: str = ""
    last_collect_id: str = ""
    first_sequence_order: int = 0
    last_sequence_order: int = 0
    destination_batch_index: int = 0
    payload_fingerprint: str = ""
    target_request_fingerprint: str = ""

    def __post_init__(self) -> None:
        if self.source_range is not None and self.source_range.family != self.scope.family:
            raise ValueError("destination batch source range family must match the scope family.")


@dataclass(frozen=True)
class DestinationBatchRecord:
    batch_id: str
    identity: DestinationBatchIdentity
    run_id: str | None = None
    attempt_id: str | None = None
    record_count: int = 0
    status: DestinationBatchStatus = "pending"
    completion_state: DestinationBatchCompletionState = "unresolved"
    attempt_count: int = 0
    last_error_summary: str | None = None
    last_error_detail: str | None = None
    last_failure_category: str | None = None
    http_status: int | None = None
    retry_eligible: bool | None = None
    first_submitted_at: datetime | None = None
    last_attempted_at: datetime | None = None
    completed_at: datetime | None = None


def destination_batch_id(identity: DestinationBatchIdentity) -> str:
    event_keyset_range = identity.scope.family == "event" and identity.source_range is not None
    if event_keyset_range:
        assert identity.source_range is not None
        payload = {
            "destination_batch_index": identity.destination_batch_index,
            "scope": {
                "declaration_name": identity.scope.declaration_name,
                "destination_name": identity.scope.destination_name,
                "family": identity.scope.family,
                "surface": identity.scope.surface,
                "sync_name": identity.scope.sync_name,
            },
            "source_range": destination_scan_range_to_jsonable(identity.source_range),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return f"destination-batch:{hashlib.sha256(encoded).hexdigest()}"
    payload = {
        "destination_batch_index": identity.destination_batch_index,
        "first_collect_id": identity.first_collect_id,
        "first_sequence_order": identity.first_sequence_order,
        "last_collect_id": identity.last_collect_id,
        "last_sequence_order": identity.last_sequence_order,
        "payload_fingerprint": identity.payload_fingerprint,
        "reconcile_page_index": identity.reconcile_page_index,
        "scope": {
            "declaration_name": identity.scope.declaration_name,
            "destination_name": identity.scope.destination_name,
            "family": identity.scope.family,
            "surface": identity.scope.surface,
            "sync_name": identity.scope.sync_name,
        },
        "source_range": (
            destination_scan_range_to_jsonable(identity.source_range)
            if identity.source_range is not None
            else None
        ),
        "source_page_index": identity.source_page_index,
        "target_request_fingerprint": identity.target_request_fingerprint,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"destination-batch:{hashlib.sha256(encoded).hexdigest()}"


def state_ordered_work_position_after(
    *,
    collect_id: str,
    sequence_order: int,
    position: StateOrderedWorkScanPosition | None,
) -> bool:
    candidate = StateOrderedWorkScanPosition(
        collect_id=collect_id,
        sequence_order=sequence_order,
    )
    if position is None:
        return True
    return compare_scan_positions(candidate, position) > 0


@dataclass(frozen=True)
class OrderedWorkRetentionCleanup:
    family: WorkFamily
    declaration_name: str
    requested_through_collect_id: str | None
    safe_through_collect_id: str | None
    deleted_ordered_work_count: int
    retained_pending_count: int
    dry_run: bool = False


@dataclass(frozen=True)
class StateSnapshotHandle:
    backend: str
    source_name: str
    source_identity: Mapping[str, Any]
    query: str
    source_space: SqlRelationSpace

    def __post_init__(self) -> None:
        _validate_source_relation_space(
            self.source_space,
            handle_backend=self.backend,
            handle_name="State snapshot",
        )


@dataclass(frozen=True)
class StateSnapshotRequest:
    source_name: str
    query: str


@runtime_checkable
class StateSnapshotSource(Protocol):
    def prepare_state_snapshot(self, request: StateSnapshotRequest) -> StateSnapshotHandle: ...


@dataclass(frozen=True)
class EventSourceWindowHandle:
    backend: str
    source_name: str
    source_identity: Mapping[str, Any]
    query: str
    cursor_column: str
    primary_key_column: str
    source_space: SqlRelationSpace
    scan_after: EventKeysetScanPosition | None = None
    scan_through: EventKeysetScanPosition | None = None
    limit: int | None = None

    def __post_init__(self) -> None:
        _validate_source_relation_space(
            self.source_space,
            handle_backend=self.backend,
            handle_name="Event source window",
        )


@dataclass(frozen=True)
class EventSourceWindowRequest:
    source_name: str
    query: str
    cursor_column: str
    primary_key_column: str
    scan_after: EventKeysetScanPosition | None = None
    scan_through: EventKeysetScanPosition | None = None
    limit: int | None = None


@runtime_checkable
class EventSourceWindowSource(Protocol):
    def prepare_event_source_window(
        self, request: EventSourceWindowRequest
    ) -> EventSourceWindowHandle: ...


@dataclass(frozen=True)
class StateProductionResult:
    collect_id: str
    declaration_name: str
    source_name: str
    current_row_count: int
    work_row_count: int
    upsert_count: int
    remove_count: int


@dataclass(frozen=True)
class StateCurrentSummary:
    declaration_name: str
    source_name: str
    collect_id: str | None
    row_count: int


@dataclass(frozen=True)
class EventProductionResult:
    collect_id: str
    declaration_name: str
    source_name: str
    scan_after: EventKeysetScanPosition | None
    scan_upper_bound: EventKeysetScanPosition | None
    window_row_count: int
    work_row_count: int
    duplicate_risk_count: int


class CollectIdAllocator(Protocol):
    def allocate_collect_id(self) -> str: ...


class RuntimeProvenanceStore(Protocol):
    def register_run(self, run: object) -> None: ...

    def complete_run(self, *, run_id: str, status: str) -> None: ...

    def register_declaration(self, metadata: object) -> None: ...


class TargetRegistryStore(Protocol):
    def get(self, key: TargetRegistryKey) -> TargetRegistryRecord | None: ...

    def put(self, record: TargetRegistryRecord) -> None: ...


class OrderedWorkStore(CollectIdAllocator, Protocol):
    def read_pending_work(
        self,
        *,
        scope: DestinationProgressScope,
        max_rows: int,
        cursor: PendingWorkCursor | None = None,
        source_collect_id: str | None = None,
        progress_position: ScanPosition | None = None,
        progress_position_loaded: bool = False,
    ) -> PendingWorkPage: ...

    def register_destination_progress(
        self,
        scope: DestinationProgressScope,
    ) -> DestinationProgress: ...

    def get_destination_progress(
        self,
        scope: DestinationProgressScope,
    ) -> DestinationProgress: ...

    def update_destination_progress(
        self,
        *,
        scope: DestinationProgressScope,
        position: ScanPosition | None,
        advance: bool = True,
        current_position: ScanPosition | None = None,
        current_position_loaded: bool = False,
    ) -> DestinationProgressUpdate: ...

    def retention_watermark(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        progress_positions: tuple[ScanPosition | None, ...] | None = None,
    ) -> str | None: ...

    def cleanup_ordered_work(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        through_collect_id: str | None = None,
        dry_run: bool = False,
    ) -> OrderedWorkRetentionCleanup: ...


class DestinationBatchLedgerStore(OrderedWorkStore, Protocol):
    def get_destination_batch(self, *, batch_id: str) -> DestinationBatchRecord | None: ...

    def get_destination_batches(
        self,
        *,
        batch_ids: tuple[str, ...],
    ) -> tuple[DestinationBatchRecord, ...]: ...

    def list_destination_batches(
        self,
        *,
        scope: DestinationProgressScope | None = None,
        statuses: tuple[DestinationBatchStatus, ...] = (),
    ) -> tuple[DestinationBatchRecord, ...]: ...

    def list_destination_batch_retry_candidates(
        self,
        *,
        scope: DestinationProgressScope,
        retry_limit: int,
    ) -> tuple[DestinationBatchRecord, ...]: ...

    def read_destination_batch_work(self, *, batch: DestinationBatchRecord) -> PendingWorkPage: ...

    def dismiss_unresolved_destination_batches(
        self,
        *,
        scope: DestinationProgressScope,
    ) -> tuple[DestinationBatchRecord, ...]: ...


class StateProductionStore(OrderedWorkStore, Protocol):
    def produce_state_collect(
        self,
        *,
        declaration: object,
        snapshot: StateSnapshotHandle,
    ) -> StateProductionResult: ...

    def state_current_summary(
        self,
        *,
        declaration_name: str,
        source_name: str,
    ) -> StateCurrentSummary: ...

    def read_state_current_upserts(
        self,
        *,
        declaration_name: str,
        source_name: str,
        max_rows: int,
        cursor: StateCurrentCursor | None = None,
        position: StateCurrentSnapshotScanPosition | None = None,
    ) -> StateCurrentPage: ...


class EventProductionStore(OrderedWorkStore, Protocol):
    def produce_event_collect(
        self,
        *,
        declaration: object,
        window: EventSourceWindowHandle,
    ) -> EventProductionResult: ...

    def read_event_source_window(
        self,
        *,
        declaration: object,
        window: EventSourceWindowHandle,
        max_rows: int,
    ) -> PendingWorkPage: ...


class RecoveryStore(Protocol):
    def begin_attempt(
        self,
        *,
        runner_name: str,
        sync_name: str,
        dry_run: bool,
    ) -> AttemptIdentity: ...

    def record_receipt(self, record: ReceiptRecord) -> None: ...

    def record_remote_handle(self, record: RemoteHandleRecord) -> None: ...

    def record_commit_decision(self, decision: CommitDecisionRecord) -> None: ...

    def complete_attempt(self, *, attempt_id: str, status: AttemptStatus) -> None: ...


class ReportStore(Protocol):
    def record_sync_report(self, report: object) -> None: ...

    def upsert_destination_batch(
        self,
        record: DestinationBatchRecord,
    ) -> DestinationBatchRecord: ...

    def upsert_destination_batches(
        self,
        records: tuple[DestinationBatchRecord, ...],
        *,
        read_back: bool = True,
        existing_batches: tuple[DestinationBatchRecord, ...] | None = None,
    ) -> tuple[DestinationBatchRecord, ...]: ...

    def get_destination_batch(self, *, batch_id: str) -> DestinationBatchRecord | None: ...

    def get_destination_batches(
        self,
        *,
        batch_ids: tuple[str, ...],
    ) -> tuple[DestinationBatchRecord, ...]: ...

    def list_destination_batches(
        self,
        *,
        scope: DestinationProgressScope | None = None,
        statuses: tuple[DestinationBatchStatus, ...] = (),
    ) -> tuple[DestinationBatchRecord, ...]: ...

    def list_destination_batch_retry_candidates(
        self,
        *,
        scope: DestinationProgressScope,
        retry_limit: int,
    ) -> tuple[DestinationBatchRecord, ...]: ...

    def read_destination_batch_work(self, *, batch: DestinationBatchRecord) -> PendingWorkPage: ...

    def dismiss_unresolved_destination_batches(
        self,
        *,
        scope: DestinationProgressScope,
    ) -> tuple[DestinationBatchRecord, ...]: ...


class RuntimeStore(
    StateProductionStore,
    EventProductionStore,
    RecoveryStore,
    ReportStore,
    RuntimeProvenanceStore,
    TargetRegistryStore,
    Protocol,
):
    """Runner-owned persistence boundary for RETL operational state."""


__all__ = [
    "CanonicalKey",
    "CanonicalKeyScalar",
    "CanonicalKeyScalarKind",
    "CollectIdAllocator",
    "DestinationBatchCompletionState",
    "DestinationBatchIdentity",
    "DestinationBatchLedgerStore",
    "DestinationBatchRecord",
    "DestinationBatchStatus",
    "DestinationScanRange",
    "DestinationProgress",
    "DestinationProgressScope",
    "DestinationProgressUpdate",
    "EventKeysetScanPosition",
    "EventSourceCursor",
    "EventProductionResult",
    "EventProductionStore",
    "EventSourceWindowHandle",
    "EventSourceWindowRequest",
    "EventSourceWindowSource",
    "OrderedWorkInput",
    "OrderedWorkRetentionCleanup",
    "PendingWorkCursor",
    "PendingWorkPage",
    "OrderedWorkRow",
    "OrderedWorkStore",
    "RecoveryStore",
    "ReportStore",
    "RuntimeStore",
    "RuntimeProvenanceStore",
    "ScanPosition",
    "SqlCollectPlacement",
    "SqlRelationAccess",
    "SqlRelationSpace",
    "StateCurrentSnapshotScanPosition",
    "StateCurrentSummary",
    "StateCurrentCursor",
    "StateCurrentPage",
    "StateOrderedWorkScanPosition",
    "StateScanMode",
    "StateProductionResult",
    "StateProductionStore",
    "TargetRegistryStore",
    "state_ordered_work_position_after",
    "StateSnapshotHandle",
    "StateSnapshotRequest",
    "StateSnapshotSource",
    "WorkFamily",
    "WorkKind",
    "canonical_key_from_jsonable",
    "canonical_key_scalar_from_jsonable",
    "canonical_key_scalar_to_jsonable",
    "canonical_key_to_jsonable",
    "compare_scan_positions",
    "destination_batch_id",
    "destination_scan_range_from_jsonable",
    "destination_scan_range_to_jsonable",
    "scan_position_from_jsonable",
    "scan_position_to_jsonable",
    "sql_collect_placement_from_jsonable",
    "sql_collect_placement_to_jsonable",
    "sql_relation_space_from_jsonable",
    "sql_relation_space_to_jsonable",
]
