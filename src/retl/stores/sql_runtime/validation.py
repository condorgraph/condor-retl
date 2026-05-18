from __future__ import annotations

import re

from retl.collect_identity import is_uuidv7
from retl.errors import DeclarationValidationError
from retl.stores.contracts import (
    DestinationProgressScope,
    EventKeysetScanPosition,
    PendingWorkCursor,
    ScanPosition,
    StateOrderedWorkScanPosition,
    state_ordered_work_position_after,
)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identity(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"`{field_name}` must be a non-empty string.")


def validate_positive_int(value: int, field_name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise DeclarationValidationError(f"`{field_name}` must be an integer > 0.")


def validate_nonnegative_int(value: int, field_name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise DeclarationValidationError(f"`{field_name}` must be an integer >= 0.")


def validate_identifier(value: str, field_name: str) -> None:
    validate_identity(value, field_name)
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"`{field_name}` must be a simple SQL identifier owned by the runtime store."
        )


def validate_collect_id(value: str) -> None:
    if not is_uuidv7(value):
        raise DeclarationValidationError("`collect_id` must be a UUIDv7 string.")


def validate_allocated_collect_id(value: str) -> None:
    if not is_uuidv7(value):
        raise DeclarationValidationError("ordered work `collect_id` must be a UUIDv7 string.")


def validate_family(value: str) -> None:
    if value not in ("state", "event"):
        raise DeclarationValidationError("ordered work `family` must be either 'state' or 'event'.")


def validate_progress_scope(scope: DestinationProgressScope) -> None:
    if not isinstance(scope, DestinationProgressScope):
        raise DeclarationValidationError("progress scope must be a DestinationProgressScope.")
    validate_identity(scope.sync_name, "sync_name")
    validate_identity(scope.destination_name, "destination_name")
    validate_identity(scope.surface, "surface")
    validate_family(scope.family)
    validate_identity(scope.declaration_name, "declaration_name")


def validate_destination_progress_position(
    *,
    scope: DestinationProgressScope,
    position: ScanPosition | None,
) -> None:
    if position is None:
        return
    if position.family != scope.family:
        raise DeclarationValidationError(
            "Destination progress scan position family must match the progress scope family."
        )


def validate_max_rows(value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise DeclarationValidationError("`max_rows` must be an integer greater than 0.")


def validate_pending_work_cursor(
    cursor: PendingWorkCursor,
    lower_bound: StateOrderedWorkScanPosition | EventKeysetScanPosition | None,
) -> None:
    if not isinstance(cursor, PendingWorkCursor):
        raise DeclarationValidationError("pending work cursor must be a PendingWorkCursor.")
    validate_identity(cursor.token, "pending_work_cursor.token")
    validate_collect_id(cursor.collect_id)
    if (
        not isinstance(cursor.sequence_order, int)
        or isinstance(cursor.sequence_order, bool)
        or cursor.sequence_order < 0
    ):
        raise DeclarationValidationError("pending work cursor `sequence_order` must be >= 0.")
    if isinstance(lower_bound, EventKeysetScanPosition):
        return
    if lower_bound is not None and (
        cursor.collect_id,
        cursor.sequence_order,
    ) == (
        lower_bound.collect_id,
        lower_bound.sequence_order,
    ):
        return
    if not state_ordered_work_position_after(
        collect_id=cursor.collect_id,
        sequence_order=cursor.sequence_order,
        position=lower_bound,
    ):
        raise DeclarationValidationError(
            "pending work cursor must be ahead of destination progress."
        )


__all__ = [
    "validate_allocated_collect_id",
    "validate_collect_id",
    "validate_destination_progress_position",
    "validate_family",
    "validate_identifier",
    "validate_identity",
    "validate_max_rows",
    "validate_nonnegative_int",
    "validate_pending_work_cursor",
    "validate_positive_int",
    "validate_progress_scope",
]
