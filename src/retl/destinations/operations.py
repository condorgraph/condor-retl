"""Destination operation contracts for surface compatibility."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal, TypeAlias

from retl.declarations import State, StateOperation, Sync

EventOperation: TypeAlias = Literal["import"]
SurfaceOperation: TypeAlias = StateOperation | EventOperation
DestinationWorkFamily: TypeAlias = Literal["state_operations", "event_imports"]

STATE_OPERATIONS: tuple[StateOperation, ...] = ("upsert", "remove")
EVENT_OPERATIONS: tuple[EventOperation, ...] = ("import",)


def normalize_state_operations(operations: Iterable[str]) -> tuple[StateOperation, ...]:
    normalized: list[StateOperation] = []
    for operation in operations:
        if operation not in STATE_OPERATIONS:
            raise ValueError("State Operations must be 'upsert' or 'remove'.")
        if operation not in normalized:
            normalized.append(operation)  # type: ignore[arg-type]
    return tuple(normalized)


def planned_state_operations(sync: Sync) -> tuple[StateOperation, ...]:
    """Return the State Operation families a Sync can produce by configuration."""

    if not isinstance(sync.declaration, State):
        return ()
    return tuple(sync.operations or STATE_OPERATIONS)


__all__ = [
    "DestinationWorkFamily",
    "EVENT_OPERATIONS",
    "EventOperation",
    "STATE_OPERATIONS",
    "StateOperation",
    "SurfaceOperation",
    "normalize_state_operations",
    "planned_state_operations",
]
