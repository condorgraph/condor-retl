from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from retl.declarations import State
from retl.errors import DeclarationValidationError
from retl.stores.contracts import (
    StateProductionResult,
    StateProductionStore,
    StateSnapshotRequest,
    StateSnapshotSource,
)


@dataclass(frozen=True)
class StateCollectEvidence:
    phase: Literal["collect"]
    status: Literal["completed"]
    collect_id: str
    declaration_name: str
    source_name: str
    current_row_count: int
    work_row_count: int
    upsert_count: int
    remove_count: int


def produce_state_collect(
    *,
    declaration: State,
    store: StateProductionStore,
) -> StateCollectEvidence:
    if not isinstance(declaration, State):
        raise DeclarationValidationError("State producer requires a State declaration.")
    backend = declaration.source.backend
    if backend is None:
        raise DeclarationValidationError("State producer requires a Source backend.")
    adapter = cast(Any, backend).adapter()
    if not isinstance(adapter, StateSnapshotSource):
        raise DeclarationValidationError(
            "State producer requires a Source adapter that can prepare State snapshots."
        )
    result = cast(
        StateProductionResult,
        store.produce_state_collect(
            declaration=declaration,
            snapshot=adapter.prepare_state_snapshot(
                StateSnapshotRequest(
                    source_name=declaration.source.name,
                    query=declaration.source.query,
                )
            ),
        ),
    )
    return StateCollectEvidence(
        phase="collect",
        status="completed",
        collect_id=result.collect_id,
        declaration_name=result.declaration_name,
        source_name=result.source_name,
        current_row_count=result.current_row_count,
        work_row_count=result.work_row_count,
        upsert_count=result.upsert_count,
        remove_count=result.remove_count,
    )


__all__ = ["StateCollectEvidence", "produce_state_collect"]
