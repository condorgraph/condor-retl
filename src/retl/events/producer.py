from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from retl.declarations import Event
from retl.errors import DeclarationValidationError
from retl.stores.contracts import (
    EventKeysetScanPosition,
    EventProductionResult,
    EventProductionStore,
    EventSourceWindowRequest,
    EventSourceWindowSource,
)


@dataclass(frozen=True)
class EventCollectEvidence:
    phase: Literal["collect"]
    status: Literal["completed"]
    collect_id: str | None
    declaration_name: str
    source_name: str
    scan_after: EventKeysetScanPosition | None
    scan_upper_bound: EventKeysetScanPosition | None
    window_row_count: int
    work_row_count: int
    import_count: int
    duplicate_risk_count: int


def produce_event_collect(
    *,
    declaration: Event,
    store: EventProductionStore,
    scan_after: EventKeysetScanPosition | None = None,
    limit: int | None = None,
) -> EventCollectEvidence:
    if not isinstance(declaration, Event):
        raise DeclarationValidationError("Event producer requires an Event declaration.")
    backend = declaration.source.backend
    if backend is None:
        raise DeclarationValidationError("Event producer requires a Source backend.")
    checkpoint = declaration.source.checkpoint
    if checkpoint is None:
        raise DeclarationValidationError("Event producer requires a checkpointed Source.")

    adapter = cast(Any, backend).adapter()
    if not isinstance(adapter, EventSourceWindowSource):
        raise DeclarationValidationError(
            "Event producer requires a Source adapter that can prepare Event source windows."
        )

    result = cast(
        EventProductionResult,
        store.produce_event_collect(
            declaration=declaration,
            window=adapter.prepare_event_source_window(
                EventSourceWindowRequest(
                    source_name=declaration.source.name,
                    query=declaration.source.query,
                    cursor_column=checkpoint["cursor"],
                    primary_key_column=checkpoint["primary_key"],
                    scan_after=scan_after,
                    limit=limit,
                )
            ),
        ),
    )
    return EventCollectEvidence(
        phase="collect",
        status="completed",
        collect_id=result.collect_id,
        declaration_name=result.declaration_name,
        source_name=result.source_name,
        scan_after=result.scan_after,
        scan_upper_bound=result.scan_upper_bound,
        window_row_count=result.window_row_count,
        work_row_count=result.work_row_count,
        import_count=result.work_row_count,
        duplicate_risk_count=result.duplicate_risk_count,
    )


__all__ = ["EventCollectEvidence", "produce_event_collect"]
