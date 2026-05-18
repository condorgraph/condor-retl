from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from retl.declarations import DestinationBinding, Event, State, Sync
from retl.errors import DeclarationValidationError
from retl.runtime.results import PhaseEvidence, PhaseStatus
from retl.runtime.staging import (
    StageEvidence,
    StagePageBoundary,
    StageWorkPage,
)
from retl.stores.contracts import DestinationProgressScope

ReconcileMode: TypeAlias = Literal["pending", "resend_all"]

_REQUIRED_STAGED_PAYLOAD_COLUMNS = frozenset(
    {
        "work_id",
        "collect_id",
        "sequence_order",
        "family",
        "kind",
        "declaration_name",
        "key_json",
        "target_json",
        "identifiers_json",
        "payload_json",
    }
)
_SKIPPED_REMOVE_SAMPLE_SIZE = 5


@dataclass(frozen=True)
class SkippedRemoveEvidence:
    work_id: str
    collect_id: str
    sequence_order: int
    record_identity: str
    reason: str


@dataclass(frozen=True)
class StateOperationPage:
    phase: Literal["reconcile"]
    family: Literal["state_operations"]
    scope: DestinationProgressScope
    mode: ReconcileMode
    input_stage_boundary: StagePageBoundary
    payload: pa.RecordBatch
    row_count: int
    upsert_count: int
    remove_count: int
    skipped_remove_count: int
    skipped_removes: tuple[SkippedRemoveEvidence, ...]
    progress_before: str | None
    progress_boundary: StagePageBoundary
    next_cursor: object | None
    safe_to_advance_collect_id: bool


@dataclass(frozen=True)
class EventImportPage:
    phase: Literal["reconcile"]
    family: Literal["event_imports"]
    scope: DestinationProgressScope
    mode: ReconcileMode
    input_stage_boundary: StagePageBoundary
    payload: pa.RecordBatch
    row_count: int
    import_count: int
    progress_before: str | None
    progress_boundary: StagePageBoundary
    next_cursor: object | None
    safe_to_advance_collect_id: bool
    event_cursor_kind: str
    event_primary_key_kind: str


@dataclass(frozen=True)
class StateReconcileEvidence:
    phase: Literal["reconcile"]
    status: Literal["succeeded"]
    phase_status: PhaseStatus
    sync_name: str
    scope: DestinationProgressScope
    mode: ReconcileMode
    input_stage_boundary: StagePageBoundary
    operation_pages: tuple[StateOperationPage, ...]
    pages: tuple[StateOperationPage, ...]
    operation_count: int
    upsert_count: int
    remove_count: int
    skipped_remove_count: int
    skipped_removes: tuple[SkippedRemoveEvidence, ...]
    progress_before: str | None
    progress_boundary: StagePageBoundary
    next_cursor: object | None
    safe_to_advance_collect_id: bool
    dry_run: bool = False


@dataclass(frozen=True)
class EventReconcilePageEvidence:
    phase: Literal["reconcile"]
    status: Literal["succeeded"]
    phase_status: PhaseStatus
    sync_name: str
    scope: DestinationProgressScope
    mode: ReconcileMode
    input_stage_boundary: StagePageBoundary
    import_pages: tuple[EventImportPage, ...]
    pages: tuple[EventImportPage, ...]
    import_count: int
    progress_before: str | None
    progress_boundary: StagePageBoundary
    next_cursor: object | None
    safe_to_advance_collect_id: bool
    event_cursor_kind: str
    event_primary_key_kind: str
    dry_run: bool = False


def reconcile_state_operations(
    *,
    sync: Sync,
    staged: StageEvidence | StageWorkPage,
    dry_run: bool | None = None,
) -> StateReconcileEvidence:
    if not isinstance(sync.declaration, State):
        raise DeclarationValidationError("State reconcile requires a State Sync.")
    page = _stage_page(staged)
    _validate_staged_page(sync=sync, page=page, family="state")
    _validate_column_values(
        payload=page.payload,
        column_name="family",
        expected="state",
        label="State staged work family",
    )
    _validate_column_values(
        payload=page.payload,
        column_name="declaration_name",
        expected=sync.declaration.name,
        label="State staged work declaration",
    )
    _validate_state_kinds(page)

    upsert_mask = _equals_mask(page.payload, "kind", "upsert")
    remove_mask = _equals_mask(page.payload, "kind", "remove")
    upsert_count = _true_count(upsert_mask)
    staged_remove_count = _true_count(remove_mask)
    skipped_remove_count = 0
    skipped_removes: tuple[SkippedRemoveEvidence, ...] = ()
    output_payload = _with_operation_column(page.payload)

    if staged_remove_count:
        if _state_removes_allowed(sync):
            remove_count = staged_remove_count
        else:
            output_payload = _with_operation_column(page.payload.filter(upsert_mask))
            remove_count = 0
            skipped_remove_count = staged_remove_count
            skipped_removes = _skipped_remove_sample(page.payload.filter(remove_mask))
    else:
        remove_count = 0

    dry_run_value = _dry_run(staged, dry_run)
    operation_page = StateOperationPage(
        phase="reconcile",
        family="state_operations",
        scope=page.scope,
        mode=page.mode,
        input_stage_boundary=page.boundary,
        payload=output_payload,
        row_count=output_payload.num_rows,
        upsert_count=upsert_count,
        remove_count=remove_count,
        skipped_remove_count=skipped_remove_count,
        skipped_removes=skipped_removes,
        progress_before=page.progress_before,
        progress_boundary=page.boundary,
        next_cursor=page.next_cursor,
        safe_to_advance_collect_id=page.safe_to_advance_collect_id,
    )
    return StateReconcileEvidence(
        phase="reconcile",
        status="succeeded",
        phase_status=_phase_status(
            message=(
                f"Reconciled {operation_page.row_count} State operation row(s) in {page.mode} mode."
            ),
            dry_run=dry_run_value,
        ),
        sync_name=sync.name,
        scope=page.scope,
        mode=page.mode,
        input_stage_boundary=page.boundary,
        operation_pages=(operation_page,),
        pages=(operation_page,),
        operation_count=operation_page.row_count,
        upsert_count=upsert_count,
        remove_count=remove_count,
        skipped_remove_count=skipped_remove_count,
        skipped_removes=skipped_removes,
        progress_before=page.progress_before,
        progress_boundary=page.boundary,
        next_cursor=page.next_cursor,
        safe_to_advance_collect_id=page.safe_to_advance_collect_id,
        dry_run=dry_run_value,
    )


def reconcile_event_imports(
    *,
    sync: Sync,
    staged: StageEvidence | StageWorkPage,
    dry_run: bool | None = None,
) -> EventReconcilePageEvidence:
    if not isinstance(sync.declaration, Event):
        raise DeclarationValidationError("Event reconcile requires an Event Sync.")
    page = _stage_page(staged)
    _validate_staged_page(sync=sync, page=page, family="event")
    if page.mode != "pending":
        raise DeclarationValidationError("Event reconcile requires pending staged work.")
    _validate_column_values(
        payload=page.payload,
        column_name="family",
        expected="event",
        label="Event staged work family",
    )
    _validate_column_values(
        payload=page.payload,
        column_name="kind",
        expected="import",
        label="Event staged work kind",
    )
    _validate_column_values(
        payload=page.payload,
        column_name="declaration_name",
        expected=sync.declaration.name,
        label="Event staged work declaration",
    )

    import_count = _true_count(_equals_mask(page.payload, "kind", "import"))
    dry_run_value = _dry_run(staged, dry_run)
    checkpoint = sync.declaration.source.checkpoint
    if checkpoint is None:
        raise DeclarationValidationError("Event declaration requires checkpoint types.")
    event_cursor_kind = checkpoint["cursor_type"]
    event_primary_key_kind = checkpoint["primary_key_type"]
    import_page = EventImportPage(
        phase="reconcile",
        family="event_imports",
        scope=page.scope,
        mode=page.mode,
        input_stage_boundary=page.boundary,
        payload=page.payload,
        row_count=page.payload.num_rows,
        import_count=import_count,
        progress_before=page.progress_before,
        progress_boundary=page.boundary,
        next_cursor=page.next_cursor,
        safe_to_advance_collect_id=page.safe_to_advance_collect_id,
        event_cursor_kind=event_cursor_kind,
        event_primary_key_kind=event_primary_key_kind,
    )
    return EventReconcilePageEvidence(
        phase="reconcile",
        status="succeeded",
        phase_status=_phase_status(
            message=f"Reconciled {import_count} Event import row(s).",
            dry_run=dry_run_value,
        ),
        sync_name=sync.name,
        scope=page.scope,
        mode=page.mode,
        input_stage_boundary=page.boundary,
        import_pages=(import_page,),
        pages=(import_page,),
        import_count=import_count,
        progress_before=page.progress_before,
        progress_boundary=page.boundary,
        next_cursor=page.next_cursor,
        safe_to_advance_collect_id=page.safe_to_advance_collect_id,
        event_cursor_kind=event_cursor_kind,
        event_primary_key_kind=event_primary_key_kind,
        dry_run=dry_run_value,
    )


def _stage_page(staged: StageEvidence | StageWorkPage) -> StageWorkPage:
    if isinstance(staged, StageEvidence):
        return staged.page
    if isinstance(staged, StageWorkPage):
        return staged
    raise DeclarationValidationError("Reconcile requires StageEvidence or StageWorkPage.")


def _validate_staged_page(
    *,
    sync: Sync,
    page: StageWorkPage,
    family: Literal["state", "event"],
) -> None:
    if page.phase != "stage":
        raise DeclarationValidationError("Reconcile requires staged work input.")
    expected = _expected_scope(sync)
    if page.scope != expected:
        raise DeclarationValidationError(
            "Staged work scope does not match the Sync declaration, destination, or surface."
        )
    if page.scope.family != family:
        raise DeclarationValidationError("Staged work family does not match reconcile family.")
    if not isinstance(page.payload, pa.RecordBatch):
        raise DeclarationValidationError("Staged work payload must be a pyarrow.RecordBatch.")
    if page.payload.num_rows != page.row_count:
        raise DeclarationValidationError(
            "Staged work payload row count does not match Stage metadata."
        )
    _require_columns(page.payload, _REQUIRED_STAGED_PAYLOAD_COLUMNS)
    if page.scope.family != family:
        raise DeclarationValidationError("Staged work family does not match reconcile family.")


def _validate_state_kinds(page: StageWorkPage) -> None:
    allowed_mask = pc.or_(
        _equals_mask(page.payload, "kind", "upsert"),
        _equals_mask(page.payload, "kind", "remove"),
    )
    if _true_count(allowed_mask) != page.payload.num_rows:
        raise DeclarationValidationError(
            "State reconcile requires staged work kind values to be `upsert` or `remove`."
        )
    if page.mode == "resend_all" and _true_count(_equals_mask(page.payload, "kind", "remove")):
        raise DeclarationValidationError("State resend-all reconcile only accepts upsert work.")


def _validate_column_values(
    *,
    payload: pa.RecordBatch,
    column_name: str,
    expected: str,
    label: str,
) -> None:
    if _true_count(_equals_mask(payload, column_name, expected)) != payload.num_rows:
        raise DeclarationValidationError(f"{label} values must all be `{expected}`.")


def _equals_mask(payload: pa.RecordBatch, column_name: str, expected: str) -> pa.BooleanArray:
    return pc.fill_null(pc.equal(payload.column(column_name), expected), False)


def _true_count(mask: pa.Array) -> int:
    value = pc.sum(pc.cast(mask, pa.int64())).as_py()
    return int(value or 0)


def _with_operation_column(payload: pa.RecordBatch) -> pa.RecordBatch:
    if "operation" in payload.schema.names:
        index = payload.schema.get_field_index("operation")
        return payload.set_column(index, "operation", payload.column("kind"))
    return payload.append_column("operation", payload.column("kind"))


def _skipped_remove_sample(payload: pa.RecordBatch) -> tuple[SkippedRemoveEvidence, ...]:
    sample_size = min(payload.num_rows, _SKIPPED_REMOVE_SAMPLE_SIZE)
    sample = payload.slice(0, sample_size)
    return tuple(
        SkippedRemoveEvidence(
            work_id=str(_scalar(sample, "work_id", index)),
            collect_id=str(_scalar(sample, "collect_id", index)),
            sequence_order=int(_scalar(sample, "sequence_order", index)),
            record_identity=str(_scalar(sample, "key_json", index)),
            reason="selected surface supports upsert only",
        )
        for index in range(sample_size)
    )


def _scalar(payload: pa.RecordBatch, column_name: str, index: int) -> Any:
    return payload.column(column_name)[index].as_py()


def _expected_scope(sync: Sync) -> DestinationProgressScope:
    from retl.runtime.progress import destination_progress_scope

    return destination_progress_scope(sync)


def _state_removes_allowed(sync: Sync) -> bool:
    if "remove" not in tuple(sync.operations or ()):
        return False
    surface = _selected_destination_surface(sync)
    supported = getattr(surface, "supported_operations", None)
    if supported is None:
        raise DeclarationValidationError(
            f"State reconcile cannot prove supported operations for surface `{sync.surface}`."
        )
    supported_operations = tuple(supported)
    if "remove" in supported_operations:
        return True
    if "upsert" in supported_operations:
        return False
    raise DeclarationValidationError(
        f"State reconcile cannot prove State supported operations for surface `{sync.surface}`."
    )


def _selected_destination_surface(sync: Sync) -> object:
    surface_lookup = None
    if isinstance(sync.destination, DestinationBinding):
        surface_lookup = sync.destination.surface
    else:
        surface_lookup = getattr(sync.destination, "surface", None)
    if not callable(surface_lookup):
        raise DeclarationValidationError(
            "State reconcile requires a destination binding that can resolve the selected surface."
        )
    try:
        return surface_lookup(sync.surface)
    except KeyError as exc:
        raise DeclarationValidationError(
            f"State reconcile could not resolve selected surface `{sync.surface}`."
        ) from exc


def _require_columns(payload: pa.RecordBatch, names: frozenset[str]) -> None:
    missing = sorted(names - set(payload.schema.names))
    if missing:
        missing_text = ", ".join(missing)
        raise DeclarationValidationError(
            f"Staged work payload is missing required column(s): {missing_text}."
        )


def _phase_status(*, message: str, dry_run: bool) -> PhaseStatus:
    return PhaseStatus(
        name="reconcile",
        status="succeeded",
        evidence=PhaseEvidence(
            kind="planned",
            message=message,
            dry_run=dry_run,
        ),
    )


def _dry_run(staged: object, override: bool | None) -> bool:
    if override is not None:
        return override
    return bool(getattr(staged, "dry_run", False))


__all__ = [
    "EventImportPage",
    "EventReconcilePageEvidence",
    "ReconcileMode",
    "SkippedRemoveEvidence",
    "StateOperationPage",
    "StateReconcileEvidence",
    "reconcile_event_imports",
    "reconcile_state_operations",
]
