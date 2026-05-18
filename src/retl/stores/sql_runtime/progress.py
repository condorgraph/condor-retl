from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp

from retl.errors import DeclarationValidationError
from retl.sql import render_sql, scalar_read, sql_and, sql_eq_param, upsert_assignment
from retl.stores.contracts import (
    DestinationProgress,
    DestinationProgressScope,
    DestinationProgressUpdate,
    ScanPosition,
    compare_scan_positions,
)
from retl.stores.sql_runtime import positions as position_helpers
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.errors import RuntimeStoreError
from retl.stores.sql_runtime.writes import execute_runtime_insert, execute_runtime_update


def register_destination_progress(
    context: SqlRuntimeContext,
    scope: DestinationProgressScope,
) -> DestinationProgress:
    validation_helpers.validate_progress_scope(scope)
    return get_destination_progress(context, scope)


def get_destination_progress(
    context: SqlRuntimeContext,
    scope: DestinationProgressScope,
) -> DestinationProgress:
    validation_helpers.validate_progress_scope(scope)
    row = _read_destination_progress_row(context, scope)
    return DestinationProgress(scope=scope, position=row.position)


def _read_destination_progress_row(
    context: SqlRuntimeContext,
    scope: DestinationProgressScope,
) -> "_DestinationProgressRow":
    params = context.new_params()
    scope_values = progress_scope_values(scope)
    query = scalar_read(
        context.runtime_relation("destination_progress"),
        "position_json",
        where=sql_and(
            sql_eq_param("sync_name", scope_values[0], params=params),
            sql_eq_param("destination_name", scope_values[1], params=params),
            sql_eq_param("surface", scope_values[2], params=params),
            sql_eq_param("family", scope_values[3], params=params),
            sql_eq_param("declaration_name", scope_values[4], params=params),
        ),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    record = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if record is None:
        return _DestinationProgressRow(exists=False, position=None)
    return _DestinationProgressRow(
        exists=True,
        position=position_helpers.scan_position_from_storage_json(
            record[0], field_name="position_json"
        ),
    )


def update_destination_progress(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    position: ScanPosition | None,
    advance: bool = True,
    current_position: ScanPosition | None = None,
    current_position_loaded: bool = False,
) -> DestinationProgressUpdate:
    validation_helpers.validate_progress_scope(scope)
    validation_helpers.validate_destination_progress_position(scope=scope, position=position)
    if not isinstance(advance, bool):
        raise DeclarationValidationError("`advance` must be a boolean.")
    current = (
        _DestinationProgressRow(exists=current_position is not None, position=current_position)
        if current_position_loaded
        else _read_destination_progress_row(context, scope)
    )
    before = current.position
    if not advance or position == before:
        return DestinationProgressUpdate(
            scope=scope,
            before=before,
            after=before,
            advanced=False,
        )
    if position is None:
        raise DeclarationValidationError(
            "Destination progress cannot move from a committed scan position back to None."
        )
    if before is not None:
        try:
            comparison = compare_scan_positions(position, before)
        except ValueError as exc:
            raise DeclarationValidationError(str(exc)) from exc
        if comparison < 0:
            raise DeclarationValidationError(
                "Destination progress cannot move behind the current scan position."
            )
    if not current.exists:
        execute_runtime_insert(
            context,
            "destination_progress",
            (
                ("sync_name", scope.sync_name),
                ("destination_name", scope.destination_name),
                ("surface", scope.surface),
                ("family", scope.family),
                ("declaration_name", scope.declaration_name),
                ("position_json", position_helpers.scan_position_to_storage_json(position)),
            ),
        )
    else:
        execute_runtime_update(
            context,
            "destination_progress",
            (("position_json", position_helpers.scan_position_to_storage_json(position)),),
            where_values=tuple(
                zip(_PROGRESS_SCOPE_COLUMNS, progress_scope_values(scope), strict=True)
            ),
            update_assignments=(upsert_assignment("updated_at", exp.CurrentTimestamp()),),
        )
        if (
            not current_position_loaded
            and get_destination_progress(context, scope).position is None
        ):
            raise RuntimeStoreError("Destination progress update did not match an existing row.")
    return DestinationProgressUpdate(
        scope=scope,
        before=before,
        after=position,
        advanced=True,
    )


def progress_scope_values(scope: DestinationProgressScope) -> list[str]:
    return [
        scope.sync_name,
        scope.destination_name,
        scope.surface,
        scope.family,
        scope.declaration_name,
    ]


@dataclass(frozen=True)
class _DestinationProgressRow:
    exists: bool
    position: ScanPosition | None


_PROGRESS_SCOPE_COLUMNS = (
    "sync_name",
    "destination_name",
    "surface",
    "family",
    "declaration_name",
)


__all__ = [
    "get_destination_progress",
    "progress_scope_values",
    "register_destination_progress",
    "update_destination_progress",
]
