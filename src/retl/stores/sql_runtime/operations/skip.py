from __future__ import annotations

from sqlglot import exp, select

from retl.collect_identity import new_collect_id
from retl.errors import DeclarationValidationError
from retl.sql import (
    SqlCondition,
    SqlParamAllocator,
    column,
    render_sql,
    sql_and,
    sql_eq_param,
    table,
)
from retl.stores.contracts import (
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    DestinationScanRange,
    StateOrderedWorkScanPosition,
    destination_batch_id,
)
from retl.stores.sql_runtime import destination_batches as batch_store
from retl.stores.sql_runtime import progress as progress_store
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.operations import RuntimeOperationSummary


def dismiss_unresolved(
    context: SqlRuntimeContext,
    destination_batches: list[DestinationBatchRecord],
    *,
    scope: DestinationProgressScope,
) -> tuple[DestinationBatchRecord, ...]:
    return batch_store.dismiss_unresolved_destination_batches(
        context,
        destination_batches,
        scope=scope,
    )


def create_system_skip_batch(
    context: SqlRuntimeContext,
    destination_batches: list[DestinationBatchRecord],
    *,
    scope: DestinationProgressScope,
    scan_range: DestinationScanRange,
) -> DestinationBatchRecord:
    validation_helpers.validate_progress_scope(scope)
    if not isinstance(scan_range, DestinationScanRange):
        raise DeclarationValidationError("system skip batch requires a DestinationScanRange.")
    if scan_range.family != scope.family:
        raise DeclarationValidationError("skip range family must match the destination scope.")
    if scope.family == "event":
        first_collect_id = last_collect_id = new_collect_id()
        first_sequence_order = last_sequence_order = 0
        declaration_version_id = "system-skip"
    else:
        row = _range_ordered_work_bounds(context, scope=scope, scan_range=scan_range)
        if row is None:
            raise DeclarationValidationError(
                "skip range does not match retained State ordered work; retained State work "
                "is required to create skipped ledger evidence."
            )
        first_collect_id, last_collect_id, first_sequence_order, last_sequence_order = row
        declaration_version_id = _declaration_version_id(
            context,
            scope=scope,
            collect_id=first_collect_id,
            sequence_order=first_sequence_order,
        )
    identity = DestinationBatchIdentity(
        scope=scope,
        declaration_version_id=declaration_version_id,
        source_range=scan_range,
        source_page_index=0,
        reconcile_page_index=0,
        first_collect_id=first_collect_id,
        last_collect_id=last_collect_id,
        first_sequence_order=first_sequence_order,
        last_sequence_order=last_sequence_order,
        destination_batch_index=0,
        payload_fingerprint="system-skip",
        target_request_fingerprint="system-skip",
    )
    record = DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        status="skipped",
        completion_state="resolved",
        retry_eligible=False,
    )
    return batch_store.upsert_destination_batch(context, destination_batches, record)


def skip_range(
    context: SqlRuntimeContext,
    destination_batches: list[DestinationBatchRecord],
    *,
    scope: DestinationProgressScope,
    scan_range: DestinationScanRange,
) -> RuntimeOperationSummary:
    original_batches = list(destination_batches)
    try:
        with context.transaction():
            batch = create_system_skip_batch(
                context,
                destination_batches,
                scope=scope,
                scan_range=scan_range,
            )
            update = progress_store.update_destination_progress(
                context,
                scope=scope,
                position=scan_range.upper_bound_inclusive,
            )
    except BaseException:
        destination_batches[:] = original_batches
        raise
    return {
        "skipped_batch_id": batch.batch_id,
        "scope": {
            "sync_name": scope.sync_name,
            "destination_name": scope.destination_name,
            "surface": scope.surface,
            "family": scope.family,
            "declaration_name": scope.declaration_name,
        },
        "progress_before": update.before,
        "progress_after": update.after,
        "progress_advanced": update.advanced,
    }


def _range_ordered_work_bounds(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    scan_range: DestinationScanRange,
) -> tuple[str, str, int, int] | None:
    first = _range_ordered_work_edge(
        context,
        scope=scope,
        scan_range=scan_range,
        desc=False,
    )
    last = _range_ordered_work_edge(
        context,
        scope=scope,
        scan_range=scan_range,
        desc=True,
    )
    if first is None or last is None:
        return None
    return first[0], last[0], first[1], last[1]


def _range_ordered_work_edge(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    scan_range: DestinationScanRange,
    desc: bool,
) -> tuple[str, int] | None:
    params = context.new_params()
    query = (
        select(
            column("collect_id"),
            column("sequence_order"),
        )
        .from_(table(context.runtime_relation("ordered_work")))
        .where(
            sql_and(
                sql_eq_param("family", scope.family, params=params),
                sql_eq_param("declaration_name", scope.declaration_name, params=params),
                _range_condition(context, scan_range, params),
            )
        )
        .order_by(
            exp.Ordered(this=column("collect_id"), desc=desc),
            exp.Ordered(this=column("sequence_order"), desc=desc),
            exp.Ordered(this=column("work_id"), desc=desc),
        )
        .limit(1)
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    row = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0]), int(row[1])


def _declaration_version_id(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    collect_id: str,
    sequence_order: int,
) -> str:
    params = context.new_params()
    query = (
        select("declaration_version_id")
        .from_(table(context.runtime_relation("ordered_work")))
        .where(
            sql_and(
                sql_eq_param("family", scope.family, params=params),
                sql_eq_param("declaration_name", scope.declaration_name, params=params),
                sql_eq_param("collect_id", collect_id, params=params),
                sql_eq_param("sequence_order", sequence_order, params=params),
            )
        )
        .limit(1)
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    row = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if row is None or row[0] is None:
        return "unknown"
    return str(row[0])


def _range_condition(
    context: SqlRuntimeContext,
    scan_range: DestinationScanRange,
    params: SqlParamAllocator,
) -> SqlCondition:
    first = scan_range.first_record_position
    last = scan_range.last_record_position
    if isinstance(first, StateOrderedWorkScanPosition) and isinstance(
        last, StateOrderedWorkScanPosition
    ):
        condition = sql_and(
            exp.or_(
                column("collect_id") > params.add(first.collect_id),
                exp.and_(
                    column("collect_id").eq(params.add(first.collect_id)),
                    column("sequence_order") >= params.add(first.sequence_order),
                ),
            ),
            exp.or_(
                column("collect_id") < params.add(last.collect_id),
                exp.and_(
                    column("collect_id").eq(params.add(last.collect_id)),
                    column("sequence_order") <= params.add(last.sequence_order),
                ),
            ),
        )
        if condition is None:
            raise DeclarationValidationError("skip range requires ordered-work bounds.")
        return condition
    _ = context
    raise DeclarationValidationError("skip range scan positions are not supported.")


__all__ = [
    "create_system_skip_batch",
    "dismiss_unresolved",
    "skip_range",
]
