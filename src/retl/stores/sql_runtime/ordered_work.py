from __future__ import annotations

import uuid
from typing import Any, cast

import pyarrow as pa  # type: ignore[import-untyped]
from sqlglot import exp, select

from retl.errors import DeclarationValidationError
from retl.sql import (
    CompiledSql,
    SqlCondition,
    SqlDialectCapabilities,
    SqlParamAllocator,
    SqlRenderable,
    column,
    count_read,
    filtered_delete,
    list_read,
    max_read,
    min_read,
    render_sql,
    row_insert,
    row_read,
    row_write_input,
    sql_and,
    sql_eq_param,
    table,
)
from retl.stores.contracts import (
    DestinationProgressScope,
    EventKeysetScanPosition,
    OrderedWorkRetentionCleanup,
    PendingWorkCursor,
    PendingWorkPage,
    ScanPosition,
    StateCurrentSnapshotScanPosition,
    StateOrderedWorkScanPosition,
    WorkFamily,
)
from retl.stores.sql_runtime import arrow as arrow_helpers
from retl.stores.sql_runtime import positions as position_helpers
from retl.stores.sql_runtime import progress as progress_store
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.errors import RuntimeStoreError
from retl.stores.sql_runtime.progress import progress_scope_values

_PENDING_WORK_COLUMNS = (
    "work_id",
    "collect_id",
    "sequence_order",
    "family",
    "kind",
    "declaration_name",
    "declaration_version_id",
    "key_json",
    "target_json",
    "identifiers_json",
    "payload_json",
)


def read_pending_work(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    max_rows: int,
    cursor: PendingWorkCursor | None = None,
    source_collect_id: str | None = None,
    progress_position: ScanPosition | None = None,
    progress_position_loaded: bool = False,
) -> PendingWorkPage:
    validation_helpers.validate_progress_scope(scope)
    validation_helpers.validate_max_rows(max_rows)
    if source_collect_id is not None:
        validation_helpers.validate_collect_id(source_collect_id)
    if not progress_position_loaded:
        progress_position = progress_store.get_destination_progress(context, scope).position
    lower_bound = pending_work_lower_bound(progress_position)
    if cursor is not None:
        validation_helpers.validate_pending_work_cursor(cursor, lower_bound)
        validate_stored_pending_work_cursor(
            context,
            scope=scope,
            lower_bound=lower_bound,
            cursor=cursor,
        )
        return read_pending_work_after_cursor(
            context,
            scope=scope,
            max_rows=max_rows,
            cursor=cursor,
        )

    compiled = compile_pending_work_read(
        context,
        scope=scope,
        lower_bound=lower_bound,
        source_collect_id=source_collect_id,
        limit=max_rows + 1,
    )
    fetched = arrow_helpers.fetch_bounded_record_batch(
        context.connection.execute(compiled.sql, compiled.params),
        row_limit=max_rows + 1,
    )
    if fetched.num_rows <= max_rows:
        return pending_page_from_batch(fetched)

    included = fetched.slice(0, max_rows)
    lookahead_collect_id = arrow_helpers.string_value(fetched, "collect_id", max_rows)
    if (
        included.num_rows == 0
        or arrow_helpers.last_string_value(included, "collect_id") != lookahead_collect_id
    ):
        return pending_page_from_batch(included)

    final_collect_id = arrow_helpers.last_string_value(included, "collect_id")
    complete_row_count = arrow_helpers.rows_before_collect_id(included, final_collect_id)
    if complete_row_count:
        return pending_page_from_batch(included.slice(0, complete_row_count))
    return pending_page_with_cursor(context, scope=scope, payload=included)


def read_pending_work_after_cursor(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    max_rows: int,
    cursor: PendingWorkCursor,
) -> PendingWorkPage:
    compiled = compile_pending_work_after_cursor_read(
        context,
        scope=scope,
        cursor=cursor,
        limit=max_rows + 1,
    )
    fetched = arrow_helpers.fetch_bounded_record_batch(
        context.connection.execute(compiled.sql, compiled.params),
        row_limit=max_rows + 1,
    )
    if fetched.num_rows <= max_rows:
        return pending_page_from_batch(fetched)
    return pending_page_with_cursor(context, scope=scope, payload=fetched.slice(0, max_rows))


def validate_stored_pending_work_cursor(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    lower_bound: StateOrderedWorkScanPosition | EventKeysetScanPosition | None,
    cursor: PendingWorkCursor,
) -> None:
    first_pending = first_pending_collect_id(context, scope=scope, lower_bound=lower_bound)
    if first_pending is None or cursor.collect_id != first_pending:
        raise DeclarationValidationError(
            "pending work cursor must belong to the first pending collect ID."
        )
    params = context.new_params()
    scope_values = progress_scope_values(scope)
    query = row_read(
        context.runtime_relation("pending_work_cursors"),
        ("collect_id", "sequence_order"),
        where=sql_and(
            sql_eq_param("token", cursor.token, params=params),
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
        raise DeclarationValidationError("pending work cursor was not issued by this store.")
    if str(record[0]) != cursor.collect_id or int(record[1]) != cursor.sequence_order:
        raise DeclarationValidationError("pending work cursor does not match store state.")


def first_pending_collect_id(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    lower_bound: StateOrderedWorkScanPosition | EventKeysetScanPosition | None,
) -> str | None:
    compiled = compile_first_pending_collect_id_read(
        context,
        scope=scope,
        lower_bound=lower_bound,
    )
    record = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if record is None or record[0] is None:
        return None
    return str(record[0])


def retention_watermark(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    progress_positions: tuple[ScanPosition | None, ...] | None = None,
) -> str | None:
    validation_helpers.validate_family(family)
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    if progress_positions is None:
        params = context.new_params()
        query = list_read(
            context.runtime_relation("destination_progress"),
            "position_json",
            where=sql_and(
                sql_eq_param("family", family, params=params),
                sql_eq_param("declaration_name", declaration_name, params=params),
            ),
        )
        compiled = render_sql(query, dialect=context.dialect, params=params)
        position_records = context.connection.execute(compiled.sql, compiled.params).fetchall()
        progress_positions = tuple(
            position_helpers.scan_position_from_storage_json(
                position_json,
                field_name="position_json",
            )
            for (position_json,) in position_records
        )
    else:
        progress_positions = tuple(position for position in progress_positions if position)
    if not progress_positions:
        return None
    progress_collect_ids: list[str] = []
    for position in progress_positions:
        collect_id = retention_collect_id_from_position(
            context,
            family=family,
            declaration_name=declaration_name,
            position=position,
        )
        if collect_id is None:
            return None
        progress_collect_ids.append(collect_id)
    if not progress_collect_ids:
        return None
    scanned_through = min(progress_collect_ids)
    blocker_before = oldest_unresolved_destination_batch_collect_id(
        context,
        family=family,
        declaration_name=declaration_name,
        through_collect_id=scanned_through,
    )
    if blocker_before is None:
        return scanned_through
    return previous_collect_id(
        context,
        family=family,
        declaration_name=declaration_name,
        before_collect_id=blocker_before,
    )


def cleanup_ordered_work(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    through_collect_id: str | None = None,
    dry_run: bool = False,
) -> OrderedWorkRetentionCleanup:
    validation_helpers.validate_family(family)
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    if through_collect_id is not None:
        validation_helpers.validate_collect_id(through_collect_id)
    if not isinstance(dry_run, bool):
        raise DeclarationValidationError("`dry_run` must be a boolean.")

    watermark = retention_watermark(context, family=family, declaration_name=declaration_name)
    safe_through_collect_id = retention_safe_collect_id(
        watermark=watermark,
        requested=through_collect_id,
    )
    delete_count = ordered_work_count_through(
        context,
        family=family,
        declaration_name=declaration_name,
        through_collect_id=safe_through_collect_id,
    )
    retained_pending_count = ordered_work_count_after(
        context,
        family=family,
        declaration_name=declaration_name,
        after_collect_id=safe_through_collect_id,
    )
    if safe_through_collect_id is not None and not dry_run:
        params = context.new_params()
        query = filtered_delete(
            context.runtime_relation("ordered_work"),
            where=and_conditions(
                [
                    sql_eq_param("family", family, params=params),
                    sql_eq_param("declaration_name", declaration_name, params=params),
                    exp.LTE(
                        this=column("collect_id"),
                        expression=params.add(safe_through_collect_id),
                    ),
                ]
            ),
        )
        compiled = render_sql(query, dialect=context.dialect, params=params)
        context.connection.execute(compiled.sql, compiled.params)
    return OrderedWorkRetentionCleanup(
        family=family,
        declaration_name=declaration_name,
        requested_through_collect_id=through_collect_id,
        safe_through_collect_id=safe_through_collect_id,
        deleted_ordered_work_count=delete_count,
        retained_pending_count=retained_pending_count,
        dry_run=dry_run,
    )


def ordered_work_count_through(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    through_collect_id: str | None,
) -> int:
    if through_collect_id is None:
        return 0
    params = context.new_params()
    query = count_read(
        context.runtime_relation("ordered_work"),
        where=sql_and(
            sql_eq_param("family", family, params=params),
            sql_eq_param("declaration_name", declaration_name, params=params),
            exp.LTE(this=column("collect_id"), expression=params.add(through_collect_id)),
        ),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    record = context.connection.execute(compiled.sql, compiled.params).fetchone()
    return int(record[0]) if record is not None else 0


def ordered_work_count_after(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    after_collect_id: str | None,
) -> int:
    params = context.new_params()
    query = count_read(
        context.runtime_relation("ordered_work"),
        where=sql_and(
            sql_eq_param("family", family, params=params),
            sql_eq_param("declaration_name", declaration_name, params=params),
            collect_id_gt(after_collect_id, params),
        ),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    record = context.connection.execute(compiled.sql, compiled.params).fetchone()
    return int(record[0]) if record is not None else 0


def collect_id_gt(
    value: str | None,
    params: SqlParamAllocator,
) -> SqlCondition | None:
    if value is None:
        return None
    return exp.GT(this=column("collect_id"), expression=params.add(value))


def retention_collect_id_from_position(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    position: ScanPosition | None,
) -> str | None:
    if not isinstance(position, StateOrderedWorkScanPosition):
        return None
    max_sequence_order = ordered_work_max_sequence_order(
        context,
        family=family,
        declaration_name=declaration_name,
        collect_id=position.collect_id,
    )
    if max_sequence_order is None:
        # The collect may already have been cleaned in an older run. Treat the
        # watermark as idempotently safe for this collect; there are no
        # remaining ordered_work rows in that collect for this cleanup to delete.
        return position.collect_id
    if position.sequence_order >= max_sequence_order:
        return position.collect_id
    return previous_collect_id(
        context,
        family=family,
        declaration_name=declaration_name,
        before_collect_id=position.collect_id,
    )


def previous_collect_id(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    before_collect_id: str,
) -> str | None:
    params = context.new_params()
    query = max_read(
        context.runtime_relation("ordered_work"),
        "collect_id",
        where=sql_and(
            sql_eq_param("family", family, params=params),
            sql_eq_param("declaration_name", declaration_name, params=params),
            column("collect_id") < params.add(before_collect_id),
        ),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    record = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if record is None:
        raise RuntimeStoreError("Runtime store did not return previous collect_id.")
    return optional_str(record[0])


def ordered_work_max_sequence_order(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    collect_id: str,
) -> int | None:
    params = context.new_params()
    query = max_read(
        context.runtime_relation("ordered_work"),
        "sequence_order",
        where=sql_and(
            sql_eq_param("family", family, params=params),
            sql_eq_param("declaration_name", declaration_name, params=params),
            sql_eq_param("collect_id", collect_id, params=params),
        ),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    record = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if record is None:
        raise RuntimeStoreError("DuckDB did not return ordered work max sequence_order.")
    return optional_int(record[0])


def oldest_unresolved_destination_batch_collect_id(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    through_collect_id: str,
) -> str | None:
    params = context.new_params()
    query = min_read(
        context.runtime_relation("destination_batches"),
        "first_collect_id",
        where=sql_and(
            sql_eq_param("family", family, params=params),
            sql_eq_param("declaration_name", declaration_name, params=params),
            sql_eq_param("completion_state", "unresolved", params=params),
            exp.LTE(
                this=column("first_collect_id"),
                expression=params.add(through_collect_id),
            ),
        ),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    record = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if record is None:
        raise RuntimeStoreError("DuckDB did not return a destination batch blocker count.")
    return optional_str(record[0])


def pending_page_with_cursor(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    payload: pa.RecordBatch,
) -> PendingWorkPage:
    if payload.num_rows == 0:
        return pending_page_from_batch(payload)
    last_collect_id = arrow_helpers.last_string_value(payload, "collect_id")
    last_sequence_order = arrow_helpers.last_int_value(payload, "sequence_order")
    if last_collect_id is None or last_sequence_order is None:
        raise RuntimeStoreError("Pending work cursor metadata is missing.")
    token = str(uuid.uuid4())
    params = context.new_params()
    row = (
        ("token", token),
        ("sync_name", scope.sync_name),
        ("destination_name", scope.destination_name),
        ("surface", scope.surface),
        ("family", scope.family),
        ("declaration_name", scope.declaration_name),
        ("collect_id", last_collect_id),
        ("sequence_order", last_sequence_order),
    )
    compiled = render_sql(
        row_insert(
            context.runtime_relation("pending_work_cursors"),
            row_write_input(row, params=params),
        ),
        dialect=context.dialect,
        params=params,
    )
    context.connection.execute(compiled.sql, compiled.params)
    return PendingWorkPage(
        payload=payload,
        row_count=payload.num_rows,
        first_collect_id=arrow_helpers.first_string_value(payload, "collect_id"),
        last_collect_id=last_collect_id,
        first_sequence_order=arrow_helpers.first_int_value(payload, "sequence_order"),
        last_sequence_order=last_sequence_order,
        complete_through_collect_id=None,
        next_cursor=PendingWorkCursor(
            token=token,
            collect_id=last_collect_id,
            sequence_order=last_sequence_order,
        ),
    )


def pending_page_from_batch(payload: pa.RecordBatch) -> PendingWorkPage:
    complete_through = arrow_helpers.last_string_value(payload, "collect_id")
    return PendingWorkPage(
        payload=payload,
        row_count=payload.num_rows,
        first_collect_id=arrow_helpers.first_string_value(payload, "collect_id"),
        last_collect_id=arrow_helpers.last_string_value(payload, "collect_id"),
        first_sequence_order=arrow_helpers.first_int_value(payload, "sequence_order"),
        last_sequence_order=arrow_helpers.last_int_value(payload, "sequence_order"),
        complete_through_collect_id=complete_through,
    )


def compile_pending_work_read(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    lower_bound: StateOrderedWorkScanPosition | EventKeysetScanPosition | None,
    source_collect_id: str | None,
    limit: int,
) -> CompiledSql:
    params = context.new_params()
    conditions: list[SqlCondition] = [
        column("family").eq(params.add(scope.family)),
        column("declaration_name").eq(params.add(scope.declaration_name)),
    ]
    lower_condition = ordered_work_after_position_condition(lower_bound, params, context.dialect)
    if lower_condition is not None:
        conditions.append(lower_condition)
    if source_collect_id is not None:
        conditions.append(column("collect_id").eq(params.add(source_collect_id)))
    query = (
        select(*_PENDING_WORK_COLUMNS)
        .from_(table(context.runtime_relation("ordered_work")))
        .where(and_conditions(conditions))
        .order_by(column("collect_id"), column("sequence_order"), column("work_id"))
        .limit(params.add(limit))
    )
    return render_sql(query, dialect=context.dialect, params=params)


def compile_pending_work_after_cursor_read(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    cursor: PendingWorkCursor,
    limit: int,
) -> CompiledSql:
    params = context.new_params()
    query = (
        select(*_PENDING_WORK_COLUMNS)
        .from_(table(context.runtime_relation("ordered_work")))
        .where(
            and_conditions(
                [
                    column("family").eq(params.add(scope.family)),
                    column("declaration_name").eq(params.add(scope.declaration_name)),
                    column("collect_id").eq(params.add(cursor.collect_id)),
                    column("sequence_order") > params.add(cursor.sequence_order),
                ]
            )
        )
        .order_by(column("sequence_order"), column("work_id"))
        .limit(params.add(limit))
    )
    return render_sql(query, dialect=context.dialect, params=params)


def compile_first_pending_collect_id_read(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    lower_bound: StateOrderedWorkScanPosition | EventKeysetScanPosition | None,
) -> CompiledSql:
    params = context.new_params()
    conditions: list[SqlCondition] = [
        column("family").eq(params.add(scope.family)),
        column("declaration_name").eq(params.add(scope.declaration_name)),
    ]
    lower_condition = ordered_work_after_position_condition(lower_bound, params, context.dialect)
    if lower_condition is not None:
        conditions.append(lower_condition)
    query = min_read(
        context.runtime_relation("ordered_work"),
        "collect_id",
        where=and_conditions(conditions),
    )
    return render_sql(query, dialect=context.dialect, params=params)


def ordered_work_after_position_condition(
    position: StateOrderedWorkScanPosition | EventKeysetScanPosition | None,
    params: SqlParamAllocator,
    dialect: SqlDialectCapabilities,
) -> SqlCondition | None:
    _ = dialect
    if position is None:
        return None
    if not isinstance(position, StateOrderedWorkScanPosition):
        raise DeclarationValidationError(
            "ordered_work lower bounds must be State ordered-work positions; "
            "Event keyset progress is replayed from Source SQL."
        )
    return exp.or_(
        column("collect_id") > params.add(position.collect_id),
        exp.and_(
            column("collect_id").eq(params.add(position.collect_id)),
            column("sequence_order") > params.add(position.sequence_order),
        ),
    )


def json_extract_string(
    dialect: SqlDialectCapabilities,
    column_name: str,
    path: str,
) -> SqlRenderable:
    if dialect.name == "bigquery":
        return exp.func("JSON_VALUE", column(column_name), exp.Literal.string(path))
    if dialect.name == "databricks":
        return exp.func("GET_JSON_OBJECT", column(column_name), exp.Literal.string(path))
    if dialect.name == "snowflake":
        return exp.cast(exp.func("GET_PATH", column(column_name), exp.Literal.string(path)), "TEXT")
    return exp.func("JSON_EXTRACT_STRING", column(column_name), exp.Literal.string(path))


def and_conditions(conditions: list[SqlCondition]) -> SqlCondition:
    if not conditions:
        raise ValueError("SQL condition list must not be empty.")
    condition: SqlCondition = conditions[0]
    for next_condition in conditions[1:]:
        condition = exp.and_(condition, next_condition)
    return condition


def pending_work_lower_bound(
    position: ScanPosition | None,
) -> StateOrderedWorkScanPosition | EventKeysetScanPosition | None:
    if position is None:
        return None
    if isinstance(position, StateOrderedWorkScanPosition):
        return position
    if isinstance(position, StateCurrentSnapshotScanPosition):
        return None
    if position.family == "event":
        raise DeclarationValidationError(
            "Event keyset progress is not an ordered_work lower bound; "
            "use Event source keyset range staging."
        )
    raise DeclarationValidationError("Unsupported pending-work scan position.")


def retention_safe_collect_id(
    *,
    watermark: str | None,
    requested: str | None,
) -> str | None:
    if watermark is None:
        return None
    if requested is None:
        return watermark
    return min(watermark, requested)


def optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(cast(Any, value))


def optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(cast(Any, value))


__all__ = [
    "cleanup_ordered_work",
    "compile_first_pending_collect_id_read",
    "compile_pending_work_after_cursor_read",
    "compile_pending_work_read",
    "first_pending_collect_id",
    "oldest_unresolved_destination_batch_collect_id",
    "ordered_work_count_after",
    "ordered_work_count_through",
    "ordered_work_max_sequence_order",
    "pending_page_from_batch",
    "pending_page_with_cursor",
    "pending_work_lower_bound",
    "previous_collect_id",
    "read_pending_work",
    "read_pending_work_after_cursor",
    "retention_collect_id_from_position",
    "retention_safe_collect_id",
    "retention_watermark",
    "validate_stored_pending_work_cursor",
]
