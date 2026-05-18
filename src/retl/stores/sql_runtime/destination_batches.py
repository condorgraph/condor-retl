from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

from sqlglot import exp, select

from retl.destinations.receipts import sanitize_partner_error_detail
from retl.errors import DeclarationValidationError
from retl.sql import (
    CompiledSql,
    SqlCondition,
    SqlParamAllocator,
    SqlRenderable,
    alias_column,
    column,
    render_sql,
    sql_alias,
    table,
    upsert_assignment,
)
from retl.stores.contracts import (
    CanonicalKey,
    CanonicalKeyScalar,
    DestinationBatchCompletionState,
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationBatchStatus,
    DestinationProgressScope,
    DestinationScanRange,
    EventKeysetScanPosition,
    PendingWorkPage,
    StateCurrentSnapshotScanPosition,
    StateOrderedWorkScanPosition,
    WorkFamily,
    destination_batch_id,
)
from retl.stores.sql_runtime import arrow as arrow_helpers
from retl.stores.sql_runtime import ordered_work as ordered_work_store
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.errors import RuntimeStoreError
from retl.stores.sql_runtime.writes import (
    compile_runtime_insert,
    compile_runtime_insert_many,
    compile_runtime_update,
    compile_runtime_update_many,
    compile_runtime_upsert,
    execute_runtime_insert_many,
    execute_runtime_update_many,
)

_DESTINATION_BATCH_COLUMNS = (
    "batch_id",
    "run_id",
    "attempt_id",
    "sync_name",
    "destination_name",
    "surface",
    "family",
    "declaration_name",
    "declaration_version_id",
    "source_page_index",
    "reconcile_page_index",
    "first_collect_id",
    "last_collect_id",
    "first_sequence_order",
    "last_sequence_order",
    "has_source_range",
    "state_lower_collect_id",
    "state_lower_sequence_order",
    "state_first_identity_json",
    "state_last_identity_json",
    "state_upper_identity_json",
    "state_lower_identity_json",
    "event_lower_cursor_value",
    "event_lower_primary_key_value",
    "event_first_cursor_value",
    "event_first_primary_key_value",
    "event_last_cursor_value",
    "event_last_primary_key_value",
    "event_upper_cursor_value",
    "event_upper_primary_key_value",
    "event_cursor_kind",
    "event_primary_key_kind",
    "destination_batch_index",
    "record_count",
    "payload_fingerprint",
    "target_request_fingerprint",
    "status",
    "completion_state",
    "attempt_count",
    "last_error_summary",
    "last_error_detail",
    "last_failure_category",
    "http_status",
    "retry_eligible",
    "first_submitted_at",
    "last_attempted_at",
    "completed_at",
)

_DESTINATION_BATCH_UPDATE_COLUMNS = (
    "declaration_version_id",
    "has_source_range",
    "state_lower_collect_id",
    "state_lower_sequence_order",
    "state_first_identity_json",
    "state_last_identity_json",
    "state_upper_identity_json",
    "state_lower_identity_json",
    "event_lower_cursor_value",
    "event_lower_primary_key_value",
    "event_first_cursor_value",
    "event_first_primary_key_value",
    "event_last_cursor_value",
    "event_last_primary_key_value",
    "event_upper_cursor_value",
    "event_upper_primary_key_value",
    "event_cursor_kind",
    "event_primary_key_kind",
    "status",
    "completion_state",
    "attempt_count",
    "run_id",
    "attempt_id",
    "record_count",
    "last_error_summary",
    "last_error_detail",
    "last_failure_category",
    "http_status",
    "retry_eligible",
    "first_submitted_at",
    "last_attempted_at",
    "completed_at",
)

_DESTINATION_BATCH_ORDER_COLUMNS = (
    "sync_name",
    "destination_name",
    "surface",
    "family",
    "declaration_name",
    "first_collect_id",
    "first_sequence_order",
    "destination_batch_index",
    "batch_id",
)

_DESTINATION_BATCH_WORK_COLUMNS: tuple[exp.Expression | str, ...] = (
    "work_id",
    "collect_id",
    "sequence_order",
    "family",
    "kind",
    sql_alias(column("kind"), "operation"),
    "declaration_name",
    "declaration_version_id",
    "key_json",
    "target_json",
    "identifiers_json",
    "payload_json",
)


def upsert_destination_batch(
    context: SqlRuntimeContext,
    destination_batches: list[DestinationBatchRecord],
    record: DestinationBatchRecord,
) -> DestinationBatchRecord:
    return upsert_destination_batches(context, destination_batches, (record,))[0]


def upsert_destination_batches(
    context: SqlRuntimeContext,
    destination_batches: list[DestinationBatchRecord],
    records: tuple[DestinationBatchRecord, ...],
    *,
    read_back: bool = True,
    existing_batches: tuple[DestinationBatchRecord, ...] | None = None,
) -> tuple[DestinationBatchRecord, ...]:
    if not records:
        return ()
    for record in records:
        _validate_destination_batch(record)
    if existing_batches is None:
        existing_by_id = destination_batches_by_id(
            context, tuple(record.batch_id for record in records)
        )
    else:
        for batch in existing_batches:
            _validate_destination_batch(batch)
        existing_by_id = {batch.batch_id: batch for batch in existing_batches}
    records_to_store = tuple(
        _sanitize_destination_batch_record(_planned_destination_batch_update(existing, record))
        if (
            (existing := existing_by_id.get(record.batch_id)) is not None
            and record.status == "pending"
            and record.completion_state == "unresolved"
            and record.attempt_count == 0
        )
        else _sanitize_destination_batch_record(record)
        for record in records
    )
    destination_batches.extend(records_to_store)
    inserts = tuple(record for record in records_to_store if record.batch_id not in existing_by_id)
    updates = tuple(record for record in records_to_store if record.batch_id in existing_by_id)
    if inserts:
        execute_runtime_insert_many(
            context,
            "destination_batches",
            tuple(_destination_batch_write_values(record) for record in inserts),
        )
    if updates:
        execute_destination_batches_update(context, updates)
    if not read_back:
        return records_to_store
    stored_by_id = destination_batches_by_id(
        context,
        tuple(record.batch_id for record in records_to_store),
    )
    if len(stored_by_id) != len({record.batch_id for record in records_to_store}):
        raise RuntimeStoreError("DuckDB did not persist the destination batch.")
    return tuple(stored_by_id[record.batch_id] for record in records_to_store)


def get_destination_batch(
    context: SqlRuntimeContext,
    *,
    batch_id: str,
) -> DestinationBatchRecord | None:
    validation_helpers.validate_identity(batch_id, "batch_id")
    return destination_batches_by_id(context, (batch_id,)).get(batch_id)


def get_destination_batches(
    context: SqlRuntimeContext,
    *,
    batch_ids: tuple[str, ...],
) -> tuple[DestinationBatchRecord, ...]:
    batches_by_id = destination_batches_by_id(context, batch_ids)
    return tuple(batch for batch_id in batch_ids if (batch := batches_by_id.get(batch_id)))


def destination_batches_by_id(
    context: SqlRuntimeContext,
    batch_ids: tuple[str, ...],
) -> dict[str, DestinationBatchRecord]:
    if not batch_ids:
        return {}
    for batch_id in batch_ids:
        validation_helpers.validate_identity(batch_id, "batch_id")
    compiled = compile_destination_batches_by_id_read(context, batch_ids=batch_ids)
    records = context.connection.execute(compiled.sql, compiled.params).fetchall()
    return {
        record.batch_id: record
        for record in (_destination_batch_from_record(row) for row in records)
    }


def list_destination_batches(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope | None = None,
    statuses: tuple[DestinationBatchStatus, ...] = (),
) -> tuple[DestinationBatchRecord, ...]:
    if scope is not None:
        validation_helpers.validate_progress_scope(scope)
    if statuses:
        for status in statuses:
            _validate_destination_batch_status(status)
    compiled = compile_destination_batches_list_read(context, scope=scope, statuses=statuses)
    records = context.connection.execute(compiled.sql, compiled.params).fetchall()
    return tuple(_destination_batch_from_record(record) for record in records)


def list_destination_batch_retry_candidates(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    retry_limit: int,
) -> tuple[DestinationBatchRecord, ...]:
    validation_helpers.validate_progress_scope(scope)
    validation_helpers.validate_nonnegative_int(retry_limit, "retry_limit")
    compiled = compile_destination_batch_retry_candidates_read(
        context,
        scope=scope,
        retry_limit=retry_limit,
    )
    records = context.connection.execute(compiled.sql, compiled.params).fetchall()
    return tuple(_destination_batch_from_record(record) for record in records)


def read_destination_batch_work(
    context: SqlRuntimeContext,
    *,
    batch: DestinationBatchRecord,
) -> PendingWorkPage:
    _validate_destination_batch(batch)
    if batch.identity.scope.family == "event":
        raise DeclarationValidationError(
            "Event destination batch work is replayed from the stored source keyset range, "
            "not from ordered_work."
        )
    compiled = compile_destination_batch_work_read(context, batch=batch)
    fetched = arrow_helpers.fetch_all_record_batches(
        context.connection.execute(compiled.sql, compiled.params),
    )
    return ordered_work_store.pending_page_from_batch(fetched)


def dismiss_unresolved_destination_batches(
    context: SqlRuntimeContext,
    destination_batches: list[DestinationBatchRecord],
    *,
    scope: DestinationProgressScope,
) -> tuple[DestinationBatchRecord, ...]:
    validation_helpers.validate_progress_scope(scope)
    actionable = tuple(
        batch
        for batch in list_destination_batches(
            context,
            scope=scope,
            statuses=("pending", "failed"),
        )
        if batch.completion_state == "unresolved"
    )
    completed_at = datetime.now(UTC)
    dismissed = tuple(
        replace(
            batch,
            status="skipped",
            completion_state="resolved",
            retry_eligible=False,
            completed_at=completed_at,
        )
        for batch in actionable
    )
    return upsert_destination_batches(context, destination_batches, dismissed)


def compile_destination_batches_by_id_read(
    context: SqlRuntimeContext,
    *,
    batch_ids: tuple[str, ...],
) -> CompiledSql:
    if not batch_ids:
        raise ValueError("destination batch id reads require at least one batch id.")
    params = context.new_params()
    query = (
        select(*_DESTINATION_BATCH_COLUMNS)
        .from_(table(context.runtime_relation("destination_batches")))
        .where(
            exp.In(
                this=column("batch_id"),
                expressions=[params.add(batch_id) for batch_id in batch_ids],
            )
        )
    )
    return render_sql(query, dialect=context.dialect, params=params)


def compile_destination_batches_list_read(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope | None = None,
    statuses: tuple[DestinationBatchStatus, ...] = (),
) -> CompiledSql:
    params = context.new_params()
    conditions: list[SqlCondition] = []
    if scope is not None:
        conditions.extend(_progress_scope_conditions(scope, params))
    if statuses:
        conditions.append(
            exp.In(
                this=column("status"),
                expressions=[params.add(status) for status in statuses],
            )
        )
    query = select(*_DESTINATION_BATCH_COLUMNS).from_(
        table(context.runtime_relation("destination_batches"))
    )
    if conditions:
        query = query.where(_and_conditions(conditions))
    query = query.order_by(*[column(name) for name in _DESTINATION_BATCH_ORDER_COLUMNS])
    return render_sql(query, dialect=context.dialect, params=params)


def compile_destination_batch_retry_candidates_read(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
    retry_limit: int,
) -> CompiledSql:
    params = context.new_params()
    query = (
        select(*_DESTINATION_BATCH_COLUMNS)
        .from_(table(context.runtime_relation("destination_batches")))
        .where(
            _and_conditions(
                [
                    *_progress_scope_conditions(scope, params),
                    column("completion_state").eq(params.add("unresolved")),
                    exp.or_(
                        column("status").eq(params.add("pending")),
                        exp.and_(
                            column("status").eq(params.add("failed")),
                            column("retry_eligible").eq(params.add(True)),
                            column("attempt_count") < params.add(retry_limit),
                        ),
                    ),
                ]
            )
        )
        .order_by(*[column(name) for name in _DESTINATION_BATCH_ORDER_COLUMNS])
    )
    return render_sql(query, dialect=context.dialect, params=params)


def compile_destination_batch_work_read(
    context: SqlRuntimeContext,
    *,
    batch: DestinationBatchRecord,
) -> CompiledSql:
    identity = batch.identity
    params = context.new_params()
    query = (
        select(*_DESTINATION_BATCH_WORK_COLUMNS)
        .from_(table(context.runtime_relation("ordered_work")))
        .where(
            _and_conditions(
                [
                    column("family").eq(params.add(identity.scope.family)),
                    column("declaration_name").eq(params.add(identity.scope.declaration_name)),
                    exp.or_(
                        column("collect_id") > params.add(identity.first_collect_id),
                        exp.and_(
                            column("collect_id").eq(params.add(identity.first_collect_id)),
                            column("sequence_order") >= params.add(identity.first_sequence_order),
                        ),
                    ),
                    exp.or_(
                        column("collect_id") < params.add(identity.last_collect_id),
                        exp.and_(
                            column("collect_id").eq(params.add(identity.last_collect_id)),
                            column("sequence_order") <= params.add(identity.last_sequence_order),
                        ),
                    ),
                ]
            )
        )
        .order_by(column("collect_id"), column("sequence_order"), column("work_id"))
    )
    return render_sql(query, dialect=context.dialect, params=params)


def compile_destination_batch_upsert(
    context: SqlRuntimeContext,
    record: DestinationBatchRecord,
) -> CompiledSql:
    return compile_runtime_upsert(
        context,
        "destination_batches",
        tuple(zip(_DESTINATION_BATCH_COLUMNS, _destination_batch_values(record), strict=True)),
        key_columns=("batch_id",),
        update_columns=(
            "declaration_version_id",
            "has_source_range",
            "state_lower_collect_id",
            "state_lower_sequence_order",
            "state_first_identity_json",
            "state_last_identity_json",
            "state_upper_identity_json",
            "state_lower_identity_json",
            "event_lower_cursor_value",
            "event_lower_primary_key_value",
            "event_first_cursor_value",
            "event_first_primary_key_value",
            "event_last_cursor_value",
            "event_last_primary_key_value",
            "event_upper_cursor_value",
            "event_upper_primary_key_value",
            "event_cursor_kind",
            "event_primary_key_kind",
            "status",
            "completion_state",
            "attempt_count",
            "run_id",
            "attempt_id",
            "record_count",
            "last_error_summary",
            "last_error_detail",
            "last_failure_category",
            "http_status",
            "retry_eligible",
            "last_attempted_at",
            "completed_at",
        ),
        update_assignments=(
            upsert_assignment(
                "first_submitted_at",
                exp.Coalesce(
                    this=_destination_batch_target_column(context, "first_submitted_at"),
                    expressions=[_destination_batch_source_column(context, "first_submitted_at")],
                ),
            ),
            upsert_assignment("updated_at", exp.Anonymous(this="NOW")),
        ),
    )


def compile_destination_batch_insert(
    context: SqlRuntimeContext,
    record: DestinationBatchRecord,
) -> CompiledSql:
    return compile_runtime_insert(
        context, "destination_batches", _destination_batch_write_values(record)
    )


def compile_destination_batches_insert(
    context: SqlRuntimeContext,
    records: tuple[DestinationBatchRecord, ...],
) -> CompiledSql:
    return compile_runtime_insert_many(
        context,
        "destination_batches",
        tuple(_destination_batch_write_values(record) for record in records),
    )


def compile_destination_batch_update(
    context: SqlRuntimeContext,
    record: DestinationBatchRecord,
) -> CompiledSql:
    return compile_runtime_update(
        context,
        "destination_batches",
        (
            ("declaration_version_id", record.identity.declaration_version_id),
            ("has_source_range", record.identity.source_range is not None),
            (
                "state_lower_collect_id",
                _state_ordered_lower_collect_id(record.identity.source_range),
            ),
            (
                "state_lower_sequence_order",
                _state_ordered_lower_sequence_order(record.identity.source_range),
            ),
            (
                "state_first_identity_json",
                _state_current_identity_value(record.identity.source_range, "first"),
            ),
            (
                "state_last_identity_json",
                _state_current_identity_value(record.identity.source_range, "last"),
            ),
            (
                "state_upper_identity_json",
                _state_current_identity_value(record.identity.source_range, "upper"),
            ),
            (
                "state_lower_identity_json",
                _state_current_identity_value(record.identity.source_range, "lower"),
            ),
            (
                "event_lower_cursor_value",
                _event_range_value(record.identity.source_range, "lower", "cursor"),
            ),
            (
                "event_lower_primary_key_value",
                _event_range_value(record.identity.source_range, "lower", "primary_key"),
            ),
            (
                "event_first_cursor_value",
                _event_range_value(record.identity.source_range, "first", "cursor"),
            ),
            (
                "event_first_primary_key_value",
                _event_range_value(record.identity.source_range, "first", "primary_key"),
            ),
            (
                "event_last_cursor_value",
                _event_range_value(record.identity.source_range, "last", "cursor"),
            ),
            (
                "event_last_primary_key_value",
                _event_range_value(record.identity.source_range, "last", "primary_key"),
            ),
            (
                "event_upper_cursor_value",
                _event_range_value(record.identity.source_range, "upper", "cursor"),
            ),
            (
                "event_upper_primary_key_value",
                _event_range_value(record.identity.source_range, "upper", "primary_key"),
            ),
            (
                "event_cursor_kind",
                _event_range_kind(record.identity.source_range, "cursor"),
            ),
            (
                "event_primary_key_kind",
                _event_range_kind(record.identity.source_range, "primary_key"),
            ),
            ("status", record.status),
            ("completion_state", record.completion_state),
            ("attempt_count", record.attempt_count),
            ("run_id", record.run_id),
            ("attempt_id", record.attempt_id),
            ("record_count", record.record_count),
            ("last_error_summary", sanitize_partner_error_detail(record.last_error_summary)),
            ("last_error_detail", sanitize_partner_error_detail(record.last_error_detail)),
            ("last_failure_category", record.last_failure_category),
            ("http_status", record.http_status),
            ("retry_eligible", record.retry_eligible),
            ("first_submitted_at", record.first_submitted_at),
            ("last_attempted_at", record.last_attempted_at),
            ("completed_at", record.completed_at),
        ),
        where_values=(("batch_id", record.batch_id),),
        update_assignments=(upsert_assignment("updated_at", exp.CurrentTimestamp()),),
    )


def compile_destination_batches_update(
    context: SqlRuntimeContext,
    records: tuple[DestinationBatchRecord, ...],
) -> CompiledSql:
    _validate_unique_destination_batch_update_records(records)
    return compile_runtime_update_many(
        context,
        "destination_batches",
        tuple(_destination_batch_update_values(record) for record in records),
        key_columns=("batch_id",),
        update_columns=_DESTINATION_BATCH_UPDATE_COLUMNS,
        update_assignments=(upsert_assignment("updated_at", exp.CurrentTimestamp()),),
    )


def execute_destination_batches_update(
    context: SqlRuntimeContext,
    records: tuple[DestinationBatchRecord, ...],
) -> None:
    _validate_unique_destination_batch_update_records(records)
    execute_runtime_update_many(
        context,
        "destination_batches",
        tuple(_destination_batch_update_values(record) for record in records),
        key_columns=("batch_id",),
        update_columns=_DESTINATION_BATCH_UPDATE_COLUMNS,
        update_assignments=(upsert_assignment("updated_at", exp.CurrentTimestamp()),),
    )


def _progress_scope_conditions(
    scope: DestinationProgressScope,
    params: SqlParamAllocator,
) -> list[SqlCondition]:
    return [
        column("sync_name").eq(params.add(scope.sync_name)),
        column("destination_name").eq(params.add(scope.destination_name)),
        column("surface").eq(params.add(scope.surface)),
        column("family").eq(params.add(scope.family)),
        column("declaration_name").eq(params.add(scope.declaration_name)),
    ]


def _and_conditions(conditions: list[SqlCondition]) -> SqlCondition:
    if not conditions:
        raise ValueError("SQL condition list must not be empty.")
    condition: SqlCondition = conditions[0]
    for next_condition in conditions[1:]:
        condition = exp.and_(condition, next_condition)
    return condition


def _destination_batch_values(record: DestinationBatchRecord) -> list[object]:
    identity = record.identity
    return [
        record.batch_id,
        record.run_id,
        record.attempt_id,
        identity.scope.sync_name,
        identity.scope.destination_name,
        identity.scope.surface,
        identity.scope.family,
        identity.scope.declaration_name,
        identity.declaration_version_id,
        identity.source_page_index,
        identity.reconcile_page_index,
        identity.first_collect_id,
        identity.last_collect_id,
        identity.first_sequence_order,
        identity.last_sequence_order,
        identity.source_range is not None,
        _state_ordered_lower_collect_id(identity.source_range),
        _state_ordered_lower_sequence_order(identity.source_range),
        _state_current_identity_value(identity.source_range, "first"),
        _state_current_identity_value(identity.source_range, "last"),
        _state_current_identity_value(identity.source_range, "upper"),
        _state_current_identity_value(identity.source_range, "lower"),
        _event_range_value(identity.source_range, "lower", "cursor"),
        _event_range_value(identity.source_range, "lower", "primary_key"),
        _event_range_value(identity.source_range, "first", "cursor"),
        _event_range_value(identity.source_range, "first", "primary_key"),
        _event_range_value(identity.source_range, "last", "cursor"),
        _event_range_value(identity.source_range, "last", "primary_key"),
        _event_range_value(identity.source_range, "upper", "cursor"),
        _event_range_value(identity.source_range, "upper", "primary_key"),
        _event_range_kind(identity.source_range, "cursor"),
        _event_range_kind(identity.source_range, "primary_key"),
        identity.destination_batch_index,
        record.record_count,
        identity.payload_fingerprint,
        identity.target_request_fingerprint,
        record.status,
        record.completion_state,
        record.attempt_count,
        sanitize_partner_error_detail(record.last_error_summary),
        sanitize_partner_error_detail(record.last_error_detail),
        record.last_failure_category,
        record.http_status,
        record.retry_eligible,
        record.first_submitted_at,
        record.last_attempted_at,
        record.completed_at,
    ]


def _destination_batch_write_values(
    record: DestinationBatchRecord,
) -> tuple[tuple[str, object], ...]:
    return tuple(zip(_DESTINATION_BATCH_COLUMNS, _destination_batch_values(record), strict=True))


def _destination_batch_update_values(
    record: DestinationBatchRecord,
) -> tuple[tuple[str, object], ...]:
    values = dict(_destination_batch_write_values(record))
    return tuple(
        (column_name, values[column_name])
        for column_name in (*_DESTINATION_BATCH_UPDATE_COLUMNS, "batch_id")
    )


def _destination_batch_target_column(
    context: SqlRuntimeContext,
    column_name: str,
) -> SqlRenderable:
    if context.dialect.name == "duckdb":
        return column(column_name, relation=context.runtime_relation("destination_batches"))
    return alias_column("target", column_name)


def _destination_batch_source_column(
    context: SqlRuntimeContext,
    column_name: str,
) -> SqlRenderable:
    if context.dialect.name == "duckdb":
        return alias_column("excluded", column_name)
    return alias_column("source", column_name)


def _sanitize_destination_batch_record(record: DestinationBatchRecord) -> DestinationBatchRecord:
    return replace(
        record,
        last_error_summary=sanitize_partner_error_detail(record.last_error_summary),
        last_error_detail=sanitize_partner_error_detail(record.last_error_detail),
    )


def _planned_destination_batch_update(
    existing: DestinationBatchRecord,
    planned: DestinationBatchRecord,
) -> DestinationBatchRecord:
    return replace(existing, identity=planned.identity, record_count=planned.record_count)


def _validate_unique_destination_batch_update_records(
    records: tuple[DestinationBatchRecord, ...],
) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for record in records:
        if record.batch_id in seen:
            duplicates.add(record.batch_id)
        seen.add(record.batch_id)
    if duplicates:
        raise RuntimeStoreError("Destination batch updates require one source row per batch id.")


def _destination_batch_from_record(record: object) -> DestinationBatchRecord:
    values = cast(tuple[Any, ...], record)
    by_column = dict(zip(_DESTINATION_BATCH_COLUMNS, values, strict=True))
    scope = DestinationProgressScope(
        sync_name=str(by_column["sync_name"]),
        destination_name=str(by_column["destination_name"]),
        surface=str(by_column["surface"]),
        family=cast(WorkFamily, str(by_column["family"])),
        declaration_name=str(by_column["declaration_name"]),
    )
    identity = DestinationBatchIdentity(
        scope=scope,
        declaration_version_id=str(by_column["declaration_version_id"]),
        source_range=_destination_scan_range_from_values(scope, by_column),
        source_page_index=_optional_int(by_column["source_page_index"]),
        reconcile_page_index=_optional_int(by_column["reconcile_page_index"]),
        first_collect_id=str(by_column["first_collect_id"]),
        last_collect_id=str(by_column["last_collect_id"]),
        first_sequence_order=int(by_column["first_sequence_order"]),
        last_sequence_order=int(by_column["last_sequence_order"]),
        destination_batch_index=int(by_column["destination_batch_index"]),
        payload_fingerprint=str(by_column["payload_fingerprint"]),
        target_request_fingerprint=str(by_column["target_request_fingerprint"]),
    )
    return DestinationBatchRecord(
        batch_id=str(by_column["batch_id"]),
        run_id=cast(str | None, by_column["run_id"]),
        attempt_id=cast(str | None, by_column["attempt_id"]),
        identity=identity,
        record_count=int(by_column["record_count"]),
        status=cast(DestinationBatchStatus, str(by_column["status"])),
        completion_state=cast(DestinationBatchCompletionState, str(by_column["completion_state"])),
        attempt_count=int(by_column["attempt_count"]),
        last_error_summary=cast(str | None, by_column["last_error_summary"]),
        last_error_detail=cast(str | None, by_column["last_error_detail"]),
        last_failure_category=cast(str | None, by_column["last_failure_category"]),
        http_status=_optional_int(by_column["http_status"]),
        retry_eligible=cast(bool | None, by_column["retry_eligible"]),
        first_submitted_at=cast(datetime | None, by_column["first_submitted_at"]),
        last_attempted_at=cast(datetime | None, by_column["last_attempted_at"]),
        completed_at=cast(datetime | None, by_column["completed_at"]),
    )


def _destination_scan_range_from_values(
    scope: DestinationProgressScope,
    values: dict[str, object],
) -> DestinationScanRange | None:
    if not bool(values["has_source_range"]):
        return None
    if scope.family == "state":
        first_current = _state_current_position_from_values(values, "first")
        last_current = _state_current_position_from_values(values, "last")
        upper_current = _state_current_position_from_values(values, "upper")
        if first_current is not None and last_current is not None and upper_current is not None:
            return DestinationScanRange(
                lower_bound_exclusive=_state_current_position_from_values(values, "lower"),
                first_record_position=first_current,
                last_record_position=last_current,
                upper_bound_inclusive=upper_current,
            )
        first = StateOrderedWorkScanPosition(
            collect_id=str(values["first_collect_id"]),
            sequence_order=int(cast(Any, values["first_sequence_order"])),
        )
        last = StateOrderedWorkScanPosition(
            collect_id=str(values["last_collect_id"]),
            sequence_order=int(cast(Any, values["last_sequence_order"])),
        )
        lower = _state_ordered_lower_from_values(values)
        return DestinationScanRange(
            lower_bound_exclusive=lower,
            first_record_position=first,
            last_record_position=last,
            upper_bound_inclusive=last,
        )
    if scope.family != "event":
        return None
    event_first: EventKeysetScanPosition | None = _event_position_from_values(values, "first")
    event_last: EventKeysetScanPosition | None = _event_position_from_values(values, "last")
    event_upper: EventKeysetScanPosition | None = _event_position_from_values(values, "upper")
    if event_first is None or event_last is None or event_upper is None:
        return None
    return DestinationScanRange(
        lower_bound_exclusive=_event_position_from_values(values, "lower"),
        first_record_position=event_first,
        last_record_position=event_last,
        upper_bound_inclusive=event_upper,
    )


def _event_position_from_values(
    values: dict[str, object],
    point: str,
) -> EventKeysetScanPosition | None:
    cursor_value = values[f"event_{point}_cursor_value"]
    primary_key_value = values[f"event_{point}_primary_key_value"]
    cursor_kind = values["event_cursor_kind"]
    primary_key_kind = values["event_primary_key_kind"]
    if (
        cursor_value is None
        or primary_key_value is None
        or cursor_kind is None
        or primary_key_kind is None
    ):
        return None
    return EventKeysetScanPosition(
        cursor_value=_event_scalar_from_storage(str(cursor_value), str(cursor_kind)),
        primary_key_value=_event_scalar_from_storage(str(primary_key_value), str(primary_key_kind)),
    )


def _state_ordered_lower_from_values(
    values: dict[str, object],
) -> StateOrderedWorkScanPosition | None:
    collect_id = values["state_lower_collect_id"]
    sequence_order = values["state_lower_sequence_order"]
    if collect_id is None or sequence_order is None:
        return None
    return StateOrderedWorkScanPosition(
        collect_id=str(collect_id),
        sequence_order=int(cast(Any, sequence_order)),
    )


def _state_current_position_from_values(
    values: dict[str, object],
    point: str,
) -> StateCurrentSnapshotScanPosition | None:
    value = values[f"state_{point}_identity_json"]
    if value is None:
        return None
    return StateCurrentSnapshotScanPosition(
        key=CanonicalKey.of(CanonicalKeyScalar.string(str(value)))
    )


def _event_scalar_from_storage(value: str, kind: str) -> CanonicalKeyScalar:
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
    raise RuntimeStoreError("Destination batch Event checkpoint scalar type is not supported.")


def _event_range_value(
    source_range: DestinationScanRange | None,
    point: str,
    field_name: str,
) -> str | None:
    position = _event_range_position(source_range, point)
    if position is None:
        return None
    scalar = position.cursor_value if field_name == "cursor" else position.primary_key_value
    return None if scalar.value is None else str(scalar.value)


def _state_ordered_lower_collect_id(source_range: DestinationScanRange | None) -> str | None:
    position = None if source_range is None else source_range.lower_bound_exclusive
    if isinstance(position, StateOrderedWorkScanPosition):
        return position.collect_id
    return None


def _state_ordered_lower_sequence_order(source_range: DestinationScanRange | None) -> int | None:
    position = None if source_range is None else source_range.lower_bound_exclusive
    if isinstance(position, StateOrderedWorkScanPosition):
        return position.sequence_order
    return None


def _state_current_identity_value(
    source_range: DestinationScanRange | None,
    point: str,
) -> str | None:
    if source_range is None:
        return None
    if point == "first":
        position: object = source_range.first_record_position
    elif point == "last":
        position = source_range.last_record_position
    elif point == "upper":
        position = source_range.upper_bound_inclusive
    elif point == "lower":
        position = source_range.lower_bound_exclusive
    else:
        raise ValueError("State current range point is not supported.")
    if not isinstance(position, StateCurrentSnapshotScanPosition):
        return None
    return str(position.key.parts[0].value)


def _event_range_kind(
    source_range: DestinationScanRange | None,
    field_name: str,
) -> str | None:
    position = _event_range_position(source_range, "upper")
    if position is None:
        return None
    scalar = position.cursor_value if field_name == "cursor" else position.primary_key_value
    return scalar.kind


def _event_range_position(
    source_range: DestinationScanRange | None,
    point: str,
) -> EventKeysetScanPosition | None:
    if source_range is None:
        return None
    position: object
    if point == "first":
        position = source_range.first_record_position
    elif point == "last":
        position = source_range.last_record_position
    elif point == "upper":
        position = source_range.upper_bound_inclusive
    elif point == "lower":
        position = source_range.lower_bound_exclusive
    else:
        raise ValueError("Event range point is not supported.")
    return position if isinstance(position, EventKeysetScanPosition) else None


def _validate_destination_batch(record: DestinationBatchRecord) -> None:
    if not isinstance(record, DestinationBatchRecord):
        raise DeclarationValidationError("destination batch must be a DestinationBatchRecord.")
    validation_helpers.validate_identity(record.batch_id, "batch_id")
    _validate_destination_batch_identity(record.identity)
    if record.batch_id != destination_batch_id(record.identity):
        raise DeclarationValidationError(
            "`batch_id` must match the destination batch identity fingerprint."
        )
    _validate_destination_batch_status(record.status)
    _validate_destination_batch_completion_state(record.completion_state)
    if record.completion_state not in _allowed_destination_batch_completion_states(record.status):
        raise DeclarationValidationError(
            "`completion_state` is not valid for the destination batch status."
        )
    if record.status == "skipped" and record.retry_eligible is True:
        raise DeclarationValidationError("skipped destination batches cannot be retryable.")
    validation_helpers.validate_nonnegative_int(record.attempt_count, "attempt_count")
    validation_helpers.validate_nonnegative_int(record.record_count, "record_count")
    _validate_http_status(record.http_status)


def _destination_batch_retry_candidate(
    batch: DestinationBatchRecord,
    *,
    retry_limit: int,
) -> bool:
    if batch.completion_state != "unresolved":
        return False
    if batch.status == "pending":
        return True
    return (
        batch.status == "failed"
        and batch.retry_eligible is True
        and batch.attempt_count < retry_limit
    )


def _validate_destination_batch_identity(identity: DestinationBatchIdentity) -> None:
    if not isinstance(identity, DestinationBatchIdentity):
        raise DeclarationValidationError(
            "destination batch identity must be a DestinationBatchIdentity."
        )
    validation_helpers.validate_progress_scope(identity.scope)
    validation_helpers.validate_identity(identity.declaration_version_id, "declaration_version_id")
    if identity.source_page_index is None and identity.reconcile_page_index is None:
        raise DeclarationValidationError(
            "destination batch identity requires source or reconcile page coordinates."
        )
    for field_name in ("source_page_index", "reconcile_page_index"):
        value = getattr(identity, field_name)
        if value is not None:
            validation_helpers.validate_nonnegative_int(value, field_name)
    for field_name in ("first_collect_id", "last_collect_id"):
        validation_helpers.validate_allocated_collect_id(getattr(identity, field_name))
    if identity.last_collect_id < identity.first_collect_id:
        raise DeclarationValidationError("`last_collect_id` must be >= `first_collect_id`.")
    for field_name in ("first_sequence_order", "last_sequence_order"):
        validation_helpers.validate_nonnegative_int(getattr(identity, field_name), field_name)
    if (
        identity.last_collect_id == identity.first_collect_id
        and identity.last_sequence_order < identity.first_sequence_order
    ):
        raise DeclarationValidationError(
            "`last_sequence_order` must be >= `first_sequence_order` within one collect."
        )
    validation_helpers.validate_nonnegative_int(
        identity.destination_batch_index, "destination_batch_index"
    )
    _validate_redacted_fingerprint(identity.payload_fingerprint, "payload_fingerprint")
    _validate_redacted_fingerprint(
        identity.target_request_fingerprint,
        "target_request_fingerprint",
    )


def _validate_redacted_fingerprint(value: str, field_name: str) -> None:
    validation_helpers.validate_identity(value, field_name)
    if sanitize_partner_error_detail(value) != value:
        raise DeclarationValidationError(
            f"`{field_name}` must not contain raw secrets or auth-bearing values."
        )


def _validate_destination_batch_status(status: str) -> None:
    if status not in {"pending", "accepted", "succeeded", "failed", "skipped"}:
        raise DeclarationValidationError("destination batch status is not supported.")


def _validate_destination_batch_completion_state(completion_state: str) -> None:
    if completion_state not in {"unresolved", "resolved"}:
        raise DeclarationValidationError("destination batch completion state is not supported.")


def _destination_batch_completion_state(status: str) -> DestinationBatchCompletionState:
    if status in {"accepted", "succeeded", "skipped"}:
        return "resolved"
    return "unresolved"


def _destination_batch_attempt_completion_state(
    status: str,
) -> DestinationBatchCompletionState:
    return _destination_batch_completion_state(status)


def _allowed_destination_batch_completion_states(
    status: str,
) -> set[DestinationBatchCompletionState]:
    if status == "failed":
        return {"unresolved", "resolved"}
    return {_destination_batch_completion_state(status)}


def _validate_http_status(value: int | None) -> None:
    if value is not None and (
        not isinstance(value, int) or isinstance(value, bool) or value < 100 or value > 599
    ):
        raise DeclarationValidationError("`http_status` must be between 100 and 599.")


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        return int(value)
    raise RuntimeStoreError("DuckDB returned a non-integer value.")


__all__ = [
    "compile_destination_batch_insert",
    "compile_destination_batch_retry_candidates_read",
    "compile_destination_batch_update",
    "compile_destination_batch_upsert",
    "compile_destination_batch_work_read",
    "compile_destination_batches_insert",
    "compile_destination_batches_update",
    "compile_destination_batches_by_id_read",
    "compile_destination_batches_list_read",
    "destination_batches_by_id",
    "dismiss_unresolved_destination_batches",
    "execute_destination_batches_update",
    "get_destination_batch",
    "get_destination_batches",
    "list_destination_batch_retry_candidates",
    "list_destination_batches",
    "read_destination_batch_work",
    "upsert_destination_batch",
    "upsert_destination_batches",
]
