from __future__ import annotations

from typing import Any, cast

from sqlglot import exp, select

from retl.sql import (
    SqlCondition,
    SqlParamAllocator,
    column,
    render_sql,
    row_read,
    sql_and,
    sql_eq_param,
    sql_order,
    table,
)
from retl.stores.contracts import DestinationProgressScope
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.operations import RuntimeOperationSummary, count_rows, sql_context
from retl.stores.sql_runtime.schema import runtime_table_names


def inspect_runtime_store(context: SqlRuntimeContext) -> RuntimeOperationSummary:
    counts: dict[str, int] = {}
    queries = []
    for relation in sorted(runtime_table_names()):
        count, compiled = count_rows(context, relation)
        counts[relation] = count
        queries.append(compiled)
    return {
        "kind": "runtime_store",
        "tables": counts,
        "sql_context": sql_context(
            context,
            relations=sorted(runtime_table_names()),
            sql=queries,
        ),
    }


def inspect_declaration(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
) -> RuntimeOperationSummary:
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    params = context.new_params()
    declaration_where = sql_eq_param("declaration_name", declaration_name, params=params)
    declaration_count, declaration_query = count_rows(
        context,
        "declarations",
        where=declaration_where,
        params=params.params,
    )

    work_count, work_count_query = _count_for_declaration(
        context,
        "ordered_work",
        declaration_name=declaration_name,
    )
    current_count, current_count_query = _count_for_declaration(
        context,
        "state_current",
        declaration_name=declaration_name,
    )
    progress_count, progress_count_query = _count_for_declaration(
        context,
        "destination_progress",
        declaration_name=declaration_name,
    )
    batch_count, batch_count_query = _count_for_declaration(
        context,
        "destination_batches",
        declaration_name=declaration_name,
    )
    bounds, bounds_query = _ordered_work_bounds(context, declaration_name=declaration_name)
    scopes, scopes_query = _destination_scopes(context, declaration_name=declaration_name)
    return {
        "kind": "declaration",
        "declaration_name": declaration_name,
        "declaration_versions": declaration_count,
        "state_current_rows": current_count,
        "ordered_work_rows": work_count,
        "ordered_work_bounds": bounds,
        "destination_scope_count": progress_count,
        "destination_batch_count": batch_count,
        "destination_scopes": scopes,
        "sql_context": sql_context(
            context,
            relations=(
                "declarations",
                "state_current",
                "ordered_work",
                "destination_progress",
                "destination_batches",
            ),
            scope_keys={"declaration_name": declaration_name},
            sql=(
                declaration_query,
                current_count_query,
                work_count_query,
                progress_count_query,
                batch_count_query,
                bounds_query,
                scopes_query,
            ),
        ),
    }


def inspect_destination_scope(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
) -> RuntimeOperationSummary:
    validation_helpers.validate_progress_scope(scope)
    progress = _progress_record(context, scope=scope)
    batch_statuses, batch_status_query = _batch_status_counts(context, scope=scope)
    retryable_count, retryable_query = _retryable_count(context, scope=scope)
    recent_runs, recent_runs_query = _recent_scope_reports(context, scope=scope)
    return {
        "kind": "destination_scope",
        "scope": _scope_dict(scope),
        "progress": progress,
        "batch_counts": batch_statuses,
        "retryable_unresolved_batches": retryable_count,
        "recent_reports": recent_runs,
        "sql_context": sql_context(
            context,
            relations=(
                "destination_progress",
                "destination_batches",
                "sync_reports",
            ),
            scope_keys=_scope_dict(scope),
            sql=(batch_status_query, retryable_query, recent_runs_query),
        ),
    }


def inspect_collect_id(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    collect_id: str,
) -> RuntimeOperationSummary:
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    validation_helpers.validate_collect_id(collect_id)
    work_count, work_query = _count_for_declaration_collect(
        context,
        "ordered_work",
        declaration_name=declaration_name,
        collect_id=collect_id,
    )
    current_count, current_query = _count_for_declaration_collect(
        context,
        "state_current",
        declaration_name=declaration_name,
        collect_id=collect_id,
    )
    batch_count, batch_query = _batch_overlap_count(
        context,
        declaration_name=declaration_name,
        collect_id=collect_id,
    )
    return {
        "kind": "collect_id",
        "declaration_name": declaration_name,
        "collect_id": collect_id,
        "ordered_work_rows": work_count,
        "state_current_rows": current_count,
        "destination_batch_rows": batch_count,
        "has_destination_batch_evidence": batch_count > 0,
        "sql_context": sql_context(
            context,
            relations=("ordered_work", "state_current", "destination_batches"),
            scope_keys={
                "declaration_name": declaration_name,
                "collect_id": collect_id,
            },
            sql=(work_query, current_query, batch_query),
        ),
    }


def inspect_target_registry(
    context: SqlRuntimeContext,
    *,
    destination_name: str | None = None,
) -> RuntimeOperationSummary:
    if destination_name is not None:
        validation_helpers.validate_identity(destination_name, "destination_name")
    params = context.new_params()
    where = (
        sql_eq_param("binding_name", destination_name, params=params)
        if destination_name is not None
        else None
    )
    total, total_query = count_rows(
        context,
        "target_registry",
        where=where,
        params=params.params,
    )
    source_counts, source_query = _target_source_counts(context, destination_name=destination_name)
    return {
        "kind": "target_registry",
        "destination_name": destination_name,
        "target_count": total,
        "source_counts": source_counts,
        "sql_context": sql_context(
            context,
            relations=("target_registry",),
            scope_keys={"destination_name": destination_name},
            sql=(total_query, source_query),
        ),
    }


def inspect_run(
    context: SqlRuntimeContext,
    *,
    run_id: str,
) -> RuntimeOperationSummary:
    validation_helpers.validate_identity(run_id, "run_id")
    run = _run_record(context, run_id=run_id)
    report_count, report_query = _count_for_run(context, "sync_reports", run_id=run_id)
    batch_count, batch_query = _count_for_run(context, "destination_batches", run_id=run_id)
    return {
        "kind": "run",
        "run_id": run_id,
        "run": run,
        "sync_report_count": report_count,
        "destination_batch_count": batch_count,
        "sql_context": sql_context(
            context,
            relations=(
                "runs",
                "sync_reports",
                "destination_batches",
            ),
            scope_keys={"run_id": run_id},
            sql=(report_query, batch_query),
        ),
    }


def _count_for_declaration(
    context: SqlRuntimeContext,
    relation: str,
    *,
    declaration_name: str,
) -> tuple[int, Any]:
    params = context.new_params()
    return count_rows(
        context,
        relation,
        where=sql_eq_param("declaration_name", declaration_name, params=params),
        params=params.params,
    )


def _count_for_declaration_collect(
    context: SqlRuntimeContext,
    relation: str,
    *,
    declaration_name: str,
    collect_id: str,
) -> tuple[int, Any]:
    params = context.new_params()
    return count_rows(
        context,
        relation,
        where=sql_and(
            sql_eq_param("declaration_name", declaration_name, params=params),
            sql_eq_param("collect_id", collect_id, params=params),
        ),
        params=params.params,
    )


def _count_for_run(
    context: SqlRuntimeContext,
    relation: str,
    *,
    run_id: str,
) -> tuple[int, Any]:
    params = context.new_params()
    return count_rows(
        context,
        relation,
        where=sql_eq_param("run_id", run_id, params=params),
        params=params.params,
    )


def _ordered_work_bounds(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
) -> tuple[dict[str, str | int | None], Any]:
    params = context.new_params()
    where = sql_eq_param("declaration_name", declaration_name, params=params)
    query = (
        select(
            exp.Min(this=column("collect_id")),
            exp.Max(this=column("collect_id")),
            exp.Min(this=column("sequence_order")),
            exp.Max(this=column("sequence_order")),
        )
        .from_(table(context.runtime_relation("ordered_work")))
        .where(where)
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    row = context.connection.execute(compiled.sql, compiled.params).fetchone()
    values = row if row is not None else (None, None, None, None)
    return {
        "first_collect_id": _optional_str(values[0]),
        "last_collect_id": _optional_str(values[1]),
        "first_sequence_order": _optional_int(values[2]),
        "last_sequence_order": _optional_int(values[3]),
    }, compiled


def _destination_scopes(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
) -> tuple[list[dict[str, object]], Any]:
    params = context.new_params()
    query = (
        select("sync_name", "destination_name", "surface", "family", "declaration_name")
        .distinct()
        .from_(table(context.runtime_relation("destination_progress")))
        .where(sql_eq_param("declaration_name", declaration_name, params=params))
        .limit(20)
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    rows = context.connection.execute(compiled.sql, compiled.params).fetchall()
    return [
        {
            "sync_name": row[0],
            "destination_name": row[1],
            "surface": row[2],
            "family": row[3],
            "declaration_name": row[4],
        }
        for row in rows
    ], compiled


def _progress_record(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
) -> dict[str, object] | None:
    params = context.new_params()
    query = row_read(
        context.runtime_relation("destination_progress"),
        ("position_json", "updated_at"),
        where=sql_and(
            sql_eq_param("sync_name", scope.sync_name, params=params),
            sql_eq_param("destination_name", scope.destination_name, params=params),
            sql_eq_param("surface", scope.surface, params=params),
            sql_eq_param("family", scope.family, params=params),
            sql_eq_param("declaration_name", scope.declaration_name, params=params),
        ),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    row = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if row is None:
        return None
    return {"position_json": row[0], "updated_at": str(row[1])}


def _batch_status_counts(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
) -> tuple[dict[str, int], Any]:
    params = context.new_params()
    query = (
        select("status", "completion_state", exp.Count(this=exp.Star()).as_("row_count"))
        .from_(table(context.runtime_relation("destination_batches")))
        .where(_scope_where(scope, params))
        .group_by(column("status"), column("completion_state"))
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    rows = context.connection.execute(compiled.sql, compiled.params).fetchall()
    return {f"{row[0]}:{row[1]}": int(row[2]) for row in rows}, compiled


def _retryable_count(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
) -> tuple[int, Any]:
    params = context.new_params()
    return count_rows(
        context,
        "destination_batches",
        where=sql_and(
            _scope_where(scope, params),
            sql_eq_param("completion_state", "unresolved", params=params),
            sql_eq_param("retry_eligible", True, params=params),
        ),
        params=params.params,
    )


def _recent_scope_reports(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
) -> tuple[list[dict[str, object]], Any]:
    params = context.new_params()
    query = (
        select("run_id", "sync_name", "status", "failure_category", "created_at")
        .from_(table(context.runtime_relation("sync_reports")))
        .where(
            sql_and(
                sql_eq_param("sync_name", scope.sync_name, params=params),
                sql_eq_param("surface", scope.surface, params=params),
                sql_eq_param("declaration_name", scope.declaration_name, params=params),
            )
        )
        .order_by(sql_order("created_at", desc=True))
        .limit(10)
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    rows = context.connection.execute(compiled.sql, compiled.params).fetchall()
    return [
        {
            "run_id": row[0],
            "sync_name": row[1],
            "status": row[2],
            "failure_category": row[3],
            "created_at": str(row[4]),
        }
        for row in rows
    ], compiled


def _batch_overlap_count(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    collect_id: str,
) -> tuple[int, Any]:
    params = context.new_params()
    return count_rows(
        context,
        "destination_batches",
        where=sql_and(
            sql_eq_param("declaration_name", declaration_name, params=params),
            exp.LTE(this=column("first_collect_id"), expression=params.add(collect_id)),
            exp.GTE(this=column("last_collect_id"), expression=params.add(collect_id)),
        ),
        params=params.params,
    )


def _target_source_counts(
    context: SqlRuntimeContext,
    *,
    destination_name: str | None,
) -> tuple[dict[str, int], Any]:
    params = context.new_params()
    query = (
        select("source", exp.Count(this=exp.Star()).as_("row_count"))
        .from_(table(context.runtime_relation("target_registry")))
        .group_by(column("source"))
    )
    if destination_name is not None:
        query = query.where(sql_eq_param("binding_name", destination_name, params=params))
    compiled = render_sql(query, dialect=context.dialect, params=params)
    rows = context.connection.execute(compiled.sql, compiled.params).fetchall()
    return {str(row[0]): int(row[1]) for row in rows}, compiled


def _run_record(context: SqlRuntimeContext, *, run_id: str) -> dict[str, object] | None:
    params = context.new_params()
    query = row_read(
        context.runtime_relation("runs"),
        (
            "run_id",
            "runner_name",
            "status",
            "dry_run",
            "script_path",
            "script_content_hash",
            "started_at",
            "completed_at",
        ),
        where=sql_eq_param("run_id", run_id, params=params),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    row = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if row is None:
        return None
    return {
        "run_id": row[0],
        "runner_name": row[1],
        "status": row[2],
        "dry_run": bool(row[3]),
        "script_path": row[4],
        "script_content_hash": row[5],
        "started_at": str(row[6]),
        "completed_at": None if row[7] is None else str(row[7]),
    }


def _scope_where(scope: DestinationProgressScope, params: SqlParamAllocator) -> SqlCondition:
    condition = sql_and(
        sql_eq_param("sync_name", scope.sync_name, params=params),
        sql_eq_param("destination_name", scope.destination_name, params=params),
        sql_eq_param("surface", scope.surface, params=params),
        sql_eq_param("family", scope.family, params=params),
        sql_eq_param("declaration_name", scope.declaration_name, params=params),
    )
    if condition is None:
        raise ValueError("SQL inspect scope requires filter conditions.")
    return condition


def _scope_dict(scope: DestinationProgressScope) -> dict[str, str]:
    return {
        "sync_name": scope.sync_name,
        "destination_name": scope.destination_name,
        "surface": scope.surface,
        "family": scope.family,
        "declaration_name": scope.declaration_name,
    }


def _optional_int(value: object) -> int | None:
    return None if value is None else int(cast(Any, value))


def _optional_str(value: object) -> str | None:
    return None if value is None else str(cast(Any, value))
