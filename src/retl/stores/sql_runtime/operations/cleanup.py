from __future__ import annotations

from datetime import datetime, timedelta

from sqlglot import exp, select

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
from retl.stores.contracts import WorkFamily
from retl.stores.sql_runtime import ordered_work as ordered_work_store
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.operations import RuntimeOperationSummary, count_rows, delete_rows


def cleanup_ordered_work(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    through_collect_id: str | None = None,
    older_than_seconds: int | None = None,
    dry_run: bool = False,
) -> RuntimeOperationSummary:
    validation_helpers.validate_family(family)
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    if through_collect_id is not None:
        validation_helpers.validate_collect_id(through_collect_id)
    if older_than_seconds is not None:
        validation_helpers.validate_nonnegative_int(older_than_seconds, "older_than_seconds")
    if not isinstance(dry_run, bool):
        raise DeclarationValidationError("`dry_run` must be a boolean.")

    age_boundary_collect_id = None
    if older_than_seconds is not None:
        age_boundary_collect_id = _ordered_work_age_boundary(
            context,
            family=family,
            declaration_name=declaration_name,
            older_than_seconds=older_than_seconds,
        )
    requested_through_collect_id = _min_collect_id(through_collect_id, age_boundary_collect_id)
    cleanup = ordered_work_store.cleanup_ordered_work(
        context,
        family=family,
        declaration_name=declaration_name,
        through_collect_id=requested_through_collect_id,
        dry_run=dry_run,
    )
    return {
        "kind": "cleanup_ordered_work",
        "family": cleanup.family,
        "declaration_name": cleanup.declaration_name,
        "requested_through_collect_id": cleanup.requested_through_collect_id,
        "requested_older_than_seconds": older_than_seconds,
        "age_boundary_collect_id": age_boundary_collect_id,
        "safe_through_collect_id": cleanup.safe_through_collect_id,
        "deleted_rows": {"ordered_work": cleanup.deleted_ordered_work_count},
        "retained_pending_count": cleanup.retained_pending_count,
        "dry_run": cleanup.dry_run,
    }


def delete_ordered_work(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    force: bool = False,
) -> RuntimeOperationSummary:
    validation_helpers.validate_family(family)
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    _validate_force(force)
    if not force:
        raise DeclarationValidationError(
            "delete_ordered_work is destructive; pass force=True to delete retained ordered work."
        )
    params = context.new_params()
    deleted, _ = delete_rows(
        context,
        "ordered_work",
        where=_required_condition(
            sql_and(
                sql_eq_param("family", family, params=params),
                sql_eq_param("declaration_name", declaration_name, params=params),
            )
        ),
        params=params.params,
    )
    return {
        "kind": "delete_ordered_work",
        "family": family,
        "declaration_name": declaration_name,
        "deleted_rows": {"ordered_work": deleted},
        "force": force,
    }


def cleanup_cursors(
    context: SqlRuntimeContext,
    *,
    older_than_seconds: int,
    dry_run: bool = False,
) -> RuntimeOperationSummary:
    validation_helpers.validate_nonnegative_int(older_than_seconds, "older_than_seconds")
    if not isinstance(dry_run, bool):
        raise DeclarationValidationError("`dry_run` must be a boolean.")
    cutoff = _cutoff(older_than_seconds)
    deleted = {
        "pending_work_cursors": _cleanup_created_at_relation(
            context,
            "pending_work_cursors",
            cutoff=cutoff,
            dry_run=dry_run,
        ),
        "state_current_cursors": _cleanup_created_at_relation(
            context,
            "state_current_cursors",
            cutoff=cutoff,
            dry_run=dry_run,
        ),
    }
    return {
        "kind": "cleanup_cursors",
        "older_than_seconds": older_than_seconds,
        "deleted_rows": deleted,
        "dry_run": dry_run,
    }


def cleanup_evidence(
    context: SqlRuntimeContext,
    *,
    older_than_seconds: int,
    run_id: str | None = None,
    sync_name: str | None = None,
    dry_run: bool = False,
) -> RuntimeOperationSummary:
    validation_helpers.validate_nonnegative_int(older_than_seconds, "older_than_seconds")
    if run_id is not None:
        validation_helpers.validate_identity(run_id, "run_id")
    if sync_name is not None:
        validation_helpers.validate_identity(sync_name, "sync_name")
    if not isinstance(dry_run, bool):
        raise DeclarationValidationError("`dry_run` must be a boolean.")
    cutoff = _cutoff(older_than_seconds)
    deleted = {
        "sync_reports": _cleanup_sync_reports(
            context,
            cutoff=cutoff,
            run_id=run_id,
            sync_name=sync_name,
            dry_run=dry_run,
        ),
        "runs": _cleanup_runs(
            context,
            cutoff=cutoff,
            run_id=run_id,
            sync_name=sync_name,
            dry_run=dry_run,
        ),
    }
    return {
        "kind": "cleanup_evidence",
        "run_id": run_id,
        "sync_name": sync_name,
        "older_than_seconds": older_than_seconds,
        "deleted_rows": deleted,
        "dry_run": dry_run,
    }


def _ordered_work_age_boundary(
    context: SqlRuntimeContext,
    *,
    family: WorkFamily,
    declaration_name: str,
    older_than_seconds: int,
) -> str | None:
    params = context.new_params()
    query = select(exp.Max(this=column("collect_id"))).from_(
        select("collect_id")
        .from_(table(context.runtime_relation("ordered_work")))
        .where(
            sql_and(
                sql_eq_param("family", family, params=params),
                sql_eq_param("declaration_name", declaration_name, params=params),
            )
        )
        .group_by(column("collect_id"))
        .having(exp.Max(this=column("created_at")) <= params.add(_cutoff(older_than_seconds)))
        .subquery("old_collects")
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    row = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def _cleanup_created_at_relation(
    context: SqlRuntimeContext,
    relation: str,
    *,
    cutoff: datetime,
    dry_run: bool,
) -> int:
    params = context.new_params()
    where = column("created_at") <= params.add(cutoff)
    if dry_run:
        count, _ = count_rows(context, relation, where=where, params=params.params)
        return count
    deleted, _ = delete_rows(context, relation, where=where, params=params.params)
    return deleted


def _cleanup_sync_reports(
    context: SqlRuntimeContext,
    *,
    cutoff: datetime,
    run_id: str | None,
    sync_name: str | None,
    dry_run: bool,
) -> int:
    params = context.new_params()
    conditions: list[SqlCondition | None] = [column("created_at") <= params.add(cutoff)]
    if run_id is not None:
        conditions.append(sql_eq_param("run_id", run_id, params=params))
    if sync_name is not None:
        conditions.append(sql_eq_param("sync_name", sync_name, params=params))
    conditions.append(_not_running_run_condition(context, params=params))
    where = _required_condition(sql_and(*conditions))
    if dry_run:
        count, _ = count_rows(context, "sync_reports", where=where, params=params.params)
        return count
    deleted, _ = delete_rows(context, "sync_reports", where=where, params=params.params)
    return deleted


def _cleanup_runs(
    context: SqlRuntimeContext,
    *,
    cutoff: datetime,
    run_id: str | None,
    sync_name: str | None,
    dry_run: bool,
) -> int:
    if sync_name is not None:
        return 0
    params = context.new_params()
    conditions: list[SqlCondition | None] = [
        column("created_at") <= params.add(cutoff),
        column("status").neq(params.add("running")),
    ]
    if run_id is not None:
        conditions.append(sql_eq_param("run_id", run_id, params=params))
    where = _required_condition(sql_and(*conditions))
    if dry_run:
        count, _ = count_rows(context, "runs", where=where, params=params.params)
        return count
    deleted, _ = delete_rows(context, "runs", where=where, params=params.params)
    return deleted


def _not_running_run_condition(
    context: SqlRuntimeContext,
    *,
    params: SqlParamAllocator,
) -> SqlCondition:
    return exp.not_(
        exp.In(
            this=column("run_id"),
            expressions=[
                select("run_id")
                .from_(table(context.runtime_relation("runs")))
                .where(column("status").eq(params.add("running")))
            ],
        )
    )


def _required_condition(condition: SqlCondition | None) -> SqlCondition:
    if condition is None:
        raise DeclarationValidationError("SQL cleanup requires at least one filter condition.")
    return condition


def _cutoff(older_than_seconds: int) -> datetime:
    return datetime.now() - timedelta(seconds=older_than_seconds)


def _min_collect_id(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _validate_force(force: bool) -> None:
    if not isinstance(force, bool):
        raise DeclarationValidationError("`force` must be a boolean.")


__all__ = [
    "cleanup_cursors",
    "cleanup_evidence",
    "cleanup_ordered_work",
    "delete_ordered_work",
]
