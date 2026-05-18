from __future__ import annotations

from sqlglot import exp

from retl.errors import DeclarationValidationError
from retl.sql import (
    SqlCondition,
    SqlParamAllocator,
    column,
    filtered_delete,
    render_sql,
    sql_and,
    sql_eq_param,
)
from retl.stores.contracts import DestinationProgressScope, WorkFamily
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.operations import RuntimeOperationSummary, count_rows, delete_rows
from retl.stores.sql_runtime.schema import runtime_table_names


def reset_runtime_store(context: SqlRuntimeContext) -> RuntimeOperationSummary:
    deleted: dict[str, int | None] = {}
    # Delete child/evidence tables before authority tables with referenced identities.
    for relation in (
        "destination_batches",
        "sync_reports",
        "runs",
        "pending_work_cursors",
        "state_current_cursors",
        "destination_progress",
        "target_registry",
        "state_current",
        "ordered_work",
        "declarations",
    ):
        deleted[relation] = _delete_all_rows(context, relation)
    return {
        "kind": "reset_runtime_store",
        "deleted_rows": deleted,
        "deleted_rows_exact": all(count is not None for count in deleted.values()),
        "runtime_tables": sorted(runtime_table_names()),
    }


def _delete_all_rows(context: SqlRuntimeContext, relation: str) -> int | None:
    delete_all_sql = context.delete_all_runtime_rows_sql(relation)
    if delete_all_sql is not None:
        context.connection.execute(delete_all_sql)
        return None
    compiled = render_sql(
        filtered_delete(context.runtime_relation(relation), where=exp.true()),
        dialect=context.dialect,
    )
    context.connection.execute(compiled.sql, compiled.params)
    return None


def reset_destination_scope(
    context: SqlRuntimeContext,
    *,
    scope: DestinationProgressScope,
) -> RuntimeOperationSummary:
    validation_helpers.validate_progress_scope(scope)
    deleted: dict[str, int] = {}
    deleted["destination_batches"] = _delete_for_scope(context, "destination_batches", scope=scope)
    deleted["destination_progress"] = _delete_for_scope(
        context,
        "destination_progress",
        scope=scope,
    )
    deleted["pending_work_cursors"] = _delete_for_scope(
        context,
        "pending_work_cursors",
        scope=scope,
    )
    return {"kind": "reset_destination_scope", "scope": _scope_dict(scope), "deleted_rows": deleted}


def delete_collect_id(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    collect_id: str,
    force: bool = False,
) -> RuntimeOperationSummary:
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    validation_helpers.validate_collect_id(collect_id)
    _validate_force(force)
    dependent_batches = _dependent_batch_count(
        context,
        declaration_name=declaration_name,
        collect_id=collect_id,
    )
    current_rows = _state_current_collect_count(
        context,
        declaration_name=declaration_name,
        collect_id=collect_id,
    )
    if dependent_batches and not force:
        raise DeclarationValidationError(
            "collect ID has destination batch evidence; reset destination scopes or "
            "pass force=True before deleting shared collect output."
        )
    if current_rows and not force:
        raise DeclarationValidationError(
            "collect ID contributes to state_current; rebaseline state or pass force=True."
        )
    deleted = {
        "ordered_work": _delete_declaration_collect(
            context,
            "ordered_work",
            declaration_name=declaration_name,
            collect_id=collect_id,
        ),
        "state_current": _delete_declaration_collect(
            context,
            "state_current",
            declaration_name=declaration_name,
            collect_id=collect_id,
        ),
    }
    return {
        "kind": "delete_collect_id",
        "declaration_name": declaration_name,
        "collect_id": collect_id,
        "dependent_destination_batches": dependent_batches,
        "deleted_rows": deleted,
    }


def delete_ordered_work_range(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    first_collect_id: str,
    first_sequence_order: int,
    last_collect_id: str,
    last_sequence_order: int,
    family: WorkFamily = "state",
    force: bool = False,
) -> RuntimeOperationSummary:
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    validation_helpers.validate_family(family)
    validation_helpers.validate_collect_id(first_collect_id)
    validation_helpers.validate_collect_id(last_collect_id)
    validation_helpers.validate_nonnegative_int(first_sequence_order, "first_sequence_order")
    validation_helpers.validate_nonnegative_int(last_sequence_order, "last_sequence_order")
    _validate_force(force)
    dependent_batches = _dependent_batch_range_count(
        context,
        declaration_name=declaration_name,
        first_collect_id=first_collect_id,
        last_collect_id=last_collect_id,
    )
    if dependent_batches and not force:
        raise DeclarationValidationError(
            "ordered work range has destination batch evidence; skip or reset dependent "
            "destination scopes before deleting it."
        )
    params = context.new_params()
    where = _required_condition(
        sql_and(
            sql_eq_param("family", family, params=params),
            sql_eq_param("declaration_name", declaration_name, params=params),
            exp.or_(
                column("collect_id") > params.add(first_collect_id),
                exp.and_(
                    column("collect_id").eq(params.add(first_collect_id)),
                    column("sequence_order") >= params.add(first_sequence_order),
                ),
            ),
            exp.or_(
                column("collect_id") < params.add(last_collect_id),
                exp.and_(
                    column("collect_id").eq(params.add(last_collect_id)),
                    column("sequence_order") <= params.add(last_sequence_order),
                ),
            ),
        )
    )
    deleted, _ = delete_rows(
        context,
        "ordered_work",
        where=where,
        params=params.params,
    )
    return {
        "kind": "delete_ordered_work_range",
        "declaration_name": declaration_name,
        "family": family,
        "dependent_destination_batches": dependent_batches,
        "deleted_rows": {"ordered_work": deleted},
    }


def rebaseline_state(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    source_name: str,
    force: bool = False,
) -> RuntimeOperationSummary:
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    validation_helpers.validate_identity(source_name, "source_name")
    _validate_force(force)
    dependent_batches = _declaration_batch_count(context, declaration_name=declaration_name)
    if dependent_batches and not force:
        raise DeclarationValidationError(
            "state declaration has destination batch evidence; skip or reset destination scopes "
            "before rebaseline, or pass force=True."
        )
    deleted = {
        "state_current": _delete_state_current(
            context,
            declaration_name=declaration_name,
            source_name=source_name,
        ),
        "state_current_cursors": _delete_state_current_cursors(
            context,
            declaration_name=declaration_name,
            source_name=source_name,
        ),
    }
    return {
        "kind": "rebaseline_state",
        "declaration_name": declaration_name,
        "source_name": source_name,
        "dependent_destination_batches": dependent_batches,
        "deleted_rows": deleted,
    }


def _delete_for_scope(
    context: SqlRuntimeContext,
    relation: str,
    *,
    scope: DestinationProgressScope,
) -> int:
    params = context.new_params()
    deleted, _ = delete_rows(
        context,
        relation,
        where=_scope_where(scope, params),
        params=params.params,
    )
    return deleted


def _delete_declaration_collect(
    context: SqlRuntimeContext,
    relation: str,
    *,
    declaration_name: str,
    collect_id: str,
) -> int:
    params = context.new_params()
    deleted, _ = delete_rows(
        context,
        relation,
        where=_required_condition(
            sql_and(
                sql_eq_param("declaration_name", declaration_name, params=params),
                sql_eq_param("collect_id", collect_id, params=params),
            )
        ),
        params=params.params,
    )
    return deleted


def _delete_state_current(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    source_name: str,
) -> int:
    params = context.new_params()
    deleted, _ = delete_rows(
        context,
        "state_current",
        where=_required_condition(
            sql_and(
                sql_eq_param("declaration_name", declaration_name, params=params),
                sql_eq_param("source_name", source_name, params=params),
            )
        ),
        params=params.params,
    )
    return deleted


def _delete_state_current_cursors(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    source_name: str,
) -> int:
    params = context.new_params()
    deleted, _ = delete_rows(
        context,
        "state_current_cursors",
        where=_required_condition(
            sql_and(
                sql_eq_param("declaration_name", declaration_name, params=params),
                sql_eq_param("source_name", source_name, params=params),
            )
        ),
        params=params.params,
    )
    return deleted


def _dependent_batch_count(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    collect_id: str,
) -> int:
    params = context.new_params()
    count, _ = count_rows(
        context,
        "destination_batches",
        where=sql_and(
            sql_eq_param("declaration_name", declaration_name, params=params),
            exp.LTE(this=column("first_collect_id"), expression=params.add(collect_id)),
            exp.GTE(this=column("last_collect_id"), expression=params.add(collect_id)),
        ),
        params=params.params,
    )
    return count


def _dependent_batch_range_count(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    first_collect_id: str,
    last_collect_id: str,
) -> int:
    params = context.new_params()
    count, _ = count_rows(
        context,
        "destination_batches",
        where=sql_and(
            sql_eq_param("declaration_name", declaration_name, params=params),
            exp.LTE(
                this=column("first_collect_id"),
                expression=params.add(last_collect_id),
            ),
            exp.GTE(
                this=column("last_collect_id"),
                expression=params.add(first_collect_id),
            ),
        ),
        params=params.params,
    )
    return count


def _state_current_collect_count(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    collect_id: str,
) -> int:
    params = context.new_params()
    count, _ = count_rows(
        context,
        "state_current",
        where=sql_and(
            sql_eq_param("declaration_name", declaration_name, params=params),
            sql_eq_param("collect_id", collect_id, params=params),
        ),
        params=params.params,
    )
    return count


def _declaration_batch_count(context: SqlRuntimeContext, *, declaration_name: str) -> int:
    params = context.new_params()
    count, _ = count_rows(
        context,
        "destination_batches",
        where=sql_eq_param("declaration_name", declaration_name, params=params),
        params=params.params,
    )
    return count


def _scope_where(scope: DestinationProgressScope, params: SqlParamAllocator) -> SqlCondition:
    return _required_condition(
        sql_and(
            sql_eq_param("sync_name", scope.sync_name, params=params),
            sql_eq_param("destination_name", scope.destination_name, params=params),
            sql_eq_param("surface", scope.surface, params=params),
            sql_eq_param("family", scope.family, params=params),
            sql_eq_param("declaration_name", scope.declaration_name, params=params),
        )
    )


def _required_condition(condition: SqlCondition | None) -> SqlCondition:
    if condition is None:
        raise DeclarationValidationError("SQL reset requires at least one filter condition.")
    return condition


def _scope_dict(scope: DestinationProgressScope) -> dict[str, str]:
    return {
        "sync_name": scope.sync_name,
        "destination_name": scope.destination_name,
        "surface": scope.surface,
        "family": scope.family,
        "declaration_name": scope.declaration_name,
    }


def _validate_force(force: bool) -> None:
    if not isinstance(force, bool):
        raise DeclarationValidationError("`force` must be a boolean.")


__all__ = [
    "delete_collect_id",
    "delete_ordered_work_range",
    "rebaseline_state",
    "reset_destination_scope",
    "reset_runtime_store",
]
