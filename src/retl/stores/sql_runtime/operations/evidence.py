from __future__ import annotations

from retl.errors import DeclarationValidationError
from retl.sql import SqlCondition, column, sql_and, sql_eq_param
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.operations import RuntimeOperationSummary, delete_rows


def delete_run_evidence(
    context: SqlRuntimeContext,
    *,
    run_id: str,
) -> RuntimeOperationSummary:
    validation_helpers.validate_identity(run_id, "run_id")
    deleted = {
        "sync_reports": _delete_by_run_id(context, "sync_reports", run_id=run_id),
        "runs": _delete_by_run_id(context, "runs", run_id=run_id),
    }
    return {"kind": "delete_run_evidence", "run_id": run_id, "deleted_rows": deleted}


def delete_report_evidence(
    context: SqlRuntimeContext,
    *,
    run_id: str | None = None,
    sync_name: str | None = None,
) -> RuntimeOperationSummary:
    if run_id is not None:
        validation_helpers.validate_identity(run_id, "run_id")
    if sync_name is not None:
        validation_helpers.validate_identity(sync_name, "sync_name")
    if run_id is None and sync_name is None:
        raise DeclarationValidationError("report evidence cleanup requires run_id or sync_name.")
    params = context.new_params()
    conditions: list[SqlCondition | None] = []
    if run_id is not None:
        conditions.append(sql_eq_param("run_id", run_id, params=params))
    if sync_name is not None:
        conditions.append(sql_eq_param("sync_name", sync_name, params=params))
    where = sql_and(*conditions)
    if where is None:
        raise DeclarationValidationError("report evidence cleanup requires run_id or sync_name.")
    deleted, _ = delete_rows(
        context,
        "sync_reports",
        where=where,
        params=params.params,
    )
    return {
        "kind": "delete_report_evidence",
        "run_id": run_id,
        "sync_name": sync_name,
        "deleted_rows": {"sync_reports": deleted},
    }


def _delete_by_run_id(
    context: SqlRuntimeContext,
    relation: str,
    *,
    run_id: str,
) -> int:
    params = context.new_params()
    deleted, _ = delete_rows(
        context,
        relation,
        where=column("run_id").eq(params.add(run_id)),
        params=params.params,
    )
    return deleted


__all__ = [
    "delete_report_evidence",
    "delete_run_evidence",
]
