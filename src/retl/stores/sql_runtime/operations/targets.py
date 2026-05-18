from __future__ import annotations

from sqlglot import exp

from retl.errors import DeclarationValidationError
from retl.sql import SqlCondition, sql_and, sql_eq_param
from retl.stores.contracts import DestinationProgressScope
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.operations import RuntimeOperationSummary, delete_rows


def reset_target_registry(
    context: SqlRuntimeContext,
    *,
    destination_name: str | None = None,
    scope: DestinationProgressScope | None = None,
    target: str | None = None,
) -> RuntimeOperationSummary:
    if destination_name is not None:
        validation_helpers.validate_identity(destination_name, "destination_name")
    if scope is not None:
        validation_helpers.validate_progress_scope(scope)
    logical_target = _logical_target(target)
    params = context.new_params()
    conditions: list[SqlCondition | None] = []
    if destination_name is not None:
        conditions.append(sql_eq_param("binding_name", destination_name, params=params))
    if scope is not None:
        conditions.extend(
            [
                sql_eq_param("binding_name", scope.destination_name, params=params),
                sql_eq_param("surface", scope.surface, params=params),
            ]
        )
    if logical_target is not None:
        conditions.append(sql_eq_param("logical_target", logical_target, params=params))
    if not conditions:
        where: SqlCondition = exp.true()
    else:
        combined = sql_and(*conditions)
        if combined is None:
            raise DeclarationValidationError("target registry reset requires a filter.")
        where = combined
    deleted, compiled = delete_rows(
        context,
        "target_registry",
        where=where,
        params=params.params,
    )
    return {
        "kind": "reset_target_registry",
        "destination_name": destination_name,
        "scope": _scope_dict(scope) if scope is not None else None,
        "target": logical_target,
        "deleted_rows": {"target_registry": deleted},
        "sql_context": {
            "queries": [{"sql": compiled.sql, "parameters": len(compiled.params)}],
            "relations": {"target_registry": context.render_runtime_relation("target_registry")},
        },
    }


def _logical_target(target: str | None) -> str | None:
    if target is None:
        return None
    if not isinstance(target, str):
        raise DeclarationValidationError("target registry reset target must be a string.")
    validation_helpers.validate_identity(target, "target")
    return target


def _scope_dict(scope: DestinationProgressScope) -> dict[str, str]:
    return {
        "sync_name": scope.sync_name,
        "destination_name": scope.destination_name,
        "surface": scope.surface,
        "family": scope.family,
        "declaration_name": scope.declaration_name,
    }


__all__ = ["reset_target_registry"]
