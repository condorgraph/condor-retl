from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from retl.sql import (
    CompiledSql,
    SqlCondition,
    count_read,
    filtered_delete,
    render_sql,
    sql_and,
    sql_eq_param,
)
from retl.stores.sql_runtime.context import SqlRuntimeContext

RuntimeOperationSummary = dict[str, Any]


def sql_context(
    context: SqlRuntimeContext,
    *,
    relations: Sequence[str],
    scope_keys: Mapping[str, object] | None = None,
    sql: Sequence[CompiledSql] = (),
) -> RuntimeOperationSummary:
    return {
        "backend": context.runtime_space.backend_name,
        "runtime_space": {
            "database": context.runtime_space.database,
            "schema": context.runtime_space.schema,
        },
        "relations": {
            relation: context.render_runtime_relation(relation) for relation in relations
        },
        "scope_keys": dict(scope_keys or {}),
        "queries": [
            {
                "sql": compiled.sql,
                "parameters": len(tuple(compiled.params)),
            }
            for compiled in sql
        ],
    }


def eq_conditions(
    context: SqlRuntimeContext,
    values: Mapping[str, object | None],
) -> tuple[SqlCondition | None, tuple[object, ...]]:
    params = context.new_params()
    conditions: list[SqlCondition | None] = []
    for column_name, value in values.items():
        if value is not None:
            conditions.append(sql_eq_param(column_name, value, params=params))
    where = sql_and(*conditions)
    return where, params.params


def count_rows(
    context: SqlRuntimeContext,
    relation: str,
    *,
    where: SqlCondition | None = None,
    params: Sequence[object] = (),
) -> tuple[int, CompiledSql]:
    compiled = render_sql(
        count_read(context.runtime_relation(relation), where=where),
        dialect=context.dialect,
        params=params,
    )
    row = context.connection.execute(compiled.sql, compiled.params).fetchone()
    return (int(row[0]) if row is not None else 0), compiled


def delete_rows(
    context: SqlRuntimeContext,
    relation: str,
    *,
    where: SqlCondition,
    params: Sequence[object] = (),
) -> tuple[int, CompiledSql]:
    before, _ = count_rows(context, relation, where=where, params=params)
    compiled = render_sql(
        filtered_delete(context.runtime_relation(relation), where=where),
        dialect=context.dialect,
        params=params,
    )
    context.connection.execute(compiled.sql, compiled.params)
    return before, compiled


__all__ = [
    "RuntimeOperationSummary",
    "count_rows",
    "delete_rows",
    "eq_conditions",
    "sql_context",
]
