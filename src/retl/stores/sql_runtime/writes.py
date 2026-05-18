from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, cast, runtime_checkable

from sqlglot import exp

from retl.sql import (
    ColumnName,
    CompiledSql,
    RelationPath,
    SqlCondition,
    SqlRenderable,
    SqlRowWriteInput,
    SqlRuntimeUpsert,
    SqlUpsertAssignment,
    alias_column,
    column,
    identifier,
    render_sql,
    row_insert,
    row_write_input,
    runtime_upsert,
    table,
)
from retl.stores.sql_runtime.context import SqlRuntimeContext


class _RuntimeUpsertDialect(Protocol):
    def upsert_sql(self, upsert: SqlRuntimeUpsert) -> str: ...


@runtime_checkable
class _RuntimeUpdateManyDialect(Protocol):
    def update_many_sql(
        self,
        *,
        relation: RelationPath,
        source: exp.Subquery,
        key_columns: Sequence[ColumnName],
        update_columns: Sequence[ColumnName],
        update_assignments: Sequence[SqlUpsertAssignment],
    ) -> str: ...


@runtime_checkable
class _RuntimeWriteCastDialect(Protocol):
    def runtime_column_cast_type(self, *, table_name: str, column_name: str) -> str | None: ...


RowWriteValues = Mapping[ColumnName | str, object] | Sequence[tuple[ColumnName | str, object]]


def compile_runtime_insert(
    context: SqlRuntimeContext,
    relation: str,
    values: RowWriteValues,
) -> CompiledSql:
    return compile_runtime_insert_many(context, relation, (values,))


def execute_runtime_insert(
    context: SqlRuntimeContext,
    relation: str,
    values: RowWriteValues,
) -> None:
    execute_runtime_insert_many(context, relation, (values,))


def compile_runtime_insert_many(
    context: SqlRuntimeContext,
    relation: str,
    rows: Sequence[RowWriteValues],
) -> CompiledSql:
    if not rows:
        raise ValueError("SQL runtime inserts require at least one row.")
    params = context.new_params()
    relation_path = context.runtime_relation(relation)
    write_rows = tuple(
        row_write_input(row, params=params, param_prefix=f"row_{index}")
        for index, row in enumerate(rows, start=1)
    )
    first_columns = write_rows[0].columns
    for row in write_rows[1:]:
        if row.columns != first_columns:
            raise ValueError("SQL runtime batch inserts require identical row columns.")
    if len(write_rows) == 1:
        statement = row_insert(
            relation_path,
            _cast_row_for_runtime_write(context, relation_path, write_rows[0]),
        )
    else:
        target = exp.Schema(
            this=table(relation_path),
            expressions=[identifier(column_name) for column_name in first_columns],
        )
        values = exp.Values(
            expressions=[
                exp.Tuple(
                    expressions=list(
                        _cast_values_for_runtime_write(
                            context,
                            relation_path,
                            row.columns,
                            row.values,
                        )
                    )
                )
                for row in write_rows
            ]
        )
        statement = exp.Insert(this=target, expression=values)
    return render_sql(statement, dialect=context.dialect, params=params)


def execute_runtime_insert_many(
    context: SqlRuntimeContext,
    relation: str,
    rows: Sequence[RowWriteValues],
) -> None:
    if context.append_writer is not None and context.append_writer.supports(relation):
        context.append_writer.append_rows(relation, rows)
        return
    compiled = compile_runtime_insert_many(context, relation, rows)
    context.connection.execute(compiled.sql, compiled.params)


def compile_runtime_update(
    context: SqlRuntimeContext,
    relation: str,
    values: RowWriteValues,
    *,
    where_values: RowWriteValues,
    update_assignments: Sequence[SqlUpsertAssignment] = (),
) -> CompiledSql:
    params = context.new_params()
    relation_path = context.runtime_relation(relation)
    row = row_write_input(values, params=params, param_prefix="set")
    where_row = row_write_input(where_values, params=params, param_prefix="where")
    assignments = [
        exp.EQ(this=column(column_name), expression=value)
        for column_name, value in zip(
            row.columns,
            _cast_values_for_runtime_write(context, relation_path, row.columns, row.values),
            strict=True,
        )
    ]
    assignments.extend(
        exp.EQ(this=column(assignment.column), expression=assignment.value)
        for assignment in update_assignments
    )
    query = exp.Update(
        this=table(relation_path),
        expressions=assignments,
    ).where(
        _and_conditions(
            [
                exp.EQ(this=column(column_name), expression=value)
                for column_name, value in zip(
                    where_row.columns,
                    _cast_values_for_runtime_write(
                        context,
                        relation_path,
                        where_row.columns,
                        where_row.values,
                    ),
                    strict=True,
                )
            ]
        )
    )
    return render_sql(query, dialect=context.dialect, params=params)


def execute_runtime_update(
    context: SqlRuntimeContext,
    relation: str,
    values: RowWriteValues,
    *,
    where_values: RowWriteValues,
    update_assignments: Sequence[SqlUpsertAssignment] = (),
) -> None:
    compiled = compile_runtime_update(
        context,
        relation,
        values,
        where_values=where_values,
        update_assignments=update_assignments,
    )
    context.connection.execute(compiled.sql, compiled.params)


def compile_runtime_update_many(
    context: SqlRuntimeContext,
    relation: str,
    rows: Sequence[RowWriteValues],
    *,
    key_columns: Sequence[ColumnName | str],
    update_columns: Sequence[ColumnName | str],
    update_assignments: Sequence[SqlUpsertAssignment] = (),
) -> CompiledSql:
    if not rows:
        raise ValueError("SQL runtime batch updates require at least one row.")
    keys = tuple(_column_name(column_name) for column_name in key_columns)
    updates = tuple(_column_name(column_name) for column_name in update_columns)
    if not keys:
        raise ValueError("SQL runtime batch updates require at least one key column.")
    if not updates and not update_assignments:
        raise ValueError("SQL runtime batch updates require at least one update.")
    params = context.new_params()
    relation_path = context.runtime_relation(relation)
    write_rows = tuple(
        row_write_input(row, params=params, param_prefix=f"row_{index}")
        for index, row in enumerate(rows, start=1)
    )
    first_columns = write_rows[0].columns
    row_column_names = {column_name.value for column_name in first_columns}
    for column_name in (*keys, *updates):
        if column_name.value not in row_column_names:
            raise ValueError(
                f"SQL runtime batch update column `{column_name.value}` is not in row values."
            )
    for row in write_rows[1:]:
        if row.columns != first_columns:
            raise ValueError("SQL runtime batch updates require identical row columns.")
    source = _runtime_update_source_rowset(
        context,
        relation_path,
        write_rows,
        source_alias="source",
    )
    assignments = [
        exp.EQ(this=column(column_name), expression=alias_column("source", column_name))
        for column_name in updates
    ]
    assignments.extend(
        exp.EQ(this=column(assignment.column), expression=assignment.value)
        for assignment in update_assignments
    )
    if isinstance(context.dialect, _RuntimeUpdateManyDialect):
        return CompiledSql(
            sql=context.dialect.update_many_sql(
                relation=relation_path,
                source=source,
                key_columns=keys,
                update_columns=updates,
                update_assignments=update_assignments,
            ),
            params=params.params,
        )
    query = exp.Update(
        this=exp.alias_(table(relation_path), "target"),
        expressions=assignments,
        from_=exp.From(this=source),
    ).where(
        _and_conditions(
            [
                exp.EQ(
                    this=alias_column("target", column_name),
                    expression=alias_column("source", column_name),
                )
                for column_name in keys
            ]
        )
    )
    return render_sql(query, dialect=context.dialect, params=params)


def execute_runtime_update_many(
    context: SqlRuntimeContext,
    relation: str,
    rows: Sequence[RowWriteValues],
    *,
    key_columns: Sequence[ColumnName | str],
    update_columns: Sequence[ColumnName | str],
    update_assignments: Sequence[SqlUpsertAssignment] = (),
) -> None:
    compiled = compile_runtime_update_many(
        context,
        relation,
        rows,
        key_columns=key_columns,
        update_columns=update_columns,
        update_assignments=update_assignments,
    )
    context.connection.execute(compiled.sql, compiled.params)


def compile_runtime_upsert(
    context: SqlRuntimeContext,
    relation: str,
    values: RowWriteValues,
    *,
    key_columns: Sequence[ColumnName | str],
    update_columns: Sequence[ColumnName | str] = (),
    update_assignments: Sequence[SqlUpsertAssignment] = (),
) -> CompiledSql:
    params = context.new_params()
    row = row_write_input(values, params=params)
    upsert = runtime_upsert(
        context.runtime_relation(relation),
        row,
        key_columns=key_columns,
        update_columns=update_columns,
        update_assignments=update_assignments,
    )
    sql = cast(_RuntimeUpsertDialect, context.dialect).upsert_sql(upsert)
    return CompiledSql(sql=sql, params=params.params)


def execute_runtime_upsert(
    context: SqlRuntimeContext,
    relation: str,
    values: RowWriteValues,
    *,
    key_columns: Sequence[ColumnName | str],
    update_columns: Sequence[ColumnName | str] = (),
    update_assignments: Sequence[SqlUpsertAssignment] = (),
) -> None:
    compiled = compile_runtime_upsert(
        context,
        relation,
        values,
        key_columns=key_columns,
        update_columns=update_columns,
        update_assignments=update_assignments,
    )
    context.connection.execute(compiled.sql, compiled.params)


def _cast_row_for_runtime_write(
    context: SqlRuntimeContext,
    relation: RelationPath,
    row: SqlRowWriteInput,
) -> SqlRowWriteInput:
    return SqlRowWriteInput(
        columns=row.columns,
        values=_cast_values_for_runtime_write(context, relation, row.columns, row.values),
    )


def _runtime_update_source_rowset(
    context: SqlRuntimeContext,
    relation: RelationPath,
    rows: Sequence[SqlRowWriteInput],
    *,
    source_alias: str,
) -> exp.Subquery:
    selects = [
        exp.select(
            *(
                exp.alias_(value, column_name.value, quoted=True)
                for column_name, value in zip(
                    row.columns,
                    _cast_values_for_runtime_write(context, relation, row.columns, row.values),
                    strict=True,
                )
            )
        )
        for row in rows
    ]
    source: exp.Expression = selects[0]
    for row_select in selects[1:]:
        source = exp.Union(this=source, expression=row_select, distinct=False)
    return exp.Subquery(
        this=source,
        alias=exp.TableAlias(this=exp.to_identifier(source_alias, quoted=False)),
    )


def _column_name(value: ColumnName | str) -> ColumnName:
    return value if isinstance(value, ColumnName) else ColumnName(value)


def _cast_values_for_runtime_write(
    context: SqlRuntimeContext,
    relation: RelationPath,
    columns: Sequence[ColumnName],
    values: Sequence[SqlRenderable],
) -> tuple[SqlRenderable, ...]:
    dialect = context.dialect
    if not isinstance(dialect, _RuntimeWriteCastDialect):
        return tuple(values)
    casted: list[SqlRenderable] = []
    for column_name, value in zip(columns, values, strict=True):
        cast_type = dialect.runtime_column_cast_type(
            table_name=relation.name.value,
            column_name=column_name.value,
        )
        if cast_type is None:
            casted.append(value)
        else:
            casted.append(exp.Cast(this=value, to=exp.DataType.build(cast_type)))
    return tuple(casted)


def _and_conditions(conditions: Sequence[SqlCondition]) -> SqlCondition:
    if not conditions:
        raise ValueError("SQL runtime updates require at least one WHERE value.")
    current: SqlCondition = conditions[0]
    for condition in conditions[1:]:
        current = exp.and_(current, condition)
    return current


__all__ = [
    "compile_runtime_insert",
    "compile_runtime_insert_many",
    "compile_runtime_update",
    "compile_runtime_update_many",
    "compile_runtime_upsert",
    "execute_runtime_insert",
    "execute_runtime_insert_many",
    "execute_runtime_update",
    "execute_runtime_update_many",
    "execute_runtime_upsert",
]
