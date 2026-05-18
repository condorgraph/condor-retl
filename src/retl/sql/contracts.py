from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, TypeAlias, cast, runtime_checkable

from sqlglot import exp, select

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQL_CATALOG_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")

# SQLGlot 30's runtime base node is Expr, but mypy does not expose that public
# name. RETL accepts the typed SQLGlot node families it renders directly.
SqlRenderable: TypeAlias = exp.Expression | exp.Condition
SqlCondition: TypeAlias = exp.Condition


class SqlParameterStyle(str, Enum):
    QMARK = "qmark"
    NUMERIC = "numeric"
    FORMAT = "format"
    NAMED = "named"
    PYFORMAT = "pyformat"


def validate_sql_identifier(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("SQL identifiers must be non-empty strings.")
    if not _SQL_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"`{value}` is not a simple SQL identifier. Use ASCII letters, digits, and "
            "underscores, and start with a letter or underscore."
        )
    return value


def validate_sql_catalog_identifier(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("SQL catalog identifiers must be non-empty strings.")
    if not _SQL_CATALOG_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"`{value}` is not a valid SQL catalog identifier. Use ASCII letters, digits, "
            "underscores, and hyphens."
        )
    return value


@dataclass(frozen=True)
class RelationName:
    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", validate_sql_identifier(self.value))

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class CatalogName:
    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", validate_sql_catalog_identifier(self.value))

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class ColumnName:
    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", validate_sql_identifier(self.value))

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, init=False)
class RelationPath:
    name: RelationName
    schema: RelationName | None
    database: CatalogName | None

    def __init__(
        self,
        name: RelationName | str,
        schema: RelationName | str | None = None,
        database: CatalogName | RelationName | str | None = None,
    ) -> None:
        name = name if isinstance(name, RelationName) else RelationName(name)
        schema = (
            schema if isinstance(schema, RelationName) or schema is None else RelationName(schema)
        )
        if isinstance(database, RelationName):
            database = CatalogName(database.value)
        elif not isinstance(database, CatalogName) and database is not None:
            database = CatalogName(database)
        if database is not None and schema is None:
            raise ValueError("SQL relation path database requires relation schema.")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "schema", schema)
        object.__setattr__(self, "database", database)

    @property
    def parts(self) -> tuple[str, ...]:
        values: list[str] = []
        if self.database is not None:
            values.append(self.database.value)
        if self.schema is not None:
            values.append(self.schema.value)
        values.append(self.name.value)
        return tuple(values)


@dataclass(frozen=True, init=False)
class ColumnRef:
    name: ColumnName
    relation: RelationPath | None

    def __init__(self, name: ColumnName | str, relation: RelationPath | None = None) -> None:
        name = name if isinstance(name, ColumnName) else ColumnName(name)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "relation", relation)


SqlColumnExpression: TypeAlias = ColumnName | ColumnRef | str | SqlRenderable
SqlProjection: TypeAlias = SqlColumnExpression


def identifier(name: RelationName | ColumnName | str) -> exp.Identifier:
    value = (
        name.value if isinstance(name, RelationName | ColumnName) else validate_sql_identifier(name)
    )
    return exp.to_identifier(value, quoted=True)


def table(path: RelationPath) -> exp.Table:
    return exp.table_(
        path.name.value,
        db=path.schema.value if path.schema is not None else None,
        catalog=path.database.value if path.database is not None else None,
        quoted=True,
    )


def column(
    ref: ColumnRef | ColumnName | str,
    *,
    relation: RelationPath | None = None,
) -> exp.Column:
    if isinstance(ref, ColumnRef):
        column_name = ref.name
        relation_path = ref.relation
    else:
        column_name = ref if isinstance(ref, ColumnName) else ColumnName(ref)
        relation_path = relation
    table_name = relation_path.name.value if relation_path is not None else None
    db_name = (
        relation_path.schema.value if relation_path is not None and relation_path.schema else None
    )
    catalog_name = (
        relation_path.database.value
        if relation_path is not None and relation_path.database is not None
        else None
    )
    return exp.column(
        column_name.value,
        table=table_name,
        db=db_name,
        catalog=catalog_name,
        quoted=True,
    )


def alias_column(alias: str, name: ColumnName | str) -> exp.Column:
    """Return a quoted column qualified by an unquoted compiler-owned alias."""

    return exp.column(
        (name if isinstance(name, ColumnName) else ColumnName(name)).value,
        table=exp.to_identifier(validate_sql_identifier(alias), quoted=False),
        quoted=True,
    )


def _column_name(value: ColumnName | str) -> ColumnName:
    return value if isinstance(value, ColumnName) else ColumnName(value)


def _column_expression(value: SqlColumnExpression) -> SqlRenderable:
    if isinstance(value, exp.Expression | exp.Condition):
        return value
    return column(value)


def _append_select_where(
    expression: exp.Select,
    where: SqlCondition | None,
) -> exp.Select:
    if where is None:
        return expression
    return expression.where(where, copy=False)


def _append_delete_where(
    expression: exp.Delete,
    where: SqlCondition | None,
) -> exp.Delete:
    if where is None:
        return expression
    return expression.where(where, copy=False)


def _append_limit(expression: exp.Select, limit: SqlRenderable | None) -> exp.Select:
    if limit is None:
        return expression
    return expression.limit(limit, copy=False)


def _select_from(
    relation: RelationPath,
    projections: Sequence[SqlProjection],
    *,
    where: SqlCondition | None = None,
    order_by: Sequence[SqlRenderable] = (),
    limit: SqlRenderable | None = None,
) -> exp.Select:
    if not projections:
        raise ValueError("SQL SELECT helpers require at least one projection.")
    query = exp.select(*(_column_expression(projection) for projection in projections)).from_(
        table(relation)
    )
    query = _append_select_where(query, where)
    if order_by:
        query = query.order_by(*order_by, copy=False)
    return _append_limit(query, limit)


def sql_order(
    column_ref: SqlColumnExpression,
    *,
    desc: bool = False,
) -> exp.Ordered:
    return exp.Ordered(this=_column_expression(column_ref), desc=desc)


def sql_and(*conditions: SqlCondition | None) -> SqlCondition | None:
    active = [condition for condition in conditions if condition is not None]
    if not active:
        return None
    current = active[0]
    for condition in active[1:]:
        current = exp.and_(current, condition)
    return current


def sql_eq_param(
    column_ref: SqlColumnExpression,
    value: object,
    *,
    params: SqlParamAllocator,
    name: str | None = None,
) -> exp.EQ:
    return exp.EQ(this=_column_expression(column_ref), expression=params.add(value, name=name))


def sql_alias(
    expression: SqlRenderable | str,
    alias: str,
    *,
    quoted: bool | None = None,
) -> exp.Expression:
    # SQLGlot 30 annotates `alias_` as Expr, but mypy cannot name that runtime
    # base class through `sqlglot.exp`; keep the gap at this boundary.
    return cast(exp.Expression, exp.alias_(expression, alias, quoted=quoted))


def scalar_read(
    relation: RelationPath,
    column_ref: SqlColumnExpression,
    *,
    where: SqlCondition | None = None,
    order_by: Sequence[SqlRenderable] = (),
    limit: SqlRenderable | None = None,
) -> exp.Select:
    return _select_from(relation, [column_ref], where=where, order_by=order_by, limit=limit)


def row_read(
    relation: RelationPath,
    columns: Sequence[SqlProjection],
    *,
    where: SqlCondition | None = None,
    order_by: Sequence[SqlRenderable] = (),
    limit: SqlRenderable | None = None,
) -> exp.Select:
    return _select_from(relation, columns, where=where, order_by=order_by, limit=limit)


def list_read(
    relation: RelationPath,
    column_ref: SqlColumnExpression,
    *,
    where: SqlCondition | None = None,
    order_by: Sequence[SqlRenderable] = (),
    limit: SqlRenderable | None = None,
) -> exp.Select:
    return scalar_read(relation, column_ref, where=where, order_by=order_by, limit=limit)


def count_read(relation: RelationPath, *, where: SqlCondition | None = None) -> exp.Select:
    return _select_from(relation, [exp.Count(this=exp.Star())], where=where)


def min_read(
    relation: RelationPath,
    column_ref: SqlColumnExpression,
    *,
    where: SqlCondition | None = None,
) -> exp.Select:
    return _select_from(relation, [exp.Min(this=_column_expression(column_ref))], where=where)


def max_read(
    relation: RelationPath,
    column_ref: SqlColumnExpression,
    *,
    where: SqlCondition | None = None,
) -> exp.Select:
    return _select_from(relation, [exp.Max(this=_column_expression(column_ref))], where=where)


def filtered_delete(relation: RelationPath, *, where: SqlCondition) -> exp.Delete:
    if where is None:
        raise ValueError("Filtered SQL deletes require a WHERE expression.")
    statement = exp.Delete(this=table(relation))
    return _append_delete_where(statement, where)


@dataclass(frozen=True)
class SqlRowWriteInput:
    columns: tuple[ColumnName, ...]
    values: tuple[SqlRenderable, ...]

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("SQL row writes require at least one column.")
        if len(self.columns) != len(self.values):
            raise ValueError("SQL row write columns and values must have the same length.")
        seen: set[str] = set()
        for column_name in self.columns:
            if column_name.value in seen:
                raise ValueError(f"SQL row write column `{column_name.value}` was provided twice.")
            seen.add(column_name.value)


def row_write_input(
    values: Mapping[ColumnName | str, object] | Sequence[tuple[ColumnName | str, object]],
    *,
    params: SqlParamAllocator,
    param_prefix: str | None = None,
) -> SqlRowWriteInput:
    items = tuple(values.items()) if isinstance(values, Mapping) else tuple(values)
    if not items:
        raise ValueError("SQL row writes require at least one value.")
    if param_prefix is not None:
        param_prefix = validate_sql_identifier(param_prefix)
    normalized: list[tuple[ColumnName, object]] = []
    seen: set[str] = set()
    for raw_column, value in items:
        column_name = _column_name(raw_column)
        if column_name.value in seen:
            raise ValueError(f"SQL row write column `{column_name.value}` was provided twice.")
        seen.add(column_name.value)
        normalized.append((column_name, value))
    columns: list[ColumnName] = []
    placeholders: list[SqlRenderable] = []
    for column_name, value in normalized:
        if params.style in {SqlParameterStyle.NAMED, SqlParameterStyle.PYFORMAT}:
            param_name = (
                column_name.value if param_prefix is None else f"{param_prefix}_{column_name.value}"
            )
        else:
            param_name = None
        columns.append(column_name)
        placeholders.append(params.add(value, name=param_name))
    return SqlRowWriteInput(columns=tuple(columns), values=tuple(placeholders))


def row_insert(relation: RelationPath, row: SqlRowWriteInput) -> exp.Insert:
    target = exp.Schema(
        this=table(relation),
        expressions=[identifier(column_name) for column_name in row.columns],
    )
    values = exp.Values(expressions=[exp.Tuple(expressions=list(row.values))])
    return exp.Insert(this=target, expression=values)


@dataclass(frozen=True)
class SqlUpsertAssignment:
    column: ColumnName
    value: SqlRenderable

    def __post_init__(self) -> None:
        if not isinstance(self.column, ColumnName):
            object.__setattr__(self, "column", ColumnName(str(self.column)))
        if not isinstance(self.value, exp.Expression | exp.Condition):
            raise ValueError("SQL upsert assignment values must be SQLGlot expressions.")


@dataclass(frozen=True)
class SqlRuntimeUpsert:
    relation: RelationPath
    row: SqlRowWriteInput
    key_columns: tuple[ColumnName, ...]
    update_columns: tuple[ColumnName, ...] = ()
    update_assignments: tuple[SqlUpsertAssignment, ...] = ()
    target_alias: str = "target"
    source_alias: str = "source"

    def __post_init__(self) -> None:
        if not isinstance(self.relation, RelationPath):
            raise ValueError("SQL runtime upserts require a validated relation path.")
        if not isinstance(self.row, SqlRowWriteInput):
            raise ValueError("SQL runtime upserts require structured row write input.")
        key_columns = tuple(_column_name(column_name) for column_name in self.key_columns)
        if not key_columns:
            raise ValueError("SQL runtime upserts require at least one key column.")
        update_columns = tuple(_column_name(column_name) for column_name in self.update_columns)
        update_assignments = tuple(self.update_assignments)
        row_column_names = {column_name.value for column_name in self.row.columns}
        for column_name in key_columns:
            if column_name.value not in row_column_names:
                raise ValueError(
                    f"SQL runtime upsert key column `{column_name.value}` is not in row values."
                )
        for column_name in update_columns:
            if column_name.value not in row_column_names:
                raise ValueError(
                    f"SQL runtime upsert update column `{column_name.value}` is not in row values."
                )
        seen_keys: set[str] = set()
        for column_name in key_columns:
            if column_name.value in seen_keys:
                raise ValueError(
                    f"SQL runtime upsert key column `{column_name.value}` was provided twice."
                )
            seen_keys.add(column_name.value)
        seen_updates: set[str] = set()
        for column_name in update_columns:
            if column_name.value in seen_keys:
                raise ValueError(
                    f"SQL runtime upsert key column `{column_name.value}` cannot be updated."
                )
            if column_name.value in seen_updates:
                raise ValueError(
                    f"SQL runtime upsert update column `{column_name.value}` was provided twice."
                )
            seen_updates.add(column_name.value)
        for assignment in update_assignments:
            if not isinstance(assignment, SqlUpsertAssignment):
                raise ValueError(
                    "SQL runtime upsert update assignments must be SqlUpsertAssignment values."
                )
            if assignment.column.value in seen_keys:
                raise ValueError(
                    f"SQL runtime upsert key column `{assignment.column.value}` cannot be updated."
                )
            if assignment.column.value in seen_updates:
                raise ValueError(
                    f"SQL runtime upsert update column `{assignment.column.value}` "
                    "was provided twice."
                )
            seen_updates.add(assignment.column.value)
        validate_sql_identifier(self.target_alias)
        validate_sql_identifier(self.source_alias)
        object.__setattr__(self, "key_columns", key_columns)
        object.__setattr__(self, "update_columns", update_columns)
        object.__setattr__(self, "update_assignments", update_assignments)

    def insert_statement(self) -> exp.Insert:
        return row_insert(self.relation, self.row)

    def source_row_select(self) -> exp.Select:
        return select(
            *(
                exp.alias_(value, column_name.value, quoted=True)
                for column_name, value in zip(self.row.columns, self.row.values, strict=True)
            )
        )

    def match_condition(self) -> SqlCondition:
        conditions = [
            exp.EQ(
                this=alias_column(self.target_alias, column_name),
                expression=alias_column(self.source_alias, column_name),
            )
            for column_name in self.key_columns
        ]
        current: SqlCondition = conditions[0]
        for condition in conditions[1:]:
            current = exp.and_(current, condition)
        return current

    def source_insert_values(self) -> tuple[SqlRenderable, ...]:
        return tuple(
            alias_column(self.source_alias, column_name) for column_name in self.row.columns
        )

    def source_update_assignments(
        self,
        *,
        source_alias: str | None = None,
    ) -> tuple[SqlUpsertAssignment, ...]:
        alias = self.source_alias if source_alias is None else validate_sql_identifier(source_alias)
        copied = tuple(
            SqlUpsertAssignment(
                column=column_name,
                value=alias_column(alias, column_name),
            )
            for column_name in self.update_columns
        )
        return copied + self.update_assignments


def upsert_assignment(
    column_name: ColumnName | str,
    value: SqlRenderable,
) -> SqlUpsertAssignment:
    return SqlUpsertAssignment(column=_column_name(column_name), value=value)


def runtime_upsert(
    relation: RelationPath,
    row: SqlRowWriteInput,
    *,
    key_columns: Sequence[ColumnName | str],
    update_columns: Sequence[ColumnName | str] = (),
    update_assignments: Sequence[SqlUpsertAssignment] = (),
    target_alias: str = "target",
    source_alias: str = "source",
) -> SqlRuntimeUpsert:
    return SqlRuntimeUpsert(
        relation=relation,
        row=row,
        key_columns=tuple(_column_name(column_name) for column_name in key_columns),
        update_columns=tuple(_column_name(column_name) for column_name in update_columns),
        update_assignments=tuple(update_assignments),
        target_alias=target_alias,
        source_alias=source_alias,
    )


def parameter(
    position: int,
    style: SqlParameterStyle,
    *,
    name: str | None = None,
) -> exp.Placeholder:
    if position <= 0:
        raise ValueError("SQL parameter position must be greater than 0.")
    if style in {SqlParameterStyle.QMARK, SqlParameterStyle.FORMAT}:
        return exp.Placeholder()
    if style == SqlParameterStyle.NUMERIC:
        return exp.Placeholder(this=str(position))
    if name is None:
        raise ValueError(f"{style.value} SQL parameters require a parameter name.")
    return exp.Placeholder(this=validate_sql_identifier(name))


def _placeholder_text(position: int, style: SqlParameterStyle, *, name: str | None = None) -> str:
    match style:
        case SqlParameterStyle.QMARK:
            return "?"
        case SqlParameterStyle.NUMERIC:
            return f":{position}"
        case SqlParameterStyle.FORMAT:
            return "%s"
        case SqlParameterStyle.NAMED:
            if name is None:
                raise ValueError("named SQL parameters require a parameter name.")
            return f":{validate_sql_identifier(name)}"
        case SqlParameterStyle.PYFORMAT:
            if name is None:
                raise ValueError("pyformat SQL parameters require a parameter name.")
            return f"%({validate_sql_identifier(name)})s"


@dataclass(frozen=True)
class _AllocatedParam:
    position: int
    value: object
    name: str | None
    expression: exp.Placeholder


class SqlParamAllocator:
    def __init__(self, style: SqlParameterStyle = SqlParameterStyle.QMARK) -> None:
        self._style = style
        self._params: list[_AllocatedParam] = []

    @property
    def style(self) -> SqlParameterStyle:
        return self._style

    @property
    def params(self) -> tuple[object, ...]:
        return tuple(param.value for param in self._params)

    @property
    def named_params(self) -> Mapping[str, object]:
        named: dict[str, object] = {}
        for param in self._params:
            if param.name is not None:
                named[param.name] = param.value
        return named

    def add(self, value: object, *, name: str | None = None) -> exp.Placeholder:
        if self._style in {SqlParameterStyle.NAMED, SqlParameterStyle.PYFORMAT} and name is None:
            raise ValueError(f"{self._style.value} SQL parameters require a parameter name.")
        if name is not None:
            name = validate_sql_identifier(name)
            if any(param.name == name for param in self._params):
                raise ValueError(f"SQL parameter name `{name}` was already allocated.")
        position = len(self._params) + 1
        expression = parameter(position, self._style, name=name)
        self._params.append(
            _AllocatedParam(position=position, value=value, name=name, expression=expression)
        )
        return expression


@dataclass(frozen=True)
class CompiledSql:
    sql: str
    params: Sequence[object] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.sql, str) or not self.sql.strip():
            raise ValueError("Compiled SQL requires a non-empty SQL string.")
        object.__setattr__(self, "params", tuple(self.params))


@runtime_checkable
class SqlDialectCapabilities(Protocol):
    name: str
    sqlglot_dialect: str
    parameter_style: SqlParameterStyle

    def quote_identifier(self, identifier: str) -> str: ...

    def placeholder(self, index: int, *, name: str | None = None) -> str: ...


@runtime_checkable
class SqlConnection(Protocol):
    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any: ...


def render_sqlglot(expression: SqlRenderable, *, dialect: SqlDialectCapabilities | str) -> str:
    sqlglot_dialect = dialect if isinstance(dialect, str) else dialect.sqlglot_dialect
    if not isinstance(dialect, str) and getattr(dialect, "uppercase_quoted_columns", False):
        expression = _uppercase_quoted_non_relation_identifiers(expression)
    return expression.sql(dialect=sqlglot_dialect)


def _uppercase_quoted_non_relation_identifiers(expression: SqlRenderable) -> SqlRenderable:
    def transform(node: SqlRenderable) -> SqlRenderable:
        if (
            isinstance(node, exp.Identifier)
            and bool(node.args.get("quoted"))
            and not isinstance(node.parent, exp.Table)
        ):
            return exp.to_identifier(str(node.this).upper(), quoted=True)
        return node

    return expression.copy().transform(transform)


def render_relation_path(
    path: RelationPath,
    *,
    dialect: SqlDialectCapabilities | str,
) -> str:
    return render_sqlglot(table(path), dialect=dialect)


def render_sql(
    expression: SqlRenderable,
    *,
    dialect: SqlDialectCapabilities | str,
    params: SqlParamAllocator | Sequence[object] = (),
) -> CompiledSql:
    if isinstance(params, SqlParamAllocator):
        return CompiledSql(sql=render_sqlglot(expression, dialect=dialect), params=params.params)
    return CompiledSql(sql=render_sqlglot(expression, dialect=dialect), params=tuple(params))


class SimpleSqlDialect:
    def __init__(
        self,
        *,
        name: str,
        sqlglot_dialect: str,
        parameter_style: SqlParameterStyle,
        identifier_quote: str = '"',
    ) -> None:
        self.name = name
        self.sqlglot_dialect = sqlglot_dialect
        self.parameter_style = parameter_style
        self._identifier_quote = identifier_quote

    def quote_identifier(self, identifier: str) -> str:
        value = validate_sql_identifier(identifier)
        escaped = value.replace(self._identifier_quote, self._identifier_quote * 2)
        return f"{self._identifier_quote}{escaped}{self._identifier_quote}"

    def placeholder(self, index: int, *, name: str | None = None) -> str:
        return _placeholder_text(index, self.parameter_style, name=name)

    def runtime_reset_uses_transaction(self) -> bool:
        return True

    def delete_all_rows_sql(self, relation_sql: str) -> str | None:
        _ = relation_sql
        return None

    def relation(self, path: RelationPath) -> exp.Table:
        return table(path)

    def column(self, ref: ColumnRef | ColumnName | str) -> exp.Column:
        return column(ref)

    def render(self, expression: SqlRenderable) -> str:
        return render_sqlglot(expression, dialect=self)
