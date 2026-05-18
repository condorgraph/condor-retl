from __future__ import annotations

import textwrap
from collections.abc import Sequence
from dataclasses import dataclass
from typing import NoReturn, cast

import pyarrow as pa
import pyarrow.compute as pc
from sqlglot import exp, parse_one, select
from sqlglot.errors import SqlglotError

from retl.errors import RetlError
from retl.sql import (
    CompiledSql,
    SqlCondition,
    SqlParameterStyle,
    SqlRenderable,
    parameter,
    render_sqlglot,
)
from retl.stores.contracts import EventKeysetScanPosition

SOURCE_ARROW_FETCH_BATCH_SIZE = 65_536


class SourceSqlError(RetlError, ValueError):
    """Raised when a SQL source contract cannot be satisfied."""


@dataclass(frozen=True)
class SqlDialect:
    name: str
    identifier_quote: str = '"'
    parameter_style: SqlParameterStyle = SqlParameterStyle.QMARK

    @property
    def sqlglot_dialect(self) -> str:
        return self.name

    def quote_identifier(self, identifier: str) -> str:
        if not isinstance(identifier, str) or not identifier:
            raise SourceSqlError("SQL identifiers must be non-empty strings.")
        escaped = identifier.replace(self.identifier_quote, self.identifier_quote * 2)
        return f"{self.identifier_quote}{escaped}{self.identifier_quote}"

    def placeholder(self, _index: int) -> str:
        if self.parameter_style == SqlParameterStyle.QMARK:
            return "?"
        if self.parameter_style == SqlParameterStyle.NUMERIC:
            return f":{_index}"
        if self.parameter_style == SqlParameterStyle.FORMAT:
            return "%s"
        raise SourceSqlError(f"SQL dialect `{self.name}` has unsupported parameter style.")


DUCKDB_DIALECT = SqlDialect(name="duckdb")


def normalize_query(query: str) -> str:
    normalized = textwrap.dedent(query).strip()
    while normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
    if not normalized:
        raise SourceSqlError("Source SQL requires a non-empty `query`.")
    return normalized


def wrap_subquery(query: str, *, alias: str = "retl_source") -> str:
    return _wrap_source_subquery(query, alias=alias, dialect=DUCKDB_DIALECT)


def _wrap_source_subquery(query: str, *, alias: str, dialect: SqlDialect) -> str:
    normalized = normalize_query(query)
    if not alias:
        raise SourceSqlError("SQL subquery alias must be non-empty.")
    # SQLGlot models the wrapper when it can, while the rendered SQL preserves
    # the authored query text as the source boundary.
    _ = _build_source_wrapper_expression(normalized, alias=alias, dialect=dialect)
    return f"(\n{normalized}\n) AS {alias}"


def compile_snapshot_query(
    query: str,
    *,
    dialect: SqlDialect = DUCKDB_DIALECT,
    alias: str = "retl_source",
) -> CompiledSql:
    wrapped_source = _wrap_source_subquery(
        query,
        alias=dialect.quote_identifier(alias),
        dialect=dialect,
    )
    return CompiledSql(
        sql=(f"{_render_select_star(dialect)}\nFROM {wrapped_source}"),
    )


def compile_keyset_scan_query(
    query: str,
    *,
    cursor_column: str,
    primary_key_column: str,
    scan_after: EventKeysetScanPosition | None,
    scan_through: EventKeysetScanPosition | None = None,
    dialect: SqlDialect = DUCKDB_DIALECT,
    alias: str = "retl_source",
    limit: int | None = None,
) -> CompiledSql:
    if limit is not None and limit <= 0:
        raise SourceSqlError("Event keyset SQL `limit` must be positive.")

    predicate = _render_keyset_predicate(
        cursor_column=cursor_column,
        primary_key_column=primary_key_column,
        scan_after=scan_after,
        scan_through=scan_through,
        dialect=dialect,
    )
    params: tuple[object, ...] = ()
    if scan_after is not None:
        cursor_value = scan_after.cursor_value.value
        params = (cursor_value, cursor_value, scan_after.primary_key_value.value)
    if scan_through is not None:
        cursor_value = scan_through.cursor_value.value
        params = (*params, cursor_value, cursor_value, scan_through.primary_key_value.value)
    wrapped_source = _wrap_source_subquery(
        query,
        alias=dialect.quote_identifier(alias),
        dialect=dialect,
    )

    sql = (
        f"{_render_select_star(dialect)}\n"
        f"FROM {wrapped_source}\n"
        f"WHERE {predicate}\n"
        f"{_render_keyset_order_by(cursor_column, primary_key_column, dialect=dialect)}"
    )
    if limit is not None:
        sql = f"{sql}\nLIMIT {_render_placeholder(len(params) + 1, dialect=dialect)}"
        params = (*params, limit)
    return CompiledSql(sql=sql, params=params)


def _build_source_wrapper_expression(
    normalized_query: str,
    *,
    alias: str,
    dialect: SqlDialect,
) -> exp.Select | None:
    try:
        source_expression = parse_one(normalized_query, read=dialect.sqlglot_dialect)
        alias_expression = _sqlglot_alias(alias, dialect=dialect)
        subquery = exp.Subquery(
            this=source_expression,
            alias=exp.TableAlias(this=alias_expression),
        )
        return select("*").from_(subquery)
    except (SqlglotError, ValueError):
        return None


def _sqlglot_alias(alias: str, *, dialect: SqlDialect) -> exp.Identifier:
    quote = dialect.identifier_quote
    if len(alias) >= 2 and alias.startswith(quote) and alias.endswith(quote):
        unquoted = alias[1:-1].replace(quote * 2, quote)
        return exp.to_identifier(unquoted, quoted=True)
    return exp.to_identifier(alias, quoted=False)


def _source_parameter_style(dialect: SqlDialect) -> SqlParameterStyle:
    if dialect.parameter_style in {
        SqlParameterStyle.QMARK,
        SqlParameterStyle.NUMERIC,
        SqlParameterStyle.FORMAT,
    }:
        return dialect.parameter_style
    raise SourceSqlError(f"SQL dialect `{dialect.name}` has unsupported parameter style.")


def _render_sqlglot_fragment(
    expression: SqlRenderable,
    *,
    dialect: SqlDialect,
    fallback: str,
) -> str:
    try:
        return render_sqlglot(expression, dialect=dialect.sqlglot_dialect)
    except (SqlglotError, ValueError):
        return fallback


def _source_column(column_name: str) -> exp.Column:
    if not isinstance(column_name, str) or not column_name:
        raise SourceSqlError("SQL identifiers must be non-empty strings.")
    return exp.column(column_name, quoted=True)


def _render_select_star(dialect: SqlDialect) -> str:
    return _render_sqlglot_fragment(select("*"), dialect=dialect, fallback="SELECT *")


def _render_placeholder(index: int, *, dialect: SqlDialect) -> str:
    style = _source_parameter_style(dialect)
    return _render_sqlglot_fragment(
        parameter(index, style),
        dialect=dialect,
        fallback=dialect.placeholder(index),
    )


def _render_keyset_predicate(
    *,
    cursor_column: str,
    primary_key_column: str,
    scan_after: EventKeysetScanPosition | None,
    scan_through: EventKeysetScanPosition | None,
    dialect: SqlDialect,
) -> str:
    cursor = dialect.quote_identifier(cursor_column)
    primary_key = dialect.quote_identifier(primary_key_column)
    predicate: SqlCondition = exp.and_(
        exp.Is(
            this=_source_column(cursor_column),
            expression=exp.Not(this=exp.Null()),
        ),
        exp.Is(
            this=_source_column(primary_key_column),
            expression=exp.Not(this=exp.Null()),
        ),
    )
    fallback = f"{cursor} IS NOT NULL AND {primary_key} IS NOT NULL"
    if scan_after is not None:
        placeholders = tuple(dialect.placeholder(index) for index in range(1, 4))
        predicate = exp.and_(
            predicate,
            exp.or_(
                exp.GT(
                    this=_source_column(cursor_column),
                    expression=parameter(1, _source_parameter_style(dialect)),
                ),
                exp.and_(
                    exp.EQ(
                        this=_source_column(cursor_column),
                        expression=parameter(2, _source_parameter_style(dialect)),
                    ),
                    exp.GT(
                        this=_source_column(primary_key_column),
                        expression=parameter(3, _source_parameter_style(dialect)),
                    ),
                ),
            ),
        )
        fallback = (
            f"{fallback} AND "
            f"({cursor} > {placeholders[0]} OR "
            f"({cursor} = {placeholders[1]} AND {primary_key} > {placeholders[2]}))"
        )
    if scan_through is not None:
        start = 1 if scan_after is None else 4
        placeholders = tuple(dialect.placeholder(index) for index in range(start, start + 3))
        upper = exp.or_(
            exp.LT(
                this=_source_column(cursor_column),
                expression=parameter(start, _source_parameter_style(dialect)),
            ),
            exp.and_(
                exp.EQ(
                    this=_source_column(cursor_column),
                    expression=parameter(start + 1, _source_parameter_style(dialect)),
                ),
                exp.LTE(
                    this=_source_column(primary_key_column),
                    expression=parameter(start + 2, _source_parameter_style(dialect)),
                ),
            ),
        )
        predicate = exp.and_(predicate, upper)
        fallback = (
            f"{fallback} AND "
            f"({cursor} < {placeholders[0]} OR "
            f"({cursor} = {placeholders[1]} AND {primary_key} <= {placeholders[2]}))"
        )
    return _render_sqlglot_fragment(predicate, dialect=dialect, fallback=fallback)


def _render_keyset_order_by(
    cursor_column: str,
    primary_key_column: str,
    *,
    dialect: SqlDialect,
) -> str:
    cursor = dialect.quote_identifier(cursor_column)
    primary_key = dialect.quote_identifier(primary_key_column)
    order_by = exp.Order(
        expressions=[
            exp.Ordered(this=_source_column(cursor_column), desc=False),
            exp.Ordered(this=_source_column(primary_key_column), desc=False),
        ],
    )
    return _render_sqlglot_fragment(
        order_by,
        dialect=dialect,
        fallback=f"ORDER BY {cursor} ASC, {primary_key} ASC",
    )


def validate_arrow_schema(schema: pa.Schema, *, backend_name: str = "Source") -> None:
    if len(schema.names) != len(set(schema.names)):
        raise SourceSqlError(f"{backend_name} query results must not contain duplicate columns.")


_SOURCE_ARROW_SHAPE_ERROR_HINT = (
    "Source SQL output must contain only scalar columns or list-of-scalar columns. "
    "Flatten columns or serialize nested objects as JSON text in upstream SQL."
)


def _raise_unsupported_source_arrow_type(
    *,
    column_name: str,
    data_type: pa.DataType,
) -> NoReturn:
    raise SourceSqlError(
        f"Source Arrow column `{column_name}` has unsupported Arrow type `{data_type}`. "
        f"{_SOURCE_ARROW_SHAPE_ERROR_HINT}"
    )


def _is_source_arrow_list_type(data_type: pa.DataType) -> bool:
    return (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
        or pa.types.is_list_view(data_type)
        or pa.types.is_large_list_view(data_type)
    )


def _source_arrow_list_value_field(data_type: pa.DataType) -> pa.Field:
    if pa.types.is_list(data_type):
        return cast(pa.ListType, data_type).value_field
    if pa.types.is_large_list(data_type):
        return cast(pa.LargeListType, data_type).value_field
    if pa.types.is_fixed_size_list(data_type):
        return cast(pa.FixedSizeListType, data_type).value_field
    if pa.types.is_list_view(data_type):
        return cast(pa.ListViewType, data_type).value_field
    if pa.types.is_large_list_view(data_type):
        return cast(pa.LargeListViewType, data_type).value_field
    raise AssertionError("expected an Arrow list type")


def _normalize_source_arrow_field(field: pa.Field) -> pa.Field:
    return pa.field(
        field.name,
        _normalize_source_arrow_type(field.type, column_name=field.name),
        nullable=field.nullable,
        metadata=field.metadata,
    )


def _normalize_source_arrow_scalar_type(
    data_type: pa.DataType,
    *,
    column_name: str,
) -> pa.DataType:
    if (
        pa.types.is_null(data_type)
        or pa.types.is_boolean(data_type)
        or pa.types.is_integer(data_type)
        or pa.types.is_floating(data_type)
        or pa.types.is_string(data_type)
        or pa.types.is_large_string(data_type)
        or pa.types.is_string_view(data_type)
    ):
        return data_type
    if (
        pa.types.is_date(data_type)
        or pa.types.is_timestamp(data_type)
        or pa.types.is_time(data_type)
        or pa.types.is_duration(data_type)
        or pa.types.is_decimal(data_type)
    ):
        return pa.string()
    if (
        pa.types.is_binary(data_type)
        or pa.types.is_large_binary(data_type)
        or pa.types.is_fixed_size_binary(data_type)
        or pa.types.is_binary_view(data_type)
    ):
        _raise_unsupported_source_arrow_type(column_name=column_name, data_type=data_type)
    if pa.types.is_dictionary(data_type):
        dictionary_type = cast(pa.DictionaryType, data_type)
        return pa.dictionary(
            dictionary_type.index_type,
            _normalize_source_arrow_scalar_type(
                dictionary_type.value_type,
                column_name=column_name,
            ),
            ordered=dictionary_type.ordered,
        )
    _raise_unsupported_source_arrow_type(column_name=column_name, data_type=data_type)


def _normalize_source_arrow_type(
    data_type: pa.DataType,
    *,
    column_name: str,
) -> pa.DataType:
    if pa.types.is_map(data_type) or pa.types.is_union(data_type) or pa.types.is_struct(data_type):
        _raise_unsupported_source_arrow_type(column_name=column_name, data_type=data_type)
    if pa.types.is_list(data_type):
        list_type = cast(pa.ListType, data_type)
        value_field = list_type.value_field
        return pa.list_(
            pa.field(
                value_field.name,
                _normalize_source_arrow_scalar_type(value_field.type, column_name=column_name),
                nullable=value_field.nullable,
                metadata=value_field.metadata,
            )
        )
    if pa.types.is_large_list(data_type):
        large_list_type = cast(pa.LargeListType, data_type)
        value_field = large_list_type.value_field
        return pa.large_list(
            pa.field(
                value_field.name,
                _normalize_source_arrow_scalar_type(value_field.type, column_name=column_name),
                nullable=value_field.nullable,
                metadata=value_field.metadata,
            )
        )
    if pa.types.is_fixed_size_list(data_type):
        fixed_size_list_type = cast(pa.FixedSizeListType, data_type)
        value_field = fixed_size_list_type.value_field
        return pa.list_(
            pa.field(
                value_field.name,
                _normalize_source_arrow_scalar_type(value_field.type, column_name=column_name),
                nullable=value_field.nullable,
                metadata=value_field.metadata,
            ),
            fixed_size_list_type.list_size,
        )
    if pa.types.is_list_view(data_type):
        list_view_type = cast(pa.ListViewType, data_type)
        value_field = list_view_type.value_field
        return pa.list_view(
            pa.field(
                value_field.name,
                _normalize_source_arrow_scalar_type(value_field.type, column_name=column_name),
                nullable=value_field.nullable,
                metadata=value_field.metadata,
            )
        )
    if pa.types.is_large_list_view(data_type):
        large_list_view_type = cast(pa.LargeListViewType, data_type)
        value_field = large_list_view_type.value_field
        return pa.large_list_view(
            pa.field(
                value_field.name,
                _normalize_source_arrow_scalar_type(value_field.type, column_name=column_name),
                nullable=value_field.nullable,
                metadata=value_field.metadata,
            )
        )
    return _normalize_source_arrow_scalar_type(data_type, column_name=column_name)


def _reject_non_finite_source_floats(
    array: pa.Array,
    *,
    column_name: str,
    data_type: pa.DataType,
) -> None:
    if pa.types.is_dictionary(data_type):
        dictionary_type = cast(pa.DictionaryType, data_type)
        decoded_array = cast(pa.Array, pc.cast(array, dictionary_type.value_type))
        _reject_non_finite_source_floats(
            decoded_array,
            column_name=column_name,
            data_type=dictionary_type.value_type,
        )
        return
    if pa.types.is_floating(data_type):
        finite_mask = pc.and_(pc.is_valid(array), pc.invert(pc.is_finite(array)))
        if pc.any(finite_mask).as_py():
            raise SourceSqlError(
                f"Source Arrow column `{column_name}` contains non-finite float values. "
                f"{_SOURCE_ARROW_SHAPE_ERROR_HINT}"
            )
        return
    if _is_source_arrow_list_type(data_type):
        value_field = _source_arrow_list_value_field(data_type)
        flattened_array = cast(pa.Array, pc.list_flatten(array))
        _reject_non_finite_source_floats(
            flattened_array,
            column_name=column_name,
            data_type=value_field.type,
        )


def _normalize_source_arrow_array(
    array: pa.Array,
    *,
    field: pa.Field,
    normalized_type: pa.DataType,
) -> pa.Array:
    _reject_non_finite_source_floats(array, column_name=field.name, data_type=field.type)
    if pa.types.is_timestamp(field.type) and pa.types.is_string(normalized_type):
        timestamp_type = cast(pa.TimestampType, field.type)
        timestamp_array = array
        if timestamp_type.tz is not None:
            timestamp_array = cast(
                pa.Array,
                pc.cast(array, pa.timestamp(timestamp_type.unit, tz="UTC")),
            )
        normalized_timestamp = cast(pa.Array, pc.cast(timestamp_array, pa.string()))
        normalized_timestamp = cast(
            pa.Array,
            pc.replace_substring(
                normalized_timestamp,
                pattern=" ",
                replacement="T",
                max_replacements=1,
            ),
        )
        if timestamp_type.tz is None:
            normalized_timestamp = cast(
                pa.Array,
                pc.binary_join_element_wise(
                    normalized_timestamp,
                    pa.scalar("Z", type=pa.string()),
                    pa.scalar("", type=pa.string()),
                ),
            )
        return cast(
            pa.Array,
            pc.replace_substring_regex(
                normalized_timestamp,
                pattern=r"\.0+(Z|[+-][0-9]{4})$",
                replacement=r"\1",
            ),
        )
    if array.type != normalized_type:
        try:
            array = cast(pa.Array, pc.cast(array, normalized_type))
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
            raise SourceSqlError(
                f"Source Arrow column `{field.name}` cannot be normalized to "
                "JSON-compatible collected values."
            ) from exc
    if pa.types.is_time(field.type) and pa.types.is_string(normalized_type):
        array = cast(
            pa.Array,
            pc.replace_substring_regex(array, pattern=r"\.0+$", replacement=""),
        )
    return array


def normalize_source_record_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Convert backend-native Arrow scalars into collected-row-safe values."""

    validate_arrow_schema(batch.schema)
    normalized_fields = [_normalize_source_arrow_field(field) for field in batch.schema]
    normalized_arrays = [
        _normalize_source_arrow_array(
            batch.column(index),
            field=field,
            normalized_type=normalized_field.type,
        )
        for index, (field, normalized_field) in enumerate(
            zip(batch.schema, normalized_fields, strict=True)
        )
    ]
    normalized_schema = pa.schema(normalized_fields, metadata=batch.schema.metadata)
    return pa.RecordBatch.from_arrays(normalized_arrays, schema=normalized_schema)


def table_from_batches(
    batches: Sequence[pa.RecordBatch],
    *,
    schema: pa.Schema | None = None,
) -> pa.Table:
    if not batches:
        return pa.Table.from_batches([], schema=schema or pa.schema([]))
    expected_schema = schema or batches[0].schema
    for batch in batches:
        if not batch.schema.equals(expected_schema, check_metadata=True):
            raise SourceSqlError("Source Arrow batches must share one schema.")
    return pa.Table.from_batches(batches, schema=expected_schema)


__all__ = [
    "DUCKDB_DIALECT",
    "CompiledSql",
    "SOURCE_ARROW_FETCH_BATCH_SIZE",
    "SourceSqlError",
    "SqlDialect",
    "compile_keyset_scan_query",
    "compile_snapshot_query",
    "normalize_query",
    "normalize_source_record_batch",
    "table_from_batches",
    "validate_arrow_schema",
    "wrap_subquery",
]
