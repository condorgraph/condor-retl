from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
from sqlglot import exp, select

from retl.backends.duckdb import DUCKDB_DIALECT
from retl.sql import (
    ColumnName,
    ColumnRef,
    CompiledSql,
    RelationName,
    RelationPath,
    SimpleSqlDialect,
    SqlConnection,
    SqlDialectCapabilities,
    SqlParamAllocator,
    SqlParameterStyle,
    alias_column,
    column,
    count_read,
    filtered_delete,
    identifier,
    list_read,
    max_read,
    min_read,
    parameter,
    render_relation_path,
    render_sql,
    row_insert,
    row_read,
    row_write_input,
    runtime_upsert,
    scalar_read,
    sql_and,
    sql_eq_param,
    sql_order,
    table,
    upsert_assignment,
    validate_sql_identifier,
)


def test_parameter_allocator_returns_sqlglot_placeholders_and_retains_values() -> None:
    allocator = SqlParamAllocator()
    first = allocator.add("cust_1")
    second = allocator.add(50)

    expression = (
        select(column("customer_id"))
        .from_(table(RelationPath("customers", schema="source")))
        .where(exp.EQ(this=column("customer_id"), expression=first))
        .limit(second)
    )

    compiled = render_sql(expression, dialect=DUCKDB_DIALECT, params=allocator)

    assert isinstance(first, exp.Placeholder)
    assert isinstance(second, exp.Placeholder)
    assert compiled.sql == (
        'SELECT "customer_id" FROM "source"."customers" WHERE "customer_id" = ? LIMIT ?'
    )
    assert compiled.params == ("cust_1", 50)


@pytest.mark.parametrize("style", [SqlParameterStyle.NAMED, SqlParameterStyle.PYFORMAT])
def test_named_parameter_styles_require_name_at_allocation_time(style: SqlParameterStyle) -> None:
    allocator = SqlParamAllocator(style)

    with pytest.raises(ValueError, match="require a parameter name"):
        allocator.add("Ada")


def test_parameter_allocator_rejects_duplicate_names() -> None:
    allocator = SqlParamAllocator(SqlParameterStyle.NAMED)
    allocator.add("Ada", name="customer_name")

    with pytest.raises(ValueError, match="already allocated"):
        allocator.add("Grace", name="customer_name")


@pytest.mark.parametrize(
    ("style", "expected"),
    [
        (SqlParameterStyle.QMARK, "?"),
        (SqlParameterStyle.NUMERIC, ":2"),
        (SqlParameterStyle.FORMAT, "%s"),
        (SqlParameterStyle.NAMED, ":limit"),
        (SqlParameterStyle.PYFORMAT, "%(limit)s"),
    ],
)
def test_placeholder_rendering_policy(style: SqlParameterStyle, expected: str) -> None:
    dialect = SimpleSqlDialect(
        name="test",
        sqlglot_dialect="postgres",
        parameter_style=style,
    )

    assert dialect.placeholder(2, name="limit") == expected


def test_placeholder_policy_uses_sqlglot_placeholder_expressions() -> None:
    assert parameter(1, SqlParameterStyle.QMARK).sql(dialect="duckdb") == "?"
    assert parameter(1, SqlParameterStyle.FORMAT).sql(dialect="postgres") == "%s"
    assert parameter(3, SqlParameterStyle.NUMERIC).sql() == ":3"
    assert parameter(1, SqlParameterStyle.PYFORMAT, name="limit").sql(dialect="postgres") == (
        "%(limit)s"
    )


@pytest.mark.parametrize(
    "invalid",
    ["", " ", "1table", "source.table", '"table"', "table-name"],
)
def test_validated_names_reject_non_simple_identifier_shapes(invalid: str) -> None:
    with pytest.raises(ValueError, match="simple SQL identifier|non-empty"):
        validate_sql_identifier(invalid)


def test_validated_relation_column_and_path_objects() -> None:
    path = RelationPath(
        database=RelationName("warehouse"),
        schema=RelationName("runtime"),
        name=RelationName("ordered_work"),
    )
    column_name = ColumnName("canonical_key")

    assert path.parts == ("warehouse", "runtime", "ordered_work")
    assert column_name.value == "canonical_key"

    with pytest.raises(ValueError, match="database requires relation schema"):
        RelationPath(database="warehouse", name="ordered_work")


def test_sqlglot_identifier_column_and_table_helpers_quote_validated_names() -> None:
    relation = RelationPath(schema="runtime", name="state_current")

    assert identifier("state_current").sql(dialect="duckdb") == '"state_current"'
    assert table(relation).sql(dialect="duckdb") == '"runtime"."state_current"'
    assert render_relation_path(relation, dialect=DUCKDB_DIALECT) == ('"runtime"."state_current"')
    assert column("identity").sql(dialect="duckdb") == '"identity"'
    assert (
        column(ColumnRef("identity", relation=relation)).sql(dialect="duckdb")
        == '"runtime"."state_current"."identity"'
    )


def test_alias_column_quotes_column_but_not_compiler_owned_alias() -> None:
    assert alias_column("source_rows", "customer_id").sql(dialect="snowflake") == (
        'source_rows."customer_id"'
    )


def test_duckdb_rendering_combines_sqlglot_sql_and_retl_owned_params() -> None:
    allocator = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)
    limit = allocator.add(25)
    expression = select("*").from_(table(RelationPath("ordered_work", schema="retl"))).limit(limit)

    compiled = render_sql(expression, dialect=DUCKDB_DIALECT, params=allocator)

    assert compiled == CompiledSql(
        sql='SELECT * FROM "retl"."ordered_work" LIMIT ?',
        params=(25,),
    )


def test_shared_read_helpers_render_parameterized_scalar_row_and_list_shapes() -> None:
    relation = RelationPath("ordered_work", schema="retl")
    params = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)
    where = sql_and(
        sql_eq_param("declaration_name", "customers", params=params),
        sql_eq_param("status", "pending", params=params),
    )

    scalar = render_sql(
        scalar_read(
            relation,
            "work_id",
            where=where,
            order_by=[sql_order("sequence_order")],
            limit=params.add(1),
        ),
        dialect=DUCKDB_DIALECT,
        params=params,
    )

    assert scalar.sql == (
        'SELECT "work_id" FROM "retl"."ordered_work" '
        'WHERE "declaration_name" = ? AND "status" = ? '
        'ORDER BY "sequence_order" ASC LIMIT ?'
    )
    assert scalar.params == ("customers", "pending", 1)

    row_params = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)
    row_where = sql_and(
        sql_eq_param("declaration_name", "customers", params=row_params),
        sql_eq_param("status", "pending", params=row_params),
    )
    row = render_sql(
        row_read(relation, ["work_id", "sequence_order"], where=row_where),
        dialect=DUCKDB_DIALECT,
        params=row_params,
    )
    list_params = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)
    list_where = sql_and(
        sql_eq_param("declaration_name", "customers", params=list_params),
        sql_eq_param("status", "pending", params=list_params),
    )
    listed = render_sql(
        list_read(relation, "work_id", where=list_where),
        dialect=DUCKDB_DIALECT,
        params=list_params,
    )

    assert row.sql == (
        'SELECT "work_id", "sequence_order" FROM "retl"."ordered_work" '
        'WHERE "declaration_name" = ? AND "status" = ?'
    )
    assert row.params == ("customers", "pending")
    assert listed.sql == (
        'SELECT "work_id" FROM "retl"."ordered_work" WHERE "declaration_name" = ? AND "status" = ?'
    )
    assert listed.params == ("customers", "pending")


def test_shared_aggregate_and_delete_helpers_render_common_runtime_shapes() -> None:
    relation = RelationPath("ordered_work", schema="retl")
    params = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)
    where = sql_eq_param("declaration_name", "customers", params=params)

    count = render_sql(count_read(relation, where=where), dialect=DUCKDB_DIALECT, params=params)
    minimum = render_sql(min_read(relation, "collect_id"), dialect=DUCKDB_DIALECT)
    maximum = render_sql(
        max_read(relation, "sequence_order", where=where),
        dialect=DUCKDB_DIALECT,
        params=params,
    )
    delete = render_sql(
        filtered_delete(relation, where=where),
        dialect=DUCKDB_DIALECT,
        params=params,
    )

    assert count.sql == ('SELECT COUNT(*) FROM "retl"."ordered_work" WHERE "declaration_name" = ?')
    assert count.params == ("customers",)
    assert minimum.sql == 'SELECT MIN("collect_id") FROM "retl"."ordered_work"'
    assert maximum.sql == (
        'SELECT MAX("sequence_order") FROM "retl"."ordered_work" WHERE "declaration_name" = ?'
    )
    assert delete.sql == 'DELETE FROM "retl"."ordered_work" WHERE "declaration_name" = ?'


def test_filtered_delete_requires_where_expression() -> None:
    with pytest.raises(ValueError, match="require a WHERE expression"):
        filtered_delete(RelationPath("ordered_work", schema="retl"), where=None)  # type: ignore[arg-type]


def test_row_insert_helper_uses_validated_columns_and_driver_bound_params() -> None:
    params = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)
    row = row_write_input(
        {
            "declaration_name": "customers",
            "position_json": '{"id": 7}',
        },
        params=params,
    )

    compiled = render_sql(
        row_insert(RelationPath("destination_progress", schema="retl"), row),
        dialect=DUCKDB_DIALECT,
        params=params,
    )

    assert compiled.sql == (
        'INSERT INTO "retl"."destination_progress" '
        '("declaration_name", "position_json") VALUES (?, ?)'
    )
    assert compiled.params == ("customers", '{"id": 7}')


def test_row_insert_helper_supports_numeric_parameter_dialects() -> None:
    from retl.backends.snowflake import SNOWFLAKE_DIALECT

    params = SqlParamAllocator(SNOWFLAKE_DIALECT.parameter_style)
    row = row_write_input(
        [("run_id", "run_1"), ("status", "running")],
        params=params,
    )

    compiled = render_sql(
        row_insert(RelationPath("runs", schema="retl", database="warehouse"), row),
        dialect=SNOWFLAKE_DIALECT,
        params=params,
    )

    assert compiled.sql == (
        'INSERT INTO "warehouse"."retl"."runs" ("RUN_ID", "STATUS") VALUES (:1, :2)'
    )
    assert compiled.params == ("run_1", "running")


def test_row_write_input_rejects_invalid_or_duplicate_columns() -> None:
    params = SqlParamAllocator()

    with pytest.raises(ValueError, match="simple SQL identifier"):
        row_write_input({"bad-column": "value"}, params=params)

    with pytest.raises(ValueError, match="provided twice"):
        row_write_input([("run_id", "run_1"), (ColumnName("run_id"), "run_2")], params=params)


def test_row_write_input_duplicate_columns_do_not_allocate_params() -> None:
    params = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)

    with pytest.raises(ValueError, match="provided twice"):
        row_write_input([("run_id", "run_1"), ("run_id", "run_2")], params=params)

    assert params.params == ()


def test_runtime_upsert_contract_exposes_sqlglot_insert_source_and_match_pieces() -> None:
    from retl.backends.snowflake import SNOWFLAKE_DIALECT

    params = SqlParamAllocator(SNOWFLAKE_DIALECT.parameter_style)
    row = row_write_input(
        [
            ("declaration_name", "customers"),
            ("position_json", '{"id": 7}'),
            ("active", True),
        ],
        params=params,
    )
    upsert = runtime_upsert(
        RelationPath("destination_progress", schema="RETL_RUNTIME", database="RETL_DB"),
        row,
        key_columns=["declaration_name"],
        update_columns=["position_json"],
        update_assignments=[
            upsert_assignment("active", exp.Boolean(this=True)),
            upsert_assignment("last_seen_at", exp.Var(this="current_timestamp")),
        ],
    )

    assert render_sql(upsert.insert_statement(), dialect=SNOWFLAKE_DIALECT, params=params) == (
        CompiledSql(
            sql=(
                'INSERT INTO "RETL_DB"."RETL_RUNTIME"."destination_progress" '
                '("DECLARATION_NAME", "POSITION_JSON", "ACTIVE") VALUES (:1, :2, :3)'
            ),
            params=("customers", '{"id": 7}', True),
        )
    )
    assert upsert.source_row_select().sql(dialect="snowflake") == (
        'SELECT :1 AS "declaration_name", :2 AS "position_json", :3 AS "active"'
    )
    assert upsert.match_condition().sql(dialect="snowflake") == (
        'target."declaration_name" = source."declaration_name"'
    )
    assert [assignment.column.value for assignment in upsert.source_update_assignments()] == [
        "position_json",
        "active",
        "last_seen_at",
    ]
    assert params.params == ("customers", '{"id": 7}', True)


def test_runtime_upsert_contract_validates_keys_updates_aliases_and_assignments() -> None:
    params = SqlParamAllocator(DUCKDB_DIALECT.parameter_style)
    row = row_write_input([("run_id", "run_1"), ("status", "running")], params=params)
    relation = RelationPath("runs", schema="retl")

    with pytest.raises(ValueError, match="at least one key column"):
        runtime_upsert(relation, row, key_columns=[])

    with pytest.raises(ValueError, match="key column `missing` is not in row values"):
        runtime_upsert(relation, row, key_columns=["missing"])

    with pytest.raises(ValueError, match="update column `missing` is not in row values"):
        runtime_upsert(relation, row, key_columns=["run_id"], update_columns=["missing"])

    with pytest.raises(ValueError, match="key column `run_id` was provided twice"):
        runtime_upsert(relation, row, key_columns=["run_id", "run_id"])

    with pytest.raises(ValueError, match="key column `run_id` cannot be updated"):
        runtime_upsert(relation, row, key_columns=["run_id"], update_columns=["run_id"])

    with pytest.raises(ValueError, match="key column `run_id` cannot be updated"):
        runtime_upsert(
            relation,
            row,
            key_columns=["run_id"],
            update_assignments=[upsert_assignment("run_id", exp.Boolean(this=True))],
        )

    with pytest.raises(ValueError, match="update column `status` was provided twice"):
        runtime_upsert(
            relation,
            row,
            key_columns=["run_id"],
            update_columns=["status"],
            update_assignments=[upsert_assignment("status", exp.Boolean(this=True))],
        )

    with pytest.raises(ValueError, match="simple SQL identifier"):
        runtime_upsert(relation, row, key_columns=["run_id"], target_alias="bad-alias")

    with pytest.raises(ValueError, match="SQLGlot expressions"):
        upsert_assignment("status", "running")  # type: ignore[arg-type]


def test_non_duckdb_render_smoke_test_does_not_require_live_backend() -> None:
    postgres = SimpleSqlDialect(
        name="postgres",
        sqlglot_dialect="postgres",
        parameter_style=SqlParameterStyle.FORMAT,
    )
    allocator = SqlParamAllocator(postgres.parameter_style)
    name_param = allocator.add("Ada")
    expression = (
        select(column("customer_id"))
        .from_(table(RelationPath("customers", schema="public")))
        .where(exp.EQ(this=column("first_name"), expression=name_param))
    )

    compiled = render_sql(expression, dialect=postgres, params=allocator)

    assert compiled.sql == (
        'SELECT "customer_id" FROM "public"."customers" WHERE "first_name" = %s'
    )
    assert compiled.params == ("Ada",)


def test_compiled_sql_rejects_blank_sql() -> None:
    with pytest.raises(ValueError, match="non-empty SQL string"):
        CompiledSql("   ")


def test_compiled_sql_normalizes_parameter_sequences_to_tuples() -> None:
    compiled = CompiledSql("select ? as value", [1])

    assert compiled.params == (1,)


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        self.calls.append((sql, tuple(parameters)))
        return self


def test_protocol_friendly_minimal_implementations() -> None:
    connection = RecordingConnection()

    assert isinstance(DUCKDB_DIALECT, SqlDialectCapabilities)
    assert DUCKDB_DIALECT.placeholder(1) == "?"
    assert isinstance(connection, SqlConnection)

    compiled = CompiledSql("select ? as value", [1])
    connection.execute(compiled.sql, compiled.params)

    assert connection.calls == [("select ? as value", (1,))]
