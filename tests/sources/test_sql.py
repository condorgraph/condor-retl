from __future__ import annotations

import math
from decimal import Decimal

import pyarrow as pa
import pytest
from sqlglot import exp, parse_one

from retl.sources.sql import (
    SourceSqlError,
    SqlDialect,
    compile_keyset_scan_query,
    compile_snapshot_query,
    normalize_query,
    normalize_source_record_batch,
    validate_arrow_schema,
)
from retl.sql import SqlParameterStyle
from retl.stores.contracts import CanonicalKeyScalar, EventKeysetScanPosition


def test_normalize_query_dedents_strips_and_removes_trailing_semicolons() -> None:
    assert (
        normalize_query(
            """
            select *
            from customers;;
            """
        )
        == "select *\nfrom customers"
    )


def test_compile_keyset_scan_quotes_identifiers_and_binds_values() -> None:
    scan_after = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-05-03T10:30:00Z'; drop table events; --"),
        primary_key_value=CanonicalKeyScalar.string("evt-9'; drop table events; --"),
    )

    compiled = compile_keyset_scan_query(
        "select * from raw_events",
        cursor_column='occurred"at',
        primary_key_column="event_id",
        scan_after=scan_after,
    )

    assert '"occurred""at" > ?' in compiled.sql
    assert '"occurred""at" = ?' in compiled.sql
    assert '"event_id" > ?' in compiled.sql
    assert "drop table" not in compiled.sql.lower()
    assert compiled.sql == (
        "SELECT *\n"
        "FROM (\n"
        "select * from raw_events\n"
        ') AS "retl_source"\n'
        'WHERE ("occurred""at" IS NOT NULL AND "event_id" IS NOT NULL) AND '
        '("occurred""at" > ? OR ("occurred""at" = ? AND "event_id" > ?))\n'
        'ORDER BY "occurred""at" ASC, "event_id" ASC'
    )
    assert compiled.params == (
        "2026-05-03T10:30:00Z'; drop table events; --",
        "2026-05-03T10:30:00Z'; drop table events; --",
        "evt-9'; drop table events; --",
    )


def test_compile_snapshot_query_wraps_source_sql_with_duckdb_shape() -> None:
    compiled = compile_snapshot_query("select id, email from customers;")

    assert compiled.sql == ('SELECT *\nFROM (\nselect id, email from customers\n) AS "retl_source"')
    assert compiled.params == ()


def test_compile_keyset_scan_wraps_user_sql_as_opaque_subquery() -> None:
    compiled = compile_keyset_scan_query(
        "select event_id, occurred_at from raw_events;",
        cursor_column="occurred_at",
        primary_key_column="event_id",
        scan_after=None,
        limit=100,
    )

    assert compiled.sql.startswith("SELECT *\nFROM (\nselect event_id")
    assert (
        ') AS "retl_source"\nWHERE "occurred_at" IS NOT NULL AND "event_id" IS NOT NULL'
        in compiled.sql
    )
    assert compiled.sql.endswith('ORDER BY "occurred_at" ASC, "event_id" ASC\nLIMIT ?')
    assert compiled.sql == (
        "SELECT *\n"
        "FROM (\n"
        "select event_id, occurred_at from raw_events\n"
        ') AS "retl_source"\n'
        'WHERE "occurred_at" IS NOT NULL AND "event_id" IS NOT NULL\n'
        'ORDER BY "occurred_at" ASC, "event_id" ASC\n'
        "LIMIT ?"
    )
    assert compiled.params == (100,)


def test_compile_keyset_scan_renders_sqlglot_parseable_wrapper_predicate_and_limit() -> None:
    scan_after = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-05-03T10:30:00Z"),
        primary_key_value=CanonicalKeyScalar.string("evt-9"),
    )

    compiled = compile_keyset_scan_query(
        "select event_id, occurred_at from raw_events",
        cursor_column="occurred_at",
        primary_key_column="event_id",
        scan_after=scan_after,
        limit=25,
    )
    parsed = parse_one(compiled.sql, read="duckdb")

    assert parsed.find(exp.Subquery) is not None
    assert parsed.find(exp.Where) is not None
    assert len(list(parsed.find_all(exp.Placeholder))) == 4
    assert [order.this.sql(dialect="duckdb") for order in parsed.find_all(exp.Ordered)] == [
        '"occurred_at"',
        '"event_id"',
    ]
    assert compiled.params == ("2026-05-03T10:30:00Z", "2026-05-03T10:30:00Z", "evt-9", 25)


def test_compile_keyset_scan_supports_numeric_parameters_for_snowflake() -> None:
    scan_after = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-05-03T10:30:00Z"),
        primary_key_value=CanonicalKeyScalar.string("evt-9"),
    )

    compiled = compile_keyset_scan_query(
        "select event_id, occurred_at from raw_events",
        cursor_column="occurred_at",
        primary_key_column="event_id",
        scan_after=scan_after,
        dialect=SqlDialect(name="snowflake", parameter_style=SqlParameterStyle.NUMERIC),
        limit=25,
    )

    assert '"occurred_at" > :1' in compiled.sql
    assert '"occurred_at" = :2' in compiled.sql
    assert '"event_id" > :3' in compiled.sql
    assert compiled.sql.endswith('ORDER BY "occurred_at" ASC, "event_id" ASC\nLIMIT :4')
    assert compiled.params == ("2026-05-03T10:30:00Z", "2026-05-03T10:30:00Z", "evt-9", 25)


def test_compile_keyset_scan_supports_format_parameters_for_postgresql() -> None:
    scan_after = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-05-03T10:30:00Z"),
        primary_key_value=CanonicalKeyScalar.string("evt-9"),
    )

    compiled = compile_keyset_scan_query(
        "select event_id, occurred_at from raw_events",
        cursor_column="occurred_at",
        primary_key_column="event_id",
        scan_after=scan_after,
        dialect=SqlDialect(name="postgres", parameter_style=SqlParameterStyle.FORMAT),
        limit=25,
    )

    assert compiled.sql.count("%s") == 4
    assert compiled.sql.endswith('ORDER BY "occurred_at" ASC, "event_id" ASC\nLIMIT %s')
    assert compiled.params == ("2026-05-03T10:30:00Z", "2026-05-03T10:30:00Z", "evt-9", 25)


def test_compile_keyset_scan_fallback_keeps_unparseable_source_sql_opaque() -> None:
    scan_after = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2026-05-03T10:30:00Z"),
        primary_key_value=CanonicalKeyScalar.string("evt-9"),
    )

    compiled = compile_keyset_scan_query(
        "select * from {{ ref('raw_events') }};",
        cursor_column="occurred_at",
        primary_key_column="event_id",
        scan_after=scan_after,
        limit=10,
    )

    assert "select * from {{ ref('raw_events') }}" in compiled.sql
    assert (
        ') AS "retl_source"\nWHERE ("occurred_at" IS NOT NULL AND "event_id" IS NOT NULL)'
        in compiled.sql
    )
    assert compiled.params == ("2026-05-03T10:30:00Z", "2026-05-03T10:30:00Z", "evt-9", 10)


def test_validate_arrow_schema_rejects_duplicate_column_names() -> None:
    batch = pa.record_batch(
        [pa.array([1]), pa.array([2])],
        names=["id", "id"],
    )

    with pytest.raises(SourceSqlError, match="duplicate"):
        validate_arrow_schema(batch.schema)


def test_normalize_source_record_batch_stringifies_temporal_and_decimal_scalars() -> None:
    batch = pa.record_batch(
        [
            pa.array([1]),
            pa.array([1_775_456_700_000_000], type=pa.timestamp("us")),
            pa.array([Decimal("12.30")], type=pa.decimal128(10, 2)),
        ],
        names=["id", "occurred_at", "amount"],
    )

    normalized = normalize_source_record_batch(batch)

    assert normalized.schema.field("occurred_at").type == pa.string()
    assert normalized.schema.field("amount").type == pa.string()
    assert normalized.column(1).to_pylist() == ["2026-04-06T06:25:00Z"]
    assert normalized.column(2).to_pylist() == ["12.30"]


def test_normalize_source_record_batch_rejects_non_finite_float_values() -> None:
    batch = pa.record_batch([pa.array([1.0, math.nan])], names=["score"])

    with pytest.raises(SourceSqlError, match="non-finite"):
        normalize_source_record_batch(batch)


def test_normalize_source_record_batch_rejects_nested_object_shapes() -> None:
    batch = pa.record_batch(
        [pa.array([{"nested": "value"}], type=pa.struct([("nested", pa.string())]))],
        names=["payload"],
    )

    with pytest.raises(SourceSqlError, match="unsupported Arrow type"):
        normalize_source_record_batch(batch)
