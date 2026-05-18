from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest

from retl.backends.snowflake import SNOWFLAKE_DIALECT
from retl.backends.snowflake.schema import (
    _runtime_relation,
    initialize_snowflake_runtime_schema,
)
from retl.stores.contracts import SqlRelationSpace
from retl.stores.sql_runtime.schema import runtime_table_names


def _runtime_space() -> SqlRelationSpace:
    return SqlRelationSpace(
        backend_name="snowflake",
        database="RETL_DB",
        schema="RETL_RUNTIME",
        access="read_write",
    )


def test_snowflake_runtime_schema_initialization_qualifies_runtime_objects_only() -> None:
    connection = RecordingSqlConnection(
        fetchall_rows=[("runner:sync:attempt-7",), ("runner:sync:attempt-12",)]
    )

    next_attempt = initialize_snowflake_runtime_schema(
        connection,
        runtime_space=_runtime_space(),
    )

    assert next_attempt == 13
    statements = [sql for sql, _ in connection.calls]
    assert statements[0] == 'create schema if not exists "RETL_DB"."RETL_RUNTIME"'
    assert statements[1].startswith('create table if not exists "RETL_DB"."RETL_RUNTIME"."runs"')
    assert 'SELECT "ATTEMPT_ID" FROM "RETL_DB"."RETL_RUNTIME"."sync_reports"' in (statements[-1])
    assert not any('"SOURCE_DB"."APP"' in sql for sql in statements)
    assert not any(sql.startswith("create index") for sql in statements)

    created_tables = {
        table_name
        for table_name in runtime_table_names()
        if any(
            sql.startswith(f'create table if not exists "RETL_DB"."RETL_RUNTIME"."{table_name}"')
            for sql in statements
        )
    }
    assert created_tables == runtime_table_names()


def test_snowflake_runtime_schema_maps_shared_runtime_types_to_snowflake_ddl() -> None:
    connection = RecordingSqlConnection()

    initialize_snowflake_runtime_schema(connection, runtime_space=_runtime_space())

    statements = [sql for sql, _ in connection.calls]
    ordered_work = next(sql for sql in statements if '"ordered_work"' in sql)
    sync_reports = next(sql for sql in statements if '"sync_reports"' in sql)

    assert "collect_id varchar not null" in ordered_work
    assert "created_at timestamp_ntz not null default current_timestamp" in ordered_work
    assert "unique (collect_id, sequence_order)" in ordered_work
    assert "http_status number(38, 0)" in sync_reports


def test_snowflake_runtime_schema_helper_renders_validated_relation_paths() -> None:
    assert _runtime_relation(_runtime_space(), "ordered_work", SNOWFLAKE_DIALECT) == (
        '"RETL_DB"."RETL_RUNTIME"."ordered_work"'
    )

    with pytest.raises(ValueError, match="simple SQL identifier"):
        _runtime_relation(_runtime_space(), "ordered-work", SNOWFLAKE_DIALECT)


def test_snowflake_runtime_schema_rejects_non_snowflake_runtime_space() -> None:
    with pytest.raises(ValueError, match="snowflake space"):
        initialize_snowflake_runtime_schema(
            RecordingSqlConnection(),
            runtime_space=SqlRelationSpace(
                backend_name="duckdb",
                database="runtime.duckdb",
                schema="retl",
                access="read_write",
            ),
        )


class RecordingSqlConnection:
    def __init__(self, *, fetchall_rows: Sequence[tuple[object, ...]] = ()) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | dict[str, object]]] = []
        self.fetchall_rows = tuple(fetchall_rows)

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] = (),
    ) -> "RecordingSqlConnection":
        params: tuple[object, ...] | dict[str, object]
        params = dict(parameters) if isinstance(parameters, Mapping) else tuple(parameters)
        self.calls.append((sql, params))
        return self

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self.fetchall_rows
