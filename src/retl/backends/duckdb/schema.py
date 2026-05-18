from __future__ import annotations

from retl.backends.duckdb.dialect import DUCKDB_DIALECT
from retl.sql.contracts import SqlConnection, SqlDialectCapabilities, validate_sql_identifier
from retl.stores.sql_runtime.schema import (
    _RUNTIME_INDEXES,
    RUNTIME_TABLE_CATALOG,
    RuntimeTable,
    _create_index_sql,
    _create_table_sql,
    restore_next_attempt_number,
)


def initialize_duckdb_runtime_schema(
    connection: SqlConnection,
    *,
    schema: str,
    dialect: SqlDialectCapabilities = DUCKDB_DIALECT,
) -> int:
    schema = validate_sql_identifier(schema)
    connection.execute(f"create schema if not exists {dialect.quote_identifier(schema)}")
    for table in RUNTIME_TABLE_CATALOG.values():
        connection.execute(_create_table_sql(schema, table, dialect))
    for index in _RUNTIME_INDEXES:
        connection.execute(_create_index_sql(schema, index, dialect))
    return restore_next_attempt_number(connection, schema=schema, dialect=dialect)


__all__ = [
    "RuntimeTable",
    "initialize_duckdb_runtime_schema",
]
