from __future__ import annotations

import re

from retl.backends.postgresql.dialect import POSTGRESQL_DIALECT
from retl.sql.contracts import (
    RelationPath,
    SqlConnection,
    SqlDialectCapabilities,
    list_read,
    render_relation_path,
    render_sql,
    validate_sql_identifier,
)
from retl.stores.contracts import SqlRelationSpace
from retl.stores.sql_runtime.schema import _RUNTIME_INDEXES, RUNTIME_TABLE_CATALOG, RuntimeTable


def initialize_postgresql_runtime_schema(
    connection: SqlConnection,
    *,
    runtime_space: SqlRelationSpace,
    dialect: SqlDialectCapabilities = POSTGRESQL_DIALECT,
) -> int:
    """Create RETL-owned PostgreSQL runtime objects in the configured runtime schema."""

    _validate_runtime_space(runtime_space)
    connection.execute("create extension if not exists pgcrypto")
    connection.execute(f"create schema if not exists {_quote(runtime_space.schema, dialect)}")
    for table in RUNTIME_TABLE_CATALOG.values():
        connection.execute(_create_table_sql(runtime_space, table, dialect))
    for index in _RUNTIME_INDEXES:
        connection.execute(
            f"create index if not exists {_quote(index.name, dialect)} "
            f"on {_runtime_relation(runtime_space, index.table, dialect)} "
            f"({_normalize_sql_body(index.columns_sql)})"
        )
    return restore_next_attempt_number(connection, runtime_space=runtime_space, dialect=dialect)


def restore_next_attempt_number(
    connection: SqlConnection,
    *,
    runtime_space: SqlRelationSpace,
    dialect: SqlDialectCapabilities = POSTGRESQL_DIALECT,
) -> int:
    reports_relation = RelationPath("sync_reports", schema=runtime_space.schema)
    query = list_read(reports_relation, "attempt_id")
    compiled = render_sql(query, dialect=dialect)
    attempt_ids = connection.execute(compiled.sql, compiled.params).fetchall()
    max_attempt = 0
    for (attempt_id,) in attempt_ids:
        match = re.search(r":attempt-(\d+)$", str(attempt_id))
        if match is not None:
            max_attempt = max(max_attempt, int(match.group(1)))
    return max_attempt + 1


def _create_table_sql(
    runtime_space: SqlRelationSpace,
    table: RuntimeTable,
    dialect: SqlDialectCapabilities,
) -> str:
    return (
        f"create table if not exists {_runtime_relation(runtime_space, table.name, dialect)} "
        f"({_normalize_sql_body(table.definition_sql)})"
    )


def _runtime_relation(
    runtime_space: SqlRelationSpace,
    relation: str,
    dialect: SqlDialectCapabilities,
) -> str:
    return render_relation_path(
        RelationPath(relation, schema=runtime_space.schema),
        dialect=dialect,
    )


def _quote(identifier: str, dialect: SqlDialectCapabilities) -> str:
    return dialect.quote_identifier(validate_sql_identifier(identifier))


def _normalize_sql_body(sql: str) -> str:
    return " ".join(sql.split())


def _validate_runtime_space(runtime_space: SqlRelationSpace) -> None:
    if not isinstance(runtime_space, SqlRelationSpace):
        raise ValueError("PostgreSQL runtime schema initialization requires a SqlRelationSpace.")
    if runtime_space.backend_name != "postgresql":
        raise ValueError("PostgreSQL runtime schema initialization requires a postgresql space.")
    if runtime_space.access != "read_write":
        raise ValueError("PostgreSQL runtime schema initialization requires read_write access.")
    validate_sql_identifier(runtime_space.database)
    validate_sql_identifier(runtime_space.schema)


__all__ = [
    "RuntimeTable",
    "initialize_postgresql_runtime_schema",
    "restore_next_attempt_number",
]
