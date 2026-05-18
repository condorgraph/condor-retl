from __future__ import annotations

import re

from retl.backends.snowflake.dialect import SNOWFLAKE_DIALECT
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
from retl.stores.sql_runtime.schema import (
    RUNTIME_TABLE_CATALOG,
    RuntimeTable,
)


def initialize_snowflake_runtime_schema(
    connection: SqlConnection,
    *,
    runtime_space: SqlRelationSpace,
    dialect: SqlDialectCapabilities = SNOWFLAKE_DIALECT,
) -> int:
    """Create RETL-owned Snowflake runtime objects in the configured runtime space."""

    _validate_runtime_space(runtime_space)
    connection.execute(f"create schema if not exists {_schema_relation(runtime_space, dialect)}")
    for table in RUNTIME_TABLE_CATALOG.values():
        connection.execute(_create_table_sql(runtime_space, table, dialect))
    return restore_next_attempt_number(
        connection,
        runtime_space=runtime_space,
        dialect=dialect,
    )


def restore_next_attempt_number(
    connection: SqlConnection,
    *,
    runtime_space: SqlRelationSpace,
    dialect: SqlDialectCapabilities = SNOWFLAKE_DIALECT,
) -> int:
    reports_relation = RelationPath(
        "sync_reports",
        schema=runtime_space.schema,
        database=runtime_space.database,
    )
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
        f"({_snowflake_definition_sql(table.definition_sql)})"
    )


def _runtime_relation(
    runtime_space: SqlRelationSpace,
    relation: str,
    dialect: SqlDialectCapabilities,
) -> str:
    return render_relation_path(
        RelationPath(relation, schema=runtime_space.schema, database=runtime_space.database),
        dialect=dialect,
    )


def _schema_relation(
    runtime_space: SqlRelationSpace,
    dialect: SqlDialectCapabilities,
) -> str:
    return f"{_quote(runtime_space.database, dialect)}.{_quote(runtime_space.schema, dialect)}"


def _quote(identifier: str, dialect: SqlDialectCapabilities) -> str:
    return dialect.quote_identifier(validate_sql_identifier(identifier))


def _validate_runtime_space(runtime_space: SqlRelationSpace) -> None:
    if not isinstance(runtime_space, SqlRelationSpace):
        raise ValueError("Snowflake runtime schema initialization requires a SqlRelationSpace.")
    if runtime_space.backend_name != "snowflake":
        raise ValueError("Snowflake runtime schema initialization requires a snowflake space.")
    if runtime_space.access != "read_write":
        raise ValueError("Snowflake runtime schema initialization requires read_write access.")
    validate_sql_identifier(runtime_space.database)
    validate_sql_identifier(runtime_space.schema)


_SNOWFLAKE_TYPE_REPLACEMENTS = (
    (re.compile(r"\btimestamp\b", flags=re.IGNORECASE), "timestamp_ntz"),
    (re.compile(r"\bbigint\b", flags=re.IGNORECASE), "number(38, 0)"),
    (re.compile(r"\binteger\b", flags=re.IGNORECASE), "number(38, 0)"),
)


def _snowflake_definition_sql(definition_sql: str) -> str:
    return _snowflake_type_sql(" ".join(definition_sql.split()))


def _snowflake_type_sql(sql: str) -> str:
    rendered = sql
    for pattern, replacement in _SNOWFLAKE_TYPE_REPLACEMENTS:
        rendered = pattern.sub(replacement, rendered)
    return rendered


__all__ = [
    "RuntimeTable",
    "initialize_snowflake_runtime_schema",
    "restore_next_attempt_number",
]
