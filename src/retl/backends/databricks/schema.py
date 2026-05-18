from __future__ import annotations

import re

from retl.backends.databricks.dialect import DATABRICKS_DIALECT
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


def initialize_databricks_runtime_schema(
    connection: SqlConnection,
    *,
    runtime_space: SqlRelationSpace,
    dialect: SqlDialectCapabilities = DATABRICKS_DIALECT,
) -> int:
    """Create RETL-owned Databricks runtime objects in the configured UC schema."""

    _validate_runtime_space(runtime_space)
    connection.execute(f"create schema if not exists {_schema_relation(runtime_space, dialect)}")
    for table in RUNTIME_TABLE_CATALOG.values():
        relation_sql = _runtime_relation(runtime_space, table.name, dialect)
        connection.execute(_create_table_sql(relation_sql, table))
        connection.execute(_enable_catalog_managed_commits_sql(relation_sql))
    return restore_next_attempt_number(
        connection,
        runtime_space=runtime_space,
        dialect=dialect,
    )


def restore_next_attempt_number(
    connection: SqlConnection,
    *,
    runtime_space: SqlRelationSpace,
    dialect: SqlDialectCapabilities = DATABRICKS_DIALECT,
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
    relation_sql: str,
    table: RuntimeTable,
) -> str:
    return (
        f"create table if not exists {relation_sql} "
        f"({_databricks_definition_sql(table.definition_sql)}) using delta "
        "tblproperties ('delta.feature.catalogManaged' = 'supported')"
    )


def _enable_catalog_managed_commits_sql(relation_sql: str) -> str:
    return (
        f"alter table {relation_sql} set tblproperties "
        "('delta.feature.catalogManaged' = 'supported')"
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
        raise ValueError("Databricks runtime schema initialization requires a SqlRelationSpace.")
    if runtime_space.backend_name != "databricks":
        raise ValueError("Databricks runtime schema initialization requires a databricks space.")
    if runtime_space.access != "read_write":
        raise ValueError("Databricks runtime schema initialization requires read_write access.")
    validate_sql_identifier(runtime_space.database)
    validate_sql_identifier(runtime_space.schema)
    if runtime_space.database.casefold() == "hive_metastore":
        raise ValueError("Databricks runtime schema initialization requires Unity Catalog.")


_DATABRICKS_TYPE_REPLACEMENTS = (
    (re.compile(r"\bvarchar\b", flags=re.IGNORECASE), "string"),
    (re.compile(r"\btimestamp\b", flags=re.IGNORECASE), "timestamp_ntz"),
)
_DEFAULT_CLAUSE_RE = re.compile(
    r"\s+not\s+null\s+default\s+(?:current_timestamp(?:[(][)])?|true|false)",
    flags=re.IGNORECASE,
)
_TABLE_CONSTRAINT_RE = re.compile(
    r",\s*(?:primary\s+key|unique)\s*\([^)]*\)",
    flags=re.IGNORECASE,
)
_COLUMN_PRIMARY_KEY_RE = re.compile(r"\s+primary\s+key\b", flags=re.IGNORECASE)


def _databricks_definition_sql(definition_sql: str) -> str:
    sql = " ".join(definition_sql.split())
    for pattern, replacement in _DATABRICKS_TYPE_REPLACEMENTS:
        sql = pattern.sub(replacement, sql)
    sql = _DEFAULT_CLAUSE_RE.sub("", sql)
    sql = _TABLE_CONSTRAINT_RE.sub("", sql)
    return _COLUMN_PRIMARY_KEY_RE.sub("", sql)


__all__ = [
    "RuntimeTable",
    "initialize_databricks_runtime_schema",
    "restore_next_attempt_number",
]
