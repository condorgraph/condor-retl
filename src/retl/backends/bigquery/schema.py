from __future__ import annotations

import re

from retl.backends.bigquery.dialect import BIGQUERY_DIALECT
from retl.sql.contracts import (
    RelationPath,
    SqlConnection,
    SqlDialectCapabilities,
    list_read,
    render_relation_path,
    render_sql,
    validate_sql_catalog_identifier,
    validate_sql_identifier,
)
from retl.stores.contracts import SqlRelationSpace
from retl.stores.sql_runtime.schema import (
    RUNTIME_TABLE_CATALOG,
    RuntimeTable,
)


def initialize_bigquery_runtime_schema(
    connection: SqlConnection,
    *,
    runtime_space: SqlRelationSpace,
    dialect: SqlDialectCapabilities = BIGQUERY_DIALECT,
) -> int:
    """Create RETL-owned BigQuery runtime objects in the configured runtime dataset."""

    _validate_runtime_space(runtime_space)
    connection.execute(f"create schema if not exists {_dataset_relation(runtime_space, dialect)}")
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
    dialect: SqlDialectCapabilities = BIGQUERY_DIALECT,
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
    create_sql = (
        f"create table if not exists {_runtime_relation(runtime_space, table.name, dialect)} "
        f"({_bigquery_definition_sql(table.definition_sql)})"
    )
    if clustering_columns := _BIGQUERY_CLUSTERING_COLUMNS.get(table.name):
        columns_sql = ", ".join(_quote(column, dialect) for column in clustering_columns)
        create_sql = f"{create_sql} cluster by {columns_sql}"
    return create_sql


def _runtime_relation(
    runtime_space: SqlRelationSpace,
    relation: str,
    dialect: SqlDialectCapabilities,
) -> str:
    return render_relation_path(
        RelationPath(relation, schema=runtime_space.schema, database=runtime_space.database),
        dialect=dialect,
    )


def _dataset_relation(
    runtime_space: SqlRelationSpace,
    dialect: SqlDialectCapabilities,
) -> str:
    return f"{_quote(runtime_space.database, dialect)}.{_quote(runtime_space.schema, dialect)}"


def _quote(identifier: str, dialect: SqlDialectCapabilities) -> str:
    return dialect.quote_identifier(identifier)


def _validate_runtime_space(runtime_space: SqlRelationSpace) -> None:
    if not isinstance(runtime_space, SqlRelationSpace):
        raise ValueError("BigQuery runtime schema initialization requires a SqlRelationSpace.")
    if runtime_space.backend_name != "bigquery":
        raise ValueError("BigQuery runtime schema initialization requires a bigquery space.")
    if runtime_space.access != "read_write":
        raise ValueError("BigQuery runtime schema initialization requires read_write access.")
    validate_sql_catalog_identifier(runtime_space.database)
    validate_sql_identifier(runtime_space.schema)


_BIGQUERY_TYPE_REPLACEMENTS = (
    (re.compile(r"\bvarchar\b", flags=re.IGNORECASE), "string"),
    (re.compile(r"\bbigint\b", flags=re.IGNORECASE), "int64"),
    (re.compile(r"\binteger\b", flags=re.IGNORECASE), "int64"),
)

_BIGQUERY_CLUSTERING_COLUMNS = {
    "ordered_work": (
        "declaration_name",
        "family",
        "collect_id",
        "sequence_order",
    ),
    "state_current": (
        "declaration_name",
        "source_name",
        "identity_json",
        "collect_id",
    ),
    "destination_batches": (
        "sync_name",
        "destination_name",
        "surface",
        "declaration_name",
    ),
}
_DEFAULT_CLAUSE_RE = re.compile(
    r"\s+not\s+null\s+default\s+(?:current_timestamp(?:[(][)])?|true|false)",
    flags=re.IGNORECASE,
)
_TABLE_CONSTRAINT_RE = re.compile(
    r",\s*(?:primary\s+key|unique)\s*\([^)]*\)",
    flags=re.IGNORECASE,
)
_COLUMN_PRIMARY_KEY_RE = re.compile(r"\s+primary\s+key\b", flags=re.IGNORECASE)


def _bigquery_definition_sql(definition_sql: str) -> str:
    sql = _bigquery_type_sql(" ".join(definition_sql.split()))
    sql = _TABLE_CONSTRAINT_RE.sub("", sql)
    return _COLUMN_PRIMARY_KEY_RE.sub("", sql)


def _bigquery_type_sql(sql: str) -> str:
    rendered = sql
    for pattern, replacement in _BIGQUERY_TYPE_REPLACEMENTS:
        rendered = pattern.sub(replacement, rendered)
    return _DEFAULT_CLAUSE_RE.sub("", rendered)


__all__ = [
    "RuntimeTable",
    "initialize_bigquery_runtime_schema",
    "restore_next_attempt_number",
]
