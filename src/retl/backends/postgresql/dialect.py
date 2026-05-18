from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Any, Iterator

from retl.sql.contracts import (
    RelationName,
    RelationPath,
    SimpleSqlDialect,
    SqlConnection,
    SqlParameterStyle,
    SqlRuntimeUpsert,
    SqlUpsertAssignment,
    identifier,
    render_relation_path,
    render_sqlglot,
    validate_sql_identifier,
)
from retl.stores.contracts import SqlRelationSpace


class PostgreSqlDialect(SimpleSqlDialect):
    """PostgreSQL SQLGlot rendering and backend-owned SQL capability helpers."""

    def __init__(self) -> None:
        super().__init__(
            name="postgresql",
            sqlglot_dialect="postgres",
            parameter_style=SqlParameterStyle.FORMAT,
        )

    @property
    def executable_collect_runtime_store_label(self) -> str:
        return "PostgreSqlBackend-owned runtime store"

    def source_relation(
        self,
        source_space: SqlRelationSpace,
        relation: RelationName | str,
    ) -> RelationPath:
        self._validate_relation_space(
            source_space,
            expected_access="read_only",
            context="PostgreSQL Source relation space",
        )
        return RelationPath(name=relation, schema=source_space.schema)

    def runtime_relation(
        self,
        runtime_space: SqlRelationSpace,
        relation: RelationName | str,
    ) -> RelationPath:
        self._validate_relation_space(
            runtime_space,
            expected_access="read_write",
            context="PostgreSQL Runtime relation space",
        )
        return RelationPath(name=relation, schema=runtime_space.schema)

    def render_source_relation(self, source_space: SqlRelationSpace, relation: str) -> str:
        return render_relation_path(self.source_relation(source_space, relation), dialect=self)

    def render_runtime_relation(self, runtime_space: SqlRelationSpace, relation: str) -> str:
        return render_relation_path(self.runtime_relation(runtime_space, relation), dialect=self)

    def current_schema_sql(self) -> str:
        return "select current_schema()"

    def set_schema_sql(self, schema: str) -> str:
        return (
            f"set search_path to {self.quote_identifier(validate_sql_identifier(schema))}, public"
        )

    @contextmanager
    def schema_context(self, connection: SqlConnection, schema: str) -> Iterator[None]:
        before_record = connection.execute(self.current_schema_sql()).fetchone()
        if before_record is None:
            raise RuntimeError("PostgreSQL did not return the current schema context.")
        before_schema = str(before_record[0])
        schema = validate_sql_identifier(schema)
        if schema != before_schema:
            connection.execute(self.set_schema_sql(schema))
        try:
            yield
        finally:
            if schema != before_schema:
                connection.execute(self.set_schema_sql(before_schema))

    def source_schema_context(
        self,
        connection: SqlConnection,
        source_space: SqlRelationSpace,
    ) -> AbstractContextManager[None]:
        self._validate_relation_space(
            source_space,
            expected_access="read_only",
            context="PostgreSQL Source relation space",
        )
        return self.schema_context(connection, source_space.schema)

    def json_object_sql(self, entries: Mapping[str, str]) -> str:
        if not entries:
            return "jsonb_build_object()"
        parts: list[str] = []
        for key, value_sql in entries.items():
            parts.extend([self.sql_literal(key), value_sql])
        return f"jsonb_build_object({', '.join(parts)})"

    def json_array_sql(self, values: list[str]) -> str:
        if not values:
            return "'[]'::jsonb"
        return f"jsonb_build_array({', '.join(values)})"

    def json_concat_arrays_sql(self, arrays: list[str]) -> str:
        if not arrays:
            return "'[]'::jsonb"
        if len(arrays) == 1:
            return arrays[0]
        return " || ".join(f"({array_sql})" for array_sql in arrays)

    def json_parse_sql(self, value_sql: str) -> str:
        return f"({value_sql})::jsonb"

    def json_serialize_sql(self, value_sql: str) -> str:
        return f"({value_sql})::text"

    def identifier_scalar_array_sql(self, *, identifier_type: str, value_sql: str) -> str:
        return self.json_array_sql(
            [
                self.json_object_sql(
                    {
                        "type": self.sql_literal(identifier_type),
                        "value": value_sql,
                    }
                )
            ]
        )

    def identifier_list_array_sql(self, *, identifier_type: str, values_sql: str) -> str:
        return (
            "coalesce((select jsonb_agg("
            + self.json_object_sql(
                {
                    "type": self.sql_literal(identifier_type),
                    "value": "retl_identifier_value::text",
                }
            )
            + " order by retl_identifier_value::text) "
            f"from unnest({values_sql}) as retl_identifier_value), '[]'::jsonb)"
        )

    def json_extract_scalar_sql(self, json_sql: str, path: str) -> str:
        return f"jsonb_path_query_first({json_sql}, {self.sql_literal(path)}) #>> '{{}}'"

    def cast_to_text_sql(self, value_sql: str) -> str:
        return f"cast({value_sql} as text)"

    def concat_sql(self, parts: list[str]) -> str:
        return " || ".join(parts)

    def sha256_sql(self, value_sql: str) -> str:
        return f"encode(digest(cast({value_sql} as text), 'sha256'), 'hex')"

    def temp_relation(self, name: RelationName | str) -> RelationPath:
        return RelationPath(name=name)

    def render_temp_relation(self, name: str) -> str:
        return render_relation_path(self.temp_relation(name), dialect=self)

    def create_temp_table_as_sql(self, name: str, query_sql: str) -> str:
        return f"create temporary table {self.render_temp_relation(name)} as {query_sql}"

    def drop_temp_table_sql(self, name: str) -> str:
        return f"drop table if exists {self.render_temp_relation(name)}"

    def limit_sql(self, query_sql: str, limit_sql: str) -> str:
        return f"{query_sql} limit {limit_sql}"

    def begin_transaction_sql(self) -> str:
        return "begin"

    def commit_sql(self) -> str:
        return "commit"

    def rollback_sql(self) -> str:
        return "rollback"

    def begin_transaction(self, connection: SqlConnection) -> None:
        connection.execute(self.begin_transaction_sql())

    def commit(self, connection: SqlConnection) -> None:
        connection.execute(self.commit_sql())

    def rollback(self, connection: SqlConnection) -> None:
        connection.execute(self.rollback_sql())

    def upsert_sql(self, upsert: SqlRuntimeUpsert) -> str:
        insert_sql = render_sqlglot(upsert.insert_statement(), dialect=self)
        key_sql = ", ".join(
            render_sqlglot(identifier(column_name), dialect=self)
            for column_name in upsert.key_columns
        )
        assignments = upsert.source_update_assignments(source_alias="excluded")
        if not assignments:
            return f"{insert_sql} ON CONFLICT ({key_sql}) DO NOTHING"
        assignment_sql = ", ".join(
            self._upsert_assignment_sql(assignment) for assignment in assignments
        )
        return f"{insert_sql} ON CONFLICT ({key_sql}) DO UPDATE SET {assignment_sql}"

    def _upsert_assignment_sql(self, assignment: SqlUpsertAssignment) -> str:
        column_sql = render_sqlglot(identifier(assignment.column), dialect=self)
        value_sql = self._upsert_assignment_value_sql(assignment.value)
        return f"{column_sql} = {value_sql}"

    def _upsert_assignment_value_sql(self, value: Any) -> str:
        if type(value).__name__ == "Anonymous" and str(value.this).casefold() == "now":
            return "now()"
        return render_sqlglot(value, dialect=self)

    def runtime_column_cast_type(self, *, table_name: str, column_name: str) -> str | None:
        from retl.stores.sql_runtime.schema import RUNTIME_TABLE_CATALOG

        table_definition = RUNTIME_TABLE_CATALOG.get(table_name)
        if table_definition is None:
            return None
        column_type = _runtime_column_type(table_definition.definition_sql, column_name)
        if column_type is None:
            return None
        if column_type == "varchar":
            return "TEXT"
        if column_type == "bigint":
            return "BIGINT"
        if column_type == "integer":
            return "INTEGER"
        if column_type == "boolean":
            return "BOOLEAN"
        if column_type == "timestamp":
            return "TIMESTAMP"
        return None

    def upsert_declaration_sql(
        self,
        declarations_relation_sql: str,
        values: Mapping[str, str],
    ) -> str:
        return f"""
        insert into {declarations_relation_sql} (
            declaration_version_id,
            declaration_name,
            declaration_kind,
            source_name,
            source_backend,
            source_location_json,
            source_query_hash,
            declaration_json,
            first_seen_at,
            last_seen_at,
            active
        )
        values (
            {values["declaration_version_id"]},
            {values["declaration_name"]},
            {values["declaration_kind"]},
            {values["source_name"]},
            {values["source_backend"]},
            {values["source_location_json"]},
            {values["source_query_hash"]},
            {values["declaration_json"]},
            now(),
            now(),
            true
        )
        on conflict (declaration_name, declaration_version_id) do update set
            declaration_kind = excluded.declaration_kind,
            source_name = excluded.source_name,
            source_backend = excluded.source_backend,
            source_location_json = excluded.source_location_json,
            source_query_hash = excluded.source_query_hash,
            declaration_json = excluded.declaration_json,
            last_seen_at = now(),
            active = true
        """

    def sql_literal(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _validate_relation_space(
        self,
        space: SqlRelationSpace,
        *,
        expected_access: str,
        context: str,
    ) -> None:
        if not isinstance(space, SqlRelationSpace):
            raise ValueError(f"{context} must be a SqlRelationSpace.")
        if space.backend_name != "postgresql":
            raise ValueError(f"{context} backend must be postgresql.")
        if space.access != expected_access:
            raise ValueError(f"{context} access must be {expected_access}.")
        validate_sql_identifier(space.database)
        validate_sql_identifier(space.schema)


POSTGRESQL_DIALECT = PostgreSqlDialect()


def _runtime_column_type(definition_sql: str, column_name: str) -> str | None:
    for raw_line in definition_sql.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2 or parts[0] != column_name:
            continue
        return parts[1].casefold()
    return None


__all__ = ["POSTGRESQL_DIALECT", "PostgreSqlDialect"]
