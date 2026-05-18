from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager, nullcontext
from typing import Any

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
    table,
    validate_sql_catalog_identifier,
    validate_sql_identifier,
)
from retl.stores.contracts import SqlRelationSpace


class BigQuerySqlDialect(SimpleSqlDialect):
    """BigQuery SQLGlot rendering and backend-owned SQL capability helpers."""

    def __init__(self) -> None:
        super().__init__(
            name="bigquery",
            sqlglot_dialect="bigquery",
            parameter_style=SqlParameterStyle.QMARK,
            identifier_quote="`",
        )

    @property
    def executable_collect_runtime_store_label(self) -> str:
        return "BigQuerySqlBackend-owned runtime store"

    def quote_identifier(self, identifier: str) -> str:
        value = validate_sql_catalog_identifier(identifier)
        escaped = value.replace("`", "``")
        return f"`{escaped}`"

    def source_relation(
        self,
        source_space: SqlRelationSpace,
        relation: RelationName | str,
    ) -> RelationPath:
        self._validate_relation_space(
            source_space,
            expected_access="read_only",
            context="BigQuery Source relation space",
        )
        return RelationPath(
            name=relation,
            schema=source_space.schema,
            database=source_space.database,
        )

    def runtime_relation(
        self,
        runtime_space: SqlRelationSpace,
        relation: RelationName | str,
    ) -> RelationPath:
        self._validate_relation_space(
            runtime_space,
            expected_access="read_write",
            context="BigQuery Runtime relation space",
        )
        return RelationPath(
            name=relation,
            schema=runtime_space.schema,
            database=runtime_space.database,
        )

    def render_source_relation(self, source_space: SqlRelationSpace, relation: str) -> str:
        return render_relation_path(self.source_relation(source_space, relation), dialect=self)

    def render_runtime_relation(self, runtime_space: SqlRelationSpace, relation: str) -> str:
        return render_relation_path(self.runtime_relation(runtime_space, relation), dialect=self)

    def source_schema_context(
        self,
        connection: SqlConnection,
        source_space: SqlRelationSpace,
    ) -> AbstractContextManager[None]:
        self._validate_relation_space(
            source_space,
            expected_access="read_only",
            context="BigQuery Source relation space",
        )
        _ = connection
        return nullcontext()

    def json_object_sql(self, entries: Mapping[str, str]) -> str:
        if not entries:
            return "JSON_OBJECT()"
        parts: list[str] = []
        for key, value_sql in entries.items():
            parts.extend([self.sql_literal(key), value_sql])
        return f"JSON_OBJECT({', '.join(parts)})"

    def json_array_sql(self, values: list[str]) -> str:
        return f"JSON_ARRAY({', '.join(values)})" if values else "JSON_ARRAY()"

    def json_concat_arrays_sql(self, arrays: list[str]) -> str:
        if not arrays:
            return "JSON_ARRAY()"
        if len(arrays) == 1:
            return arrays[0]
        query_arrays = ", ".join(f"JSON_QUERY_ARRAY({array})" for array in arrays)
        return f"TO_JSON(ARRAY_CONCAT({query_arrays}))"

    def json_parse_sql(self, value_sql: str) -> str:
        return f"PARSE_JSON({value_sql})"

    def json_serialize_sql(self, value_sql: str) -> str:
        return f"TO_JSON_STRING({value_sql})"

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
            "TO_JSON(ARRAY("
            "SELECT AS STRUCT "
            f"{self.sql_literal(identifier_type)} AS type, "
            "CAST(retl_identifier_value AS STRING) AS value "
            f"FROM UNNEST({values_sql}) AS retl_identifier_value "
            "ORDER BY retl_identifier_value"
            "))"
        )

    def json_extract_scalar_sql(self, json_sql: str, path: str) -> str:
        return f"JSON_VALUE({json_sql}, {self.sql_literal(path)})"

    def cast_to_text_sql(self, value_sql: str) -> str:
        return f"CAST({value_sql} AS STRING)"

    def text_null_sql(self) -> str:
        return "CAST(NULL AS STRING)"

    def concat_sql(self, parts: list[str]) -> str:
        return f"CONCAT({', '.join(parts)})"

    def sha256_sql(self, value_sql: str) -> str:
        return f"TO_HEX(SHA256(CAST({value_sql} AS STRING)))"

    def temp_relation(self, name: RelationName | str) -> RelationPath:
        return RelationPath(name=name)

    def render_temp_relation(self, name: str) -> str:
        return render_relation_path(self.temp_relation(name), dialect=self)

    def create_temp_table_as_sql(self, name: str, query_sql: str) -> str:
        return f"create temp table {self.render_temp_relation(name)} as {query_sql}"

    def drop_temp_table_sql(self, name: str) -> str:
        return f"drop table if exists _SESSION.{self.render_temp_relation(name)}"

    def limit_sql(self, query_sql: str, limit_sql: str) -> str:
        return f"{query_sql} LIMIT {limit_sql}"

    def begin_transaction_sql(self) -> str:
        return "begin transaction"

    def commit_sql(self) -> str:
        return "commit transaction"

    def rollback_sql(self) -> str:
        return "rollback transaction"

    def begin_transaction(self, connection: SqlConnection) -> None:
        connection.execute(self.begin_transaction_sql())

    def commit(self, connection: SqlConnection) -> None:
        connection.execute(self.commit_sql())

    def rollback(self, connection: SqlConnection) -> None:
        connection.execute(self.rollback_sql())

    def runtime_reset_uses_transaction(self) -> bool:
        return False

    def delete_all_rows_sql(self, relation_sql: str) -> str:
        return f"TRUNCATE TABLE {relation_sql}"

    def upsert_sql(self, upsert: SqlRuntimeUpsert) -> str:
        target_sql = render_sqlglot(table(upsert.relation), dialect=self)
        source_sql = self._upsert_source_row_select_sql(upsert)
        match_sql = render_sqlglot(upsert.match_condition(), dialect=self)
        insert_columns_sql = ", ".join(
            render_sqlglot(identifier(column_name), dialect=self)
            for column_name in upsert.row.columns
        )
        insert_values_sql = ", ".join(
            render_sqlglot(value, dialect=self) for value in upsert.source_insert_values()
        )
        matched_sql = ""
        assignments = upsert.source_update_assignments()
        if assignments:
            assignment_sql = ", ".join(
                self._upsert_assignment_sql(assignment) for assignment in assignments
            )
            matched_sql = f" WHEN MATCHED THEN UPDATE SET {assignment_sql}"
        return (
            f"MERGE `{'.'.join(str(part) for part in upsert.relation.parts)}` AS "
            f"{upsert.target_alias} "
            f"USING ({source_sql}) AS {upsert.source_alias} "
            f"ON {match_sql}"
            f"{matched_sql} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns_sql}) VALUES ({insert_values_sql})"
        ).replace(
            f"MERGE `{'.'.join(str(part) for part in upsert.relation.parts)}`",
            f"MERGE {target_sql}",
        )

    def _upsert_assignment_sql(self, assignment: SqlUpsertAssignment) -> str:
        column_sql = render_sqlglot(identifier(assignment.column), dialect=self)
        value_sql = self._upsert_assignment_value_sql(assignment.value)
        return f"{column_sql} = {value_sql}"

    def _upsert_assignment_value_sql(self, value: Any) -> str:
        if type(value).__name__ == "Anonymous" and str(value.this).casefold() == "now":
            return "CURRENT_TIMESTAMP()"
        if str(value).casefold() == "current_timestamp":
            return "CURRENT_TIMESTAMP()"
        return render_sqlglot(value, dialect=self)

    def _upsert_source_row_select_sql(self, upsert: SqlRuntimeUpsert) -> str:
        expressions: list[str] = []
        for column_name, value in zip(upsert.row.columns, upsert.row.values, strict=True):
            value_sql = render_sqlglot(value, dialect=self)
            cast_type = self._runtime_column_cast_type(
                table_name=upsert.relation.name.value,
                column_name=column_name.value,
            )
            if cast_type is not None:
                value_sql = f"CAST({value_sql} AS {cast_type})"
            expressions.append(f"{value_sql} AS {self.quote_identifier(column_name.value)}")
        return "SELECT " + ", ".join(expressions)

    def _runtime_column_cast_type(self, *, table_name: str, column_name: str) -> str | None:
        from retl.stores.sql_runtime.schema import RUNTIME_TABLE_CATALOG

        table_definition = RUNTIME_TABLE_CATALOG.get(table_name)
        if table_definition is None:
            return None
        column_type = _runtime_column_type(table_definition.definition_sql, column_name)
        if column_type is None:
            return None
        if column_type in {"varchar", "string"}:
            return "STRING"
        if column_type in {"bigint", "integer", "int"}:
            return "INT64"
        if column_type == "boolean":
            return "BOOL"
        if column_type == "timestamp":
            return "TIMESTAMP"
        return None

    def runtime_column_cast_type(self, *, table_name: str, column_name: str) -> str | None:
        cast_type = self._runtime_column_cast_type(table_name=table_name, column_name=column_name)
        if cast_type == "TIMESTAMP":
            return "TIMESTAMPTZ"
        return cast_type

    def upsert_declaration_sql(
        self,
        declarations_relation_sql: str,
        values: Mapping[str, str],
    ) -> str:
        return f"""
        merge {declarations_relation_sql} as target
        using (
            select
                {values["declaration_version_id"]} as declaration_version_id,
                {values["declaration_name"]} as declaration_name,
                {values["declaration_kind"]} as declaration_kind,
                {values["source_name"]} as source_name,
                {values["source_backend"]} as source_backend,
                {values["source_location_json"]} as source_location_json,
                {values["source_query_hash"]} as source_query_hash,
                {values["declaration_json"]} as declaration_json
        ) as source
        on target.declaration_name = source.declaration_name
       and target.declaration_version_id = source.declaration_version_id
        when matched then update set
            declaration_kind = source.declaration_kind,
            source_name = source.source_name,
            source_backend = source.source_backend,
            source_location_json = source.source_location_json,
            source_query_hash = source.source_query_hash,
            declaration_json = source.declaration_json,
            last_seen_at = current_timestamp(),
            active = true
        when not matched then insert (
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
        ) values (
            source.declaration_version_id,
            source.declaration_name,
            source.declaration_kind,
            source.source_name,
            source.source_backend,
            source.source_location_json,
            source.source_query_hash,
            source.declaration_json,
            current_timestamp(),
            current_timestamp(),
            true
        )
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
        if space.backend_name != "bigquery":
            raise ValueError(f"{context} backend must be bigquery.")
        if space.access != expected_access:
            raise ValueError(f"{context} access must be {expected_access}.")
        validate_sql_catalog_identifier(space.database)
        validate_sql_identifier(space.schema)


BIGQUERY_DIALECT = BigQuerySqlDialect()


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


__all__ = ["BIGQUERY_DIALECT", "BigQuerySqlDialect"]
