from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from typing import Any, Iterator

from retl.sql.contracts import (
    ColumnName,
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
    validate_sql_identifier,
)
from retl.stores.contracts import SqlRelationSpace


class SnowflakeSqlDialect(SimpleSqlDialect):
    """Snowflake SQLGlot rendering and Snowflake-owned SQL capability helpers."""

    uppercase_quoted_columns = True

    def __init__(self) -> None:
        super().__init__(
            name="snowflake",
            sqlglot_dialect="snowflake",
            parameter_style=SqlParameterStyle.NUMERIC,
        )

    @property
    def executable_collect_runtime_store_label(self) -> str:
        return "SnowflakeSqlBackend-owned runtime store"

    def source_relation(
        self,
        source_space: SqlRelationSpace,
        relation: RelationName | str,
    ) -> RelationPath:
        self._validate_relation_space(
            source_space,
            expected_access="read_only",
            context="Snowflake Source relation space",
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
            context="Snowflake Runtime relation space",
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

    def current_database_schema_sql(self) -> str:
        return "select current_database(), current_schema()"

    def use_schema_sql(self, *, database: str, schema: str) -> str:
        return (
            "use schema "
            f"{self.quote_identifier(validate_sql_identifier(database))}."
            f"{self.quote_identifier(validate_sql_identifier(schema))}"
        )

    @contextmanager
    def schema_context(
        self,
        connection: SqlConnection,
        *,
        database: str,
        schema: str,
    ) -> Iterator[None]:
        before_record = connection.execute(self.current_database_schema_sql()).fetchone()
        if before_record is None:
            raise RuntimeError("Snowflake did not return the current database/schema context.")
        before_database = str(before_record[0])
        before_schema = str(before_record[1])
        database = validate_sql_identifier(database)
        schema = validate_sql_identifier(schema)
        if (
            database.casefold(),
            schema.casefold(),
        ) != (
            before_database.casefold(),
            before_schema.casefold(),
        ):
            connection.execute(self.use_schema_sql(database=database, schema=schema))
        try:
            yield
        finally:
            if (
                database.casefold(),
                schema.casefold(),
            ) != (
                before_database.casefold(),
                before_schema.casefold(),
            ):
                connection.execute(
                    self.use_schema_sql(database=before_database, schema=before_schema)
                )

    def source_schema_context(
        self,
        connection: SqlConnection,
        source_space: SqlRelationSpace,
    ) -> AbstractContextManager[None]:
        self._validate_relation_space(
            source_space,
            expected_access="read_only",
            context="Snowflake Source relation space",
        )
        return self.schema_context(
            connection,
            database=source_space.database,
            schema=source_space.schema,
        )

    def json_object_sql(self, entries: Mapping[str, str]) -> str:
        if not entries:
            return "object_construct_keep_null()"
        parts: list[str] = []
        for key, value_sql in entries.items():
            parts.extend([self.sql_literal(key), value_sql])
        return f"object_construct_keep_null({', '.join(parts)})"

    def json_array_sql(self, values: list[str]) -> str:
        if not values:
            return "array_construct()"
        return f"array_construct({', '.join(values)})"

    def json_concat_arrays_sql(self, arrays: list[str]) -> str:
        if not arrays:
            return "array_construct()"
        if len(arrays) == 1:
            return arrays[0]
        combined = f"array_cat({arrays[0]}, {arrays[1]})"
        for array_sql in arrays[2:]:
            combined = f"array_cat({combined}, {array_sql})"
        return combined

    def json_parse_sql(self, value_sql: str) -> str:
        return f"parse_json({value_sql})"

    def json_serialize_sql(self, value_sql: str) -> str:
        return f"to_json({value_sql})"

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
        value_sql = "retl_identifier_value::string"
        return (
            "coalesce(transform(array_sort("
            f"{values_sql}), retl_identifier_value -> "
            + self.json_object_sql(
                {
                    "type": self.sql_literal(identifier_type),
                    "value": value_sql,
                }
            )
            + "), array_construct())"
        )

    def json_extract_scalar_sql(self, json_sql: str, path: str) -> str:
        return f"get_path({json_sql}, {self.sql_literal(path)})::string"

    def cast_to_text_sql(self, value_sql: str) -> str:
        return f"cast({value_sql} as string)"

    def concat_sql(self, parts: list[str]) -> str:
        return f"concat({', '.join(parts)})"

    def sha256_sql(self, value_sql: str) -> str:
        return f"sha2(cast({value_sql} as string), 256)"

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
        target_sql = render_sqlglot(table(upsert.relation), dialect=self)
        source_sql = render_sqlglot(upsert.source_row_select(), dialect=self)
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
            f"MERGE INTO {target_sql} AS {upsert.target_alias} "
            f"USING ({source_sql}) AS {upsert.source_alias} "
            f"ON {match_sql}"
            f"{matched_sql} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns_sql}) VALUES ({insert_values_sql})"
        )

    def _upsert_assignment_sql(self, assignment: SqlUpsertAssignment) -> str:
        column_sql = render_sqlglot(identifier(assignment.column), dialect=self)
        value_sql = self._upsert_assignment_value_sql(assignment.value)
        return f"{column_sql} = {value_sql}"

    def _upsert_assignment_value_sql(self, value: Any) -> str:
        if type(value).__name__ == "Anonymous" and str(value.this).casefold() == "now":
            return "current_timestamp"
        return render_sqlglot(value, dialect=self)

    def update_many_sql(
        self,
        *,
        relation: RelationPath,
        source: Any,
        key_columns: Sequence[ColumnName],
        update_columns: Sequence[ColumnName],
        update_assignments: Sequence[SqlUpsertAssignment],
    ) -> str:
        target_sql = render_sqlglot(table(relation), dialect=self)
        source_sql = render_sqlglot(source.this, dialect=self)
        match_sql = " AND ".join(
            f"target.{self._column_identifier_sql(column_name)} = "
            f"source.{self._column_identifier_sql(column_name)}"
            for column_name in key_columns
        )
        assignments = [
            f"{self._column_identifier_sql(column_name)} = "
            f"source.{self._column_identifier_sql(column_name)}"
            for column_name in update_columns
        ]
        assignments.extend(
            self._upsert_assignment_sql(assignment) for assignment in update_assignments
        )
        return (
            f"MERGE INTO {target_sql} AS target "
            f"USING ({source_sql}) AS source "
            f"ON {match_sql} "
            f"WHEN MATCHED THEN UPDATE SET {', '.join(assignments)}"
        )

    def _column_identifier_sql(self, column_name: ColumnName) -> str:
        return render_sqlglot(identifier(column_name), dialect=self)

    def upsert_declaration_sql(
        self,
        declarations_relation_sql: str,
        values: Mapping[str, str],
    ) -> str:
        return f"""
        merge into {declarations_relation_sql} as target
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
            last_seen_at = current_timestamp,
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
            current_timestamp,
            current_timestamp,
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
        if space.backend_name != "snowflake":
            raise ValueError(f"{context} backend must be snowflake.")
        if space.access != expected_access:
            raise ValueError(f"{context} access must be {expected_access}.")
        validate_sql_identifier(space.database)
        validate_sql_identifier(space.schema)


SNOWFLAKE_DIALECT = SnowflakeSqlDialect()


__all__ = ["SNOWFLAKE_DIALECT", "SnowflakeSqlDialect"]
