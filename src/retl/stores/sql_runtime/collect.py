from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext, suppress
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Protocol, cast, runtime_checkable

from sqlglot import exp

from retl.collect_identity import new_collect_id
from retl.declarations.provenance import declaration_metadata
from retl.errors import DeclarationValidationError
from retl.sources.sql import SqlDialect as SourceSqlDialect
from retl.sources.sql import compile_keyset_scan_query
from retl.sql import (
    CompiledSql,
    SqlParamAllocator,
    alias_column,
    column,
    count_read,
    filtered_delete,
    identifier,
    max_read,
    render_sql,
    row_read,
    sql_and,
    sql_eq_param,
    sql_order,
)
from retl.stores.contracts import (
    CanonicalKeyScalar,
    EventKeysetScanPosition,
    EventProductionResult,
    EventSourceCursor,
    EventSourceWindowHandle,
    PendingWorkPage,
    SqlRelationSpace,
    StateProductionResult,
    StateSnapshotHandle,
    sql_relation_space_to_jsonable,
)
from retl.stores.sql_runtime import arrow as arrow_helpers
from retl.stores.sql_runtime import json as json_helpers
from retl.stores.sql_runtime import provenance as provenance_store
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.errors import RuntimeStoreError

_STATE_COLLECT_TEMP = "retl_state_collect_snapshot"
_EVENT_COLLECT_TEMP = "retl_event_collect_window"
_SOURCE_ROWS_ALIAS = "source_rows"


@runtime_checkable
class _CollectDialect(Protocol):
    name: str
    sqlglot_dialect: str

    def source_schema_context(
        self,
        connection: object,
        source_space: SqlRelationSpace,
    ) -> AbstractContextManager[None]: ...

    @property
    def executable_collect_runtime_store_label(self) -> str: ...

    def sql_literal(self, value: str) -> str: ...

    def json_object_sql(self, entries: Mapping[str, str]) -> str: ...

    def json_array_sql(self, values: list[str]) -> str: ...

    def json_concat_arrays_sql(self, arrays: list[str]) -> str: ...

    def json_parse_sql(self, value_sql: str) -> str: ...

    def json_serialize_sql(self, value_sql: str) -> str: ...

    def identifier_scalar_array_sql(self, *, identifier_type: str, value_sql: str) -> str: ...

    def identifier_list_array_sql(self, *, identifier_type: str, values_sql: str) -> str: ...

    def cast_to_text_sql(self, value_sql: str) -> str: ...

    def concat_sql(self, parts: list[str]) -> str: ...

    def sha256_sql(self, value_sql: str) -> str: ...


@dataclass(frozen=True)
class StateProjectionSql:
    key_json: str
    target_json: str
    identifiers_json: str
    payload_json: str
    identity_json: str


@dataclass(frozen=True)
class EventProjectionSql:
    key_json: str
    occurred_at_sql: str
    identifiers_json: str
    payload_json: str
    identity_json: str
    fingerprint_json: str


def produce_state_collect(
    context: SqlRuntimeContext,
    *,
    declaration: object,
    snapshot: StateSnapshotHandle,
) -> StateProductionResult:
    from retl.declarations import State

    if not isinstance(declaration, State):
        raise DeclarationValidationError("State production requires a State declaration.")
    if not isinstance(snapshot, StateSnapshotHandle):
        raise DeclarationValidationError("State production requires a StateSnapshotHandle.")
    validate_collect_source_space(context, snapshot.backend, snapshot.source_space)
    if snapshot.source_name != declaration.source.name:
        raise DeclarationValidationError(
            "State snapshot source does not match the State declaration source."
        )

    metadata = declaration_metadata(
        declaration,
        source_backend=snapshot.backend,
        source_location=sql_relation_space_to_jsonable(snapshot.source_space),
    )
    provenance_store.register_declaration(context, metadata)
    source_identity_json = json_helpers.to_json(snapshot.source_identity, "source_identity")
    compiled_snapshot = compile_state_collect_snapshot_query(
        context,
        declaration=declaration,
        snapshot=snapshot,
    )
    collect_id = ""
    current_row_count = 0
    upsert_count = 0
    remove_count = 0

    try:
        context.drop_temp_table(_STATE_COLLECT_TEMP)
        with source_schema_context(context, snapshot.source_space):
            context.connection.execute(compiled_snapshot.sql, compiled_snapshot.params)
            context.begin_transaction()
            duplicate = context.connection.execute(
                f"""
                select identity_json
                from {context.render_temp_relation(_STATE_COLLECT_TEMP)}
                group by identity_json
                having count(*) > 1
                limit 1
                """
            ).fetchone()
            if duplicate is not None:
                raise DeclarationValidationError(
                    "State collect produced duplicate State identity rows."
                )

            collect_id = allocate_collect_id(context)
            remove_count = insert_state_remove_work(
                context,
                declaration_name=declaration.name,
                declaration_version_id=metadata.declaration_version_id,
                collect_id=collect_id,
                source_name=snapshot.source_name,
                source_identity_json=source_identity_json,
                sequence_order_offset=0,
            )
            upsert_count = insert_state_upsert_work(
                context,
                declaration_name=declaration.name,
                declaration_version_id=metadata.declaration_version_id,
                collect_id=collect_id,
                source_name=snapshot.source_name,
                source_identity_json=source_identity_json,
                sequence_order_offset=remove_count,
            )
            replace_state_current(
                context,
                declaration_name=declaration.name,
                declaration_version_id=metadata.declaration_version_id,
                source_name=snapshot.source_name,
                source_identity_json=source_identity_json,
                collect_id=collect_id,
            )
            count_query = count_read(context.temp_relation(_STATE_COLLECT_TEMP))
            count_compiled = render_sql(count_query, dialect=context.dialect)
            count_record = context.connection.execute(
                count_compiled.sql, count_compiled.params
            ).fetchone()
            current_row_count = int(count_record[0]) if count_record is not None else 0
            context.commit()
            context.drop_temp_table(_STATE_COLLECT_TEMP)
    except Exception:
        with suppress(Exception):
            context.rollback()
        context.drop_temp_table(_STATE_COLLECT_TEMP)
        raise

    return StateProductionResult(
        collect_id=collect_id,
        declaration_name=declaration.name,
        source_name=snapshot.source_name,
        current_row_count=current_row_count,
        work_row_count=upsert_count + remove_count,
        upsert_count=upsert_count,
        remove_count=remove_count,
    )


def produce_event_collect(
    context: SqlRuntimeContext,
    *,
    declaration: object,
    window: EventSourceWindowHandle,
) -> EventProductionResult:
    from retl.declarations import Event

    if not isinstance(declaration, Event):
        raise DeclarationValidationError("Event production requires an Event declaration.")
    if not isinstance(window, EventSourceWindowHandle):
        raise DeclarationValidationError("Event production requires an EventSourceWindowHandle.")
    validate_collect_source_space(context, window.backend, window.source_space)
    if window.source_name != declaration.source.name:
        raise DeclarationValidationError(
            "Event source window does not match the Event declaration source."
        )

    metadata = declaration_metadata(
        declaration,
        source_backend=window.backend,
        source_location=sql_relation_space_to_jsonable(window.source_space),
    )
    provenance_store.register_declaration(context, metadata)
    compiled_window = compile_event_collect_window_query(
        context,
        declaration=declaration,
        window=window,
    )
    collect_id = ""
    window_row_count = 0
    work_row_count = 0
    scan_upper_bound: EventKeysetScanPosition | None = None

    try:
        context.drop_temp_table(_EVENT_COLLECT_TEMP)
        with source_schema_context(context, window.source_space):
            context.connection.execute(compiled_window.sql, compiled_window.params)
            context.begin_transaction()
            duplicate = context.connection.execute(
                f"""
                select identity_json
                from {context.render_temp_relation(_EVENT_COLLECT_TEMP)}
                group by identity_json
                having count(*) > 1
                limit 1
                """
            ).fetchone()
            if duplicate is not None:
                raise DeclarationValidationError(
                    "Event collect produced duplicate Event identity rows."
                )

            checkpoint = declaration.source.checkpoint
            if checkpoint is None:
                raise DeclarationValidationError("Event declaration requires checkpoint types.")
            collect_id = allocate_collect_id(context)
            count_query = count_read(context.temp_relation(_EVENT_COLLECT_TEMP))
            count_compiled = render_sql(count_query, dialect=context.dialect)
            count_record = context.connection.execute(
                count_compiled.sql, count_compiled.params
            ).fetchone()
            window_row_count = int(count_record[0]) if count_record is not None else 0
            scan_upper_bound = event_scan_upper_bound_from_window(
                context,
                cursor_kind=checkpoint["cursor_type"],
                primary_key_kind=checkpoint["primary_key_type"],
            )
            context.commit()
            context.drop_temp_table(_EVENT_COLLECT_TEMP)
    except Exception:
        with suppress(Exception):
            context.rollback()
        context.drop_temp_table(_EVENT_COLLECT_TEMP)
        raise

    return EventProductionResult(
        collect_id=collect_id,
        declaration_name=declaration.name,
        source_name=window.source_name,
        scan_after=window.scan_after,
        scan_upper_bound=scan_upper_bound,
        window_row_count=window_row_count,
        work_row_count=work_row_count,
        duplicate_risk_count=0,
    )


def read_event_source_window(
    context: SqlRuntimeContext,
    *,
    declaration: object,
    window: EventSourceWindowHandle,
    max_rows: int,
) -> PendingWorkPage:
    from retl.declarations import Event

    if not isinstance(declaration, Event):
        raise DeclarationValidationError("Event source replay requires an Event declaration.")
    if not isinstance(window, EventSourceWindowHandle):
        raise DeclarationValidationError("Event source replay requires an EventSourceWindowHandle.")
    validation_helpers.validate_max_rows(max_rows)
    validate_collect_source_space(context, window.backend, window.source_space)
    checkpoint = declaration.source.checkpoint
    if checkpoint is None:
        raise DeclarationValidationError("Event declaration requires checkpoint types.")
    metadata = declaration_metadata(
        declaration,
        source_backend=window.backend,
        source_location=sql_relation_space_to_jsonable(window.source_space),
    )
    provenance_store.register_declaration(context, metadata)
    compiled_window = compile_event_collect_window_query(
        context,
        declaration=declaration,
        window=window,
    )
    collect_id = allocate_collect_id(context)
    try:
        context.drop_temp_table(_EVENT_COLLECT_TEMP)
        with source_schema_context(context, window.source_space):
            context.connection.execute(compiled_window.sql, compiled_window.params)
            payload = _event_source_window_payload(
                context,
                declaration_name=declaration.name,
                declaration_version_id=metadata.declaration_version_id,
                collect_id=collect_id,
                cursor_kind=checkpoint["cursor_type"],
                primary_key_kind=checkpoint["primary_key_type"],
                lower_bound=window.scan_after,
                limit=max_rows + 1,
            )
        context.drop_temp_table(_EVENT_COLLECT_TEMP)
    except Exception:
        context.drop_temp_table(_EVENT_COLLECT_TEMP)
        raise

    included = payload.slice(0, max_rows) if payload.num_rows > max_rows else payload
    next_cursor = None
    if payload.num_rows > max_rows and included.num_rows:
        next_cursor = EventSourceCursor(
            position=_event_position_from_payload(
                included,
                included.num_rows - 1,
                cursor_kind=checkpoint["cursor_type"],
                primary_key_kind=checkpoint["primary_key_type"],
            )
        )
    return PendingWorkPage(
        payload=included,
        row_count=included.num_rows,
        first_collect_id=arrow_helpers.first_string_value(included, "collect_id"),
        last_collect_id=arrow_helpers.last_string_value(included, "collect_id"),
        first_sequence_order=arrow_helpers.first_int_value(included, "sequence_order"),
        last_sequence_order=arrow_helpers.last_int_value(included, "sequence_order"),
        next_cursor=next_cursor,
    )


def compile_state_collect_snapshot_query(
    context: SqlRuntimeContext,
    *,
    declaration: object,
    snapshot: StateSnapshotHandle,
) -> CompiledSql:
    projection = state_projection_sql(context, declaration)
    source_query = snapshot.query.rstrip().rstrip(";")
    dialect = collect_dialect(context)
    fingerprint_sql = dialect.sha256_sql(
        dialect.json_serialize_sql(
            dialect.json_object_sql(
                {
                    "identifiers": dialect.json_parse_sql(projection.identifiers_json),
                    "key": dialect.json_parse_sql(projection.key_json),
                    "payload": dialect.json_parse_sql(projection.payload_json),
                    "target": (
                        f"case when {projection.target_json} is null then null "
                        f"else {dialect.json_parse_sql(projection.target_json)} end"
                    ),
                }
            )
        )
    )
    query_sql = f"""
        select
            {projection.key_json} as key_json,
            {projection.target_json} as target_json,
            {projection.identifiers_json} as identifiers_json,
            {projection.payload_json} as payload_json,
            {projection.identity_json} as identity_json,
            {fingerprint_sql} as fingerprint
        from ({source_query}) as source_rows
    """
    return CompiledSql(sql=context.create_temp_table_as_sql(_STATE_COLLECT_TEMP, query_sql))


def compile_event_collect_window_query(
    context: SqlRuntimeContext,
    *,
    declaration: object,
    window: EventSourceWindowHandle,
) -> CompiledSql:
    projection = event_projection_sql(context, declaration)
    compiled = compile_keyset_scan_query(
        window.query,
        cursor_column=window.cursor_column,
        primary_key_column=window.primary_key_column,
        scan_after=window.scan_after,
        scan_through=window.scan_through,
        dialect=source_sql_dialect(context),
        limit=window.limit,
    )
    query_sql = f"""
        select
            {projection.key_json} as key_json,
            {projection.occurred_at_sql} as occurred_at,
            {projection.identifiers_json} as identifiers_json,
            {projection.payload_json} as payload_json,
            {projection.identity_json} as identity_json,
            {collect_dialect(context).sha256_sql(projection.fingerprint_json)} as fingerprint,
            {source_column_sql(context, window.cursor_column, "Event cursor")}
                as {sql_identifier(context, "retl_cursor")},
            {source_column_sql(context, window.primary_key_column, "Event primary key")}
                as {sql_identifier(context, "retl_primary_key")}
        from ({compiled.sql}) as source_rows
    """
    return CompiledSql(
        sql=context.create_temp_table_as_sql(_EVENT_COLLECT_TEMP, query_sql),
        params=compiled.params,
    )


def validate_collect_source_space(
    context: SqlRuntimeContext,
    backend: str,
    source_space: SqlRelationSpace,
) -> None:
    if context.collect_placement is None:
        raise RuntimeStoreError(
            f"{context.dialect.name} executable collect requires a "
            f"{collect_dialect(context).executable_collect_runtime_store_label}."
        )
    if not isinstance(source_space, SqlRelationSpace):
        raise DeclarationValidationError("SQL source space must be a SqlRelationSpace.")
    if source_space.access != "read_only":
        raise DeclarationValidationError("SQL source space access must be read_only.")
    if backend != context.dialect.name:
        raise RuntimeStoreError(
            f"{context.dialect.name} runtime store cannot consume `{backend}` source handles."
        )
    if source_space.backend_name != context.dialect.name:
        raise RuntimeStoreError(
            f"{context.dialect.name} runtime store cannot consume "
            f"`{source_space.backend_name}` source spaces."
        )
    if source_space != context.collect_placement.source:
        raise RuntimeStoreError(
            f"{context.dialect.name} executable collect source space must match the runtime "
            "store backend."
        )


def validate_duckdb_collect_source_space(
    context: SqlRuntimeContext,
    source_space: SqlRelationSpace,
) -> None:
    validate_collect_source_space(context, "duckdb", source_space)


def source_schema_context(
    context: SqlRuntimeContext,
    source_space: SqlRelationSpace,
) -> AbstractContextManager[None]:
    if (
        source_space.database.casefold(),
        source_space.schema.casefold(),
    ) == (
        context.runtime_space.database.casefold(),
        context.runtime_space.schema.casefold(),
    ):
        return nullcontext()
    return collect_dialect(context).source_schema_context(context.connection, source_space)


def _event_source_window_payload(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    declaration_version_id: str,
    collect_id: str,
    cursor_kind: str,
    primary_key_kind: str,
    lower_bound: EventKeysetScanPosition | None,
    limit: int,
):
    event_collect_window = context.render_temp_relation(_EVENT_COLLECT_TEMP)
    dialect = collect_dialect(context)
    params = context.new_params()
    collect_id_work_param = add_param(context, params, collect_id)
    collect_id_row_param = add_param(context, params, collect_id)
    declaration_name_param = add_param(context, params, declaration_name)
    declaration_version_id_param = add_param(context, params, declaration_version_id)
    work_id_sql = dialect.sha256_sql(
        dialect.concat_sql(
            [
                dialect.sql_literal("event|import|"),
                dialect.cast_to_text_sql(collect_id_work_param),
                dialect.sql_literal("|"),
                "identity_json",
                dialect.sql_literal("|"),
                "fingerprint",
            ]
        )
    )
    event_cursor_value_sql = event_checkpoint_value_sql(context, "retl_cursor", cursor_kind)
    event_primary_key_value_sql = event_checkpoint_value_sql(
        context,
        "retl_primary_key",
        primary_key_kind,
    )
    lower_cursor_sql = _event_bound_value_sql(context, params, lower_bound, "cursor")
    lower_primary_key_sql = _event_bound_value_sql(context, params, lower_bound, "primary_key")
    limit_param = add_param(context, params, limit)
    cursor_order = temp_column_sql(context, "retl_cursor")
    primary_key_order = temp_column_sql(context, "retl_primary_key")
    return arrow_helpers.fetch_bounded_record_batch(
        context.connection.execute(
            f"""
            select
                {work_id_sql} as work_id,
                {collect_id_row_param} as collect_id,
                row_number() over (
                    order by {cursor_order}, {primary_key_order}, identity_json
                ) - 1 as sequence_order,
                'event' as family,
                'import' as kind,
                {declaration_name_param} as declaration_name,
                {declaration_version_id_param} as declaration_version_id,
                key_json,
                {_text_null_sql(dialect)} as target_json,
                identifiers_json,
                payload_json,
                {dialect.cast_to_text_sql("occurred_at")} as event_occurred_at,
                {event_cursor_value_sql} as event_cursor_value,
                {event_primary_key_value_sql} as event_primary_key_value,
                {lower_cursor_sql} as event_lower_cursor_value,
                {lower_primary_key_sql} as event_lower_primary_key_value,
                identity_json as event_identity
            from {event_collect_window}
            order by {cursor_order}, {primary_key_order}, identity_json
            limit {limit_param}
            """,
            params.params,
        ),
        row_limit=limit,
    )


def _event_bound_value_sql(
    context: SqlRuntimeContext,
    params: SqlParamAllocator,
    position: EventKeysetScanPosition | None,
    field_name: str,
) -> str:
    if position is None:
        return _text_null_sql(collect_dialect(context))
    scalar = position.cursor_value if field_name == "cursor" else position.primary_key_value
    return add_param(context, params, None if scalar.value is None else str(scalar.value))


def _event_position_from_payload(
    payload: object,
    index: int,
    *,
    cursor_kind: str,
    primary_key_kind: str,
) -> EventKeysetScanPosition:
    cursor_value = arrow_helpers.string_value(payload, "event_cursor_value", index)
    primary_key_value = arrow_helpers.string_value(payload, "event_primary_key_value", index)
    if cursor_value is None or primary_key_value is None:
        raise RuntimeStoreError("Event source replay cursor metadata is missing.")
    return EventKeysetScanPosition(
        cursor_value=canonical_key_scalar_from_declared_type(cursor_value, cursor_kind),
        primary_key_value=canonical_key_scalar_from_declared_type(
            primary_key_value,
            primary_key_kind,
        ),
    )


def event_scan_upper_bound_from_window(
    context: SqlRuntimeContext,
    *,
    cursor_kind: str,
    primary_key_kind: str,
) -> EventKeysetScanPosition | None:
    query = row_read(
        context.temp_relation(_EVENT_COLLECT_TEMP),
        ("retl_cursor", "retl_primary_key"),
        order_by=(sql_order("retl_cursor", desc=True), sql_order("retl_primary_key", desc=True)),
        limit=exp.Literal.number(1),
    )
    compiled = render_sql(query, dialect=context.sqlglot_dialect)
    record = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if record is None:
        return None
    return EventKeysetScanPosition(
        cursor_value=canonical_key_scalar_from_declared_type(record[0], cursor_kind),
        primary_key_value=canonical_key_scalar_from_declared_type(record[1], primary_key_kind),
    )


def insert_state_upsert_work(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    declaration_version_id: str,
    collect_id: str,
    source_name: str,
    source_identity_json: str,
    sequence_order_offset: int,
) -> int:
    ordered_work = context.render_runtime_relation("ordered_work")
    state_current = context.render_runtime_relation("state_current")
    state_collect_snapshot = context.render_temp_relation(_STATE_COLLECT_TEMP)
    dialect = collect_dialect(context)
    params = context.new_params()
    collect_id_work_param = add_param(context, params, collect_id)
    collect_id_row_param = add_param(context, params, collect_id)
    declaration_name_param = add_param(context, params, declaration_name)
    declaration_version_id_param = add_param(context, params, declaration_version_id)
    join_declaration_name_param = add_param(context, params, declaration_name)
    join_source_name_param = add_param(context, params, source_name)
    work_id_sql = dialect.sha256_sql(
        dialect.concat_sql(
            [
                dialect.sql_literal("state|upsert|"),
                dialect.cast_to_text_sql(collect_id_work_param),
                dialect.sql_literal("|"),
                "s.identity_json",
                dialect.sql_literal("|"),
                "s.fingerprint",
            ]
        )
    )
    context.connection.execute(
        f"""
        insert into {ordered_work} (
            work_id,
            collect_id,
            sequence_order,
            family,
            kind,
            declaration_name,
            declaration_version_id,
            key_json,
            target_json,
            identifiers_json,
            payload_json
        )
        select
            {work_id_sql} as work_id,
            {collect_id_row_param} as collect_id,
            {sequence_order_offset}
                + row_number() over (order by s.target_json, s.identity_json) - 1
                as sequence_order,
            'state' as family,
            'upsert' as kind,
            {declaration_name_param} as declaration_name,
            {declaration_version_id_param} as declaration_version_id,
            s.key_json,
            s.target_json,
            s.identifiers_json,
            s.payload_json
        from {state_collect_snapshot} s
        left join {state_current} c
          on c.declaration_name = {join_declaration_name_param}
         and c.source_name = {join_source_name_param}
         and c.identity_json = s.identity_json
        where c.identity_json is null
           or c.fingerprint <> s.fingerprint
        order by s.target_json, s.identity_json
        """,
        params.params,
    )
    count_params = context.new_params()
    count_query = count_read(
        context.runtime_relation("ordered_work"),
        where=sql_and(
            sql_eq_param("collect_id", collect_id, params=count_params),
            sql_eq_param("family", "state", params=count_params),
            sql_eq_param("kind", "upsert", params=count_params),
            sql_eq_param("declaration_name", declaration_name, params=count_params),
        ),
    )
    count_compiled = render_sql(count_query, dialect=context.dialect, params=count_params)
    return int(context.connection.execute(count_compiled.sql, count_compiled.params).fetchone()[0])


def insert_state_remove_work(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    declaration_version_id: str,
    collect_id: str,
    source_name: str,
    source_identity_json: str,
    sequence_order_offset: int,
) -> int:
    ordered_work = context.render_runtime_relation("ordered_work")
    state_current = context.render_runtime_relation("state_current")
    state_collect_snapshot = context.render_temp_relation(_STATE_COLLECT_TEMP)
    dialect = collect_dialect(context)
    params = context.new_params()
    collect_id_work_param = add_param(context, params, collect_id)
    collect_id_row_param = add_param(context, params, collect_id)
    declaration_name_param = add_param(context, params, declaration_name)
    declaration_version_id_param = add_param(context, params, declaration_version_id)
    where_declaration_name_param = add_param(context, params, declaration_name)
    where_source_name_param = add_param(context, params, source_name)
    work_id_sql = dialect.sha256_sql(
        dialect.concat_sql(
            [
                dialect.sql_literal("state|remove|"),
                dialect.cast_to_text_sql(collect_id_work_param),
                dialect.sql_literal("|"),
                "c.identity_json",
                dialect.sql_literal("|"),
                "c.fingerprint",
            ]
        )
    )
    context.connection.execute(
        f"""
        insert into {ordered_work} (
            work_id,
            collect_id,
            sequence_order,
            family,
            kind,
            declaration_name,
            declaration_version_id,
            key_json,
            target_json,
            identifiers_json,
            payload_json
        )
        select
            {work_id_sql} as work_id,
            {collect_id_row_param} as collect_id,
            {sequence_order_offset}
                + row_number() over (order by c.target_json, c.identity_json) - 1
                as sequence_order,
            'state' as family,
            'remove' as kind,
            {declaration_name_param} as declaration_name,
            {declaration_version_id_param} as declaration_version_id,
            c.key_json,
            c.target_json,
            c.identifiers_json,
            c.payload_json
        from {state_current} c
        left join {state_collect_snapshot} s
          on s.identity_json = c.identity_json
        where c.declaration_name = {where_declaration_name_param}
          and c.source_name = {where_source_name_param}
          and s.identity_json is null
        order by c.target_json, c.identity_json
        """,
        params.params,
    )
    count_params = context.new_params()
    count_query = count_read(
        context.runtime_relation("ordered_work"),
        where=sql_and(
            sql_eq_param("collect_id", collect_id, params=count_params),
            sql_eq_param("family", "state", params=count_params),
            sql_eq_param("kind", "remove", params=count_params),
            sql_eq_param("declaration_name", declaration_name, params=count_params),
        ),
    )
    count_compiled = render_sql(count_query, dialect=context.dialect, params=count_params)
    return int(context.connection.execute(count_compiled.sql, count_compiled.params).fetchone()[0])


def replace_state_current(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    declaration_version_id: str,
    source_name: str,
    source_identity_json: str,
    collect_id: str,
) -> None:
    state_current = context.render_runtime_relation("state_current")
    state_collect_snapshot = context.render_temp_relation(_STATE_COLLECT_TEMP)
    delete_params = context.new_params()
    delete_query = filtered_delete(
        context.runtime_relation("state_current"),
        where=exp.and_(
            sql_eq_param("declaration_name", declaration_name, params=delete_params),
            sql_eq_param("source_name", source_name, params=delete_params),
        ),
    )
    delete_compiled = render_sql(delete_query, dialect=context.dialect, params=delete_params)
    context.connection.execute(delete_compiled.sql, delete_compiled.params)
    insert_params = context.new_params()
    declaration_name_param = add_param(context, insert_params, declaration_name)
    declaration_version_id_param = add_param(context, insert_params, declaration_version_id)
    source_name_param = add_param(context, insert_params, source_name)
    source_identity_param = add_param(context, insert_params, source_identity_json)
    collect_id_param = add_param(context, insert_params, collect_id)
    context.connection.execute(
        f"""
        insert into {state_current} (
            declaration_name,
            declaration_version_id,
            source_name,
            source_identity_json,
            identity_json,
            key_json,
            target_json,
            identifiers_json,
            payload_json,
            fingerprint,
            collect_id
        )
        select
            {declaration_name_param} as declaration_name,
            {declaration_version_id_param} as declaration_version_id,
            {source_name_param} as source_name,
            {source_identity_param} as source_identity_json,
            identity_json,
            key_json,
            target_json,
            identifiers_json,
            payload_json,
            fingerprint,
            {collect_id_param} as collect_id
        from {state_collect_snapshot}
        order by identity_json
        """,
        insert_params.params,
    )


def allocate_collect_id(context: SqlRuntimeContext) -> str:
    _ = context
    return new_collect_id()


def next_sequence_order(context: SqlRuntimeContext, collect_id: str) -> int:
    params = context.new_params()
    query = max_read(
        context.runtime_relation("ordered_work"),
        "sequence_order",
        where=sql_eq_param("collect_id", collect_id, params=params),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    record = context.connection.execute(
        compiled.sql,
        compiled.params,
    ).fetchone()
    if record is None:
        raise RuntimeStoreError(f"{context.dialect.name} did not return a sequence order.")
    max_sequence_order = record[0]
    return 0 if max_sequence_order is None else int(max_sequence_order) + 1


def state_projection_sql(context: SqlRuntimeContext, declaration: object) -> StateProjectionSql:
    from retl.declarations import State, StaticTarget

    if not isinstance(declaration, State):
        raise DeclarationValidationError("State projection requires a State declaration.")
    dialect = collect_dialect(context)
    key_json = dialect.json_serialize_sql(
        dialect.json_object_sql(
            {
                name: source_column_sql(context, column_name, f"State key `{name}`")
                for name, column_name in sorted(declaration.key.items())
            }
        )
    )
    if declaration.target is None:
        target_json = "null"
    elif isinstance(declaration.target, StaticTarget):
        target_json = dialect.json_serialize_sql(
            dialect.json_object_sql({"value": dialect.sql_literal(declaration.target.value)})
        )
    else:
        target_json = dialect.json_serialize_sql(
            dialect.json_object_sql(
                {
                    "value": source_column_sql(
                        context,
                        declaration.target,
                        "State target",
                    )
                }
            )
        )
    identifiers_json = _identifiers_json_sql(
        context,
        declaration.identifiers,
        label="State identifier",
    )
    payload_json = dialect.json_serialize_sql(
        dialect.json_object_sql(
            {
                name: source_column_sql(context, column_name, f"State payload `{name}`")
                for name, column_name in sorted(declaration.payload.items())
            }
        )
    )
    identity_json = dialect.json_serialize_sql(
        dialect.json_object_sql(
            {
                "key": dialect.json_parse_sql(key_json),
                "target": (
                    f"case when {target_json} is null then null "
                    f"else {dialect.json_parse_sql(target_json)} end"
                ),
            }
        )
    )
    return StateProjectionSql(
        key_json=key_json,
        target_json=target_json,
        identifiers_json=identifiers_json,
        payload_json=payload_json,
        identity_json=identity_json,
    )


def event_projection_sql(context: SqlRuntimeContext, declaration: object) -> EventProjectionSql:
    from retl.declarations import Event

    if not isinstance(declaration, Event):
        raise DeclarationValidationError("Event projection requires an Event declaration.")
    dialect = collect_dialect(context)
    key_json = dialect.json_serialize_sql(
        dialect.json_object_sql(
            {
                name: source_column_sql(context, column_name, f"Event key `{name}`")
                for name, column_name in sorted(declaration.key.items())
            }
        )
    )
    occurred_at_sql = source_column_sql(context, declaration.occurred_at, "Event occurred_at")
    identifiers_json = _identifiers_json_sql(
        context,
        declaration.identifiers,
        label="Event identifier",
    )
    payload_json = dialect.json_serialize_sql(
        dialect.json_object_sql(
            {
                name: source_column_sql(context, column_name, f"Event payload `{name}`")
                for name, column_name in sorted(declaration.payload.items())
            }
        )
    )
    identity_json = dialect.json_serialize_sql(
        dialect.json_object_sql(
            {
                "key": dialect.json_parse_sql(key_json),
                "occurred_at": occurred_at_sql,
            }
        )
    )
    return EventProjectionSql(
        key_json=key_json,
        occurred_at_sql=occurred_at_sql,
        identifiers_json=identifiers_json,
        payload_json=payload_json,
        identity_json=identity_json,
        fingerprint_json=identity_json,
    )


def _identifiers_json_sql(
    context: SqlRuntimeContext,
    identifiers: object,
    *,
    label: str,
) -> str:
    dialect = collect_dialect(context)
    if not isinstance(identifiers, Sequence) or isinstance(identifiers, str):
        raise DeclarationValidationError(f"{label} mappings must be a sequence.")
    arrays: list[str] = []
    for identifier_mapping in identifiers:
        if not isinstance(identifier_mapping, Mapping):
            raise DeclarationValidationError(f"{label} mappings must be mappings.")
        identifier_type = identifier_mapping.get("type")
        if not isinstance(identifier_type, str) or not identifier_type.strip():
            raise DeclarationValidationError(f"{label} mappings require non-empty `type`.")
        if "value" in identifier_mapping:
            value_column = identifier_mapping["value"]
            if not isinstance(value_column, str):
                raise DeclarationValidationError(f"{label} `value` must be a source column.")
            arrays.append(
                dialect.identifier_scalar_array_sql(
                    identifier_type=identifier_type,
                    value_sql=source_column_sql(context, value_column, f"{label} `value`"),
                )
            )
            continue
        if "values" in identifier_mapping:
            values_column = identifier_mapping["values"]
            if not isinstance(values_column, str):
                raise DeclarationValidationError(f"{label} `values` must be a source column.")
            arrays.append(
                dialect.identifier_list_array_sql(
                    identifier_type=identifier_type,
                    values_sql=source_column_sql(context, values_column, f"{label} `values`"),
                )
            )
            continue
        raise DeclarationValidationError(f"{label} mappings require `value` or `values`.")
    return dialect.json_serialize_sql(dialect.json_concat_arrays_sql(arrays))


def source_column_sql(context: SqlRuntimeContext, value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"{label} must name a source column.")
    validation_helpers.validate_identifier(value, label)
    return alias_column(_SOURCE_ROWS_ALIAS, value).sql(dialect=context.sqlglot_dialect)


def temp_column_sql(context: SqlRuntimeContext, value: str) -> str:
    validation_helpers.validate_identifier(value, "temporary column")
    return column(value).sql(dialect=context.sqlglot_dialect)


def sql_identifier(context: SqlRuntimeContext, value: str) -> str:
    validation_helpers.validate_identifier(value, "SQL identifier")
    return identifier(value).sql(dialect=context.sqlglot_dialect)


def event_checkpoint_value_sql(
    context: SqlRuntimeContext,
    column_name: str,
    scalar_kind: str,
) -> str:
    dialect = collect_dialect(context)
    column_sql = temp_column_sql(context, column_name)
    if scalar_kind not in {"boolean", "integer", "number", "string"}:
        raise DeclarationValidationError("Event checkpoint scalar type is not supported.")
    return dialect.cast_to_text_sql(column_sql)


def canonical_key_scalar_from_declared_type(value: object, scalar_kind: str) -> CanonicalKeyScalar:
    normalized = checkpoint_value(value)
    if scalar_kind == "string":
        return CanonicalKeyScalar.string(str(normalized))
    if scalar_kind == "integer":
        if isinstance(normalized, bool):
            raise DeclarationValidationError("integer Event checkpoint value cannot be boolean.")
        return CanonicalKeyScalar.integer(int(cast(Any, normalized)))
    if scalar_kind == "number":
        return CanonicalKeyScalar.number(float(cast(Any, normalized)))
    if scalar_kind == "boolean":
        if isinstance(normalized, bool):
            return CanonicalKeyScalar.boolean(normalized)
        if isinstance(normalized, str):
            lowered = normalized.casefold()
            if lowered in {"true", "1"}:
                return CanonicalKeyScalar.boolean(True)
            if lowered in {"false", "0"}:
                return CanonicalKeyScalar.boolean(False)
        raise DeclarationValidationError("boolean Event checkpoint value must be boolean.")
    raise DeclarationValidationError("Event checkpoint scalar type is not supported.")


def checkpoint_value(value: object) -> object:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    if isinstance(value, Decimal | uuid.UUID):
        return str(value)
    return value


def source_sql_dialect(context: SqlRuntimeContext) -> SourceSqlDialect:
    return SourceSqlDialect(
        name=context.dialect.sqlglot_dialect,
        identifier_quote=getattr(context.dialect, "_identifier_quote", '"'),
        parameter_style=context.dialect.parameter_style,
    )


def add_param(context: SqlRuntimeContext, params: SqlParamAllocator, value: object) -> str:
    params.add(value)
    return context.dialect.placeholder(len(params.params))


def collect_dialect(context: SqlRuntimeContext) -> _CollectDialect:
    if not isinstance(context.dialect, _CollectDialect):
        raise RuntimeStoreError("SQL runtime collect requires collect dialect capabilities.")
    return cast(_CollectDialect, context.dialect)


def _text_null_sql(dialect: _CollectDialect) -> str:
    renderer = getattr(dialect, "text_null_sql", None)
    if callable(renderer):
        return str(renderer())
    return "null"
