from __future__ import annotations

import uuid

from sqlglot import exp, select

from retl.errors import DeclarationValidationError
from retl.sql import (
    CompiledSql,
    SqlCondition,
    column,
    render_sql,
    row_insert,
    row_read,
    row_write_input,
    sql_alias,
    sql_and,
    sql_eq_param,
    table,
)
from retl.stores.contracts import (
    CanonicalKey,
    CanonicalKeyScalar,
    StateCurrentCursor,
    StateCurrentPage,
    StateCurrentSnapshotScanPosition,
    StateCurrentSummary,
    compare_scan_positions,
)
from retl.stores.sql_runtime import arrow as arrow_helpers
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext


def state_current_summary(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    source_name: str,
) -> StateCurrentSummary:
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    validation_helpers.validate_identity(source_name, "source_name")
    compiled = compile_state_current_summary_read(
        context,
        declaration_name=declaration_name,
        source_name=source_name,
    )
    record = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if record is None or record[0] is None:
        return StateCurrentSummary(
            declaration_name=declaration_name,
            source_name=source_name,
            collect_id=None,
            row_count=0,
        )
    return StateCurrentSummary(
        declaration_name=declaration_name,
        source_name=source_name,
        collect_id=str(record[0]),
        row_count=int(record[1]),
    )


def read_state_current_upserts(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    source_name: str,
    max_rows: int,
    cursor: StateCurrentCursor | None = None,
    position: StateCurrentSnapshotScanPosition | None = None,
) -> StateCurrentPage:
    validation_helpers.validate_identity(declaration_name, "declaration_name")
    validation_helpers.validate_identity(source_name, "source_name")
    validation_helpers.validate_max_rows(max_rows)
    if position is not None:
        validate_state_current_snapshot_position(position)
    if cursor is not None:
        validate_stored_state_current_cursor(
            context,
            declaration_name=declaration_name,
            source_name=source_name,
            cursor=cursor,
        )
        if position is not None and compare_scan_positions(cursor.position, position) < 0:
            raise DeclarationValidationError(
                "State current cursor must be ahead of the committed scan position."
            )
    lower_identity = cursor.identity if cursor is not None else state_current_identity(position)
    compiled = compile_state_current_upserts_read(
        context,
        declaration_name=declaration_name,
        source_name=source_name,
        lower_identity=lower_identity,
        limit=max_rows + 1,
    )
    batch_with_cursor_identity = arrow_helpers.fetch_bounded_record_batch(
        context.connection.execute(compiled.sql, compiled.params),
        row_limit=max_rows + 1,
    )
    payload = batch_with_cursor_identity.slice(0, max_rows)
    row_count = payload.num_rows
    next_cursor = None
    if batch_with_cursor_identity.num_rows > max_rows and row_count:
        cursor_identity = arrow_helpers.string_value(payload, "identity_json", row_count - 1)
        next_cursor = state_current_cursor(
            context,
            declaration_name=declaration_name,
            source_name=source_name,
            identity=cursor_identity,
        )
    return StateCurrentPage(
        payload=payload,
        row_count=row_count,
        collect_id=arrow_helpers.last_string_value(payload, "collect_id"),
        first_collect_id=arrow_helpers.first_string_value(payload, "collect_id"),
        last_collect_id=arrow_helpers.last_string_value(payload, "collect_id"),
        first_sequence_order=arrow_helpers.first_int_value(payload, "sequence_order"),
        last_sequence_order=arrow_helpers.last_int_value(payload, "sequence_order"),
        next_cursor=next_cursor,
    )


def validate_stored_state_current_cursor(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    source_name: str,
    cursor: StateCurrentCursor,
) -> None:
    if not isinstance(cursor, StateCurrentCursor):
        raise DeclarationValidationError("State current cursor must be a StateCurrentCursor.")
    validation_helpers.validate_identity(cursor.token, "state_current_cursor.token")
    if not isinstance(cursor.identity, str) or not cursor.identity.strip():
        raise DeclarationValidationError("State current cursor identity must be non-empty.")
    validate_state_current_snapshot_position(cursor.position)
    if state_current_identity(cursor.position) != cursor.identity:
        raise DeclarationValidationError(
            "State current cursor position does not match cursor identity."
        )
    params = context.new_params()
    token_param = params.add(cursor.token)
    declaration_name_param = params.add(declaration_name)
    source_name_param = params.add(source_name)
    compiled = render_sql(
        select("identity_json")
        .from_(table(context.runtime_relation("state_current_cursors")))
        .where(
            exp.and_(
                column("token").eq(token_param),
                column("declaration_name").eq(declaration_name_param),
                column("source_name").eq(source_name_param),
            )
        ),
        dialect=context.dialect,
        params=params,
    )
    record = context.connection.execute(
        compiled.sql,
        compiled.params,
    ).fetchone()
    if record is None:
        raise DeclarationValidationError("State current cursor was not issued by this store.")
    if str(record[0]) != cursor.identity:
        raise DeclarationValidationError("State current cursor does not match store state.")


def state_current_cursor(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    source_name: str,
    identity: str,
) -> StateCurrentCursor:
    token = str(uuid.uuid4())
    params = context.new_params()
    compiled = render_sql(
        row_insert(
            context.runtime_relation("state_current_cursors"),
            row_write_input(
                (
                    ("token", token),
                    ("declaration_name", declaration_name),
                    ("source_name", source_name),
                    ("identity_json", identity),
                ),
                params=params,
            ),
        ),
        dialect=context.dialect,
        params=params,
    )
    context.connection.execute(compiled.sql, compiled.params)
    return StateCurrentCursor(
        token=token,
        identity=identity,
        position=state_current_snapshot_position(identity),
    )


def compile_state_current_summary_read(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    source_name: str,
) -> CompiledSql:
    params = context.new_params()
    query = row_read(
        context.runtime_relation("state_current"),
        (exp.Max(this=column("collect_id")), exp.Count(this=exp.Star())),
        where=sql_and(
            sql_eq_param("declaration_name", declaration_name, params=params),
            sql_eq_param("source_name", source_name, params=params),
        ),
    )
    return render_sql(query, dialect=context.dialect, params=params)


def compile_state_current_upserts_read(
    context: SqlRuntimeContext,
    *,
    declaration_name: str,
    source_name: str,
    lower_identity: str | None,
    limit: int,
) -> CompiledSql:
    params = context.new_params()
    conditions: list[SqlCondition] = [
        column("declaration_name").eq(params.add(declaration_name)),
        column("source_name").eq(params.add(source_name)),
    ]
    if lower_identity is not None:
        conditions.append(column("identity_json") > params.add(lower_identity))
    query = (
        select(*_state_current_upsert_columns(context))
        .from_(table(context.runtime_relation("state_current")))
        .where(_and_conditions(conditions))
        .order_by(column("identity_json"))
        .limit(params.add(limit))
    )
    return render_sql(query, dialect=context.dialect, params=params)


def _state_current_upsert_columns(context: SqlRuntimeContext) -> tuple[exp.Expression | str, ...]:
    return (
        sql_alias(
            exp.DPipe(
                this=exp.Literal.string("state-current:"),
                expression=column("identity_json"),
            ),
            "work_id",
            quoted=True,
        ),
        column("collect_id"),
        sql_alias(
            exp.Sub(
                this=exp.Window(
                    this=exp.RowNumber(),
                    order=exp.Order(
                        expressions=[exp.Ordered(this=column("identity_json"), nulls_first=None)]
                    ),
                ),
                expression=exp.Literal.number(1),
            ),
            "sequence_order",
            quoted=True,
        ),
        sql_alias(exp.Literal.string("state"), "family", quoted=True),
        sql_alias(exp.Literal.string("upsert"), "kind", quoted=True),
        column("declaration_name"),
        column("declaration_version_id"),
        column("key_json"),
        column("target_json"),
        column("identifiers_json"),
        column("payload_json"),
        column("identity_json"),
    )


def state_current_snapshot_position(identity: str) -> StateCurrentSnapshotScanPosition:
    return StateCurrentSnapshotScanPosition(
        key=CanonicalKey.of(CanonicalKeyScalar.string(identity))
    )


def state_current_identity(position: StateCurrentSnapshotScanPosition | None) -> str | None:
    if position is None:
        return None
    validate_state_current_snapshot_position(position)
    return str(position.key.parts[0].value)


def validate_state_current_snapshot_position(
    position: StateCurrentSnapshotScanPosition,
) -> None:
    if not isinstance(position, StateCurrentSnapshotScanPosition):
        raise DeclarationValidationError(
            "State current scan position must be a StateCurrentSnapshotScanPosition."
        )
    if len(position.key.parts) != 1:
        raise DeclarationValidationError(
            "DuckDB State current reads require a single-part current-snapshot key."
        )
    part = position.key.parts[0]
    if part.kind != "string" or not isinstance(part.value, str) or not part.value.strip():
        raise DeclarationValidationError(
            "DuckDB State current reads require a string current-snapshot key."
        )


def _and_conditions(conditions: list[SqlCondition]) -> SqlCondition:
    if not conditions:
        raise ValueError("SQL condition list must not be empty.")
    condition: SqlCondition = conditions[0]
    for next_condition in conditions[1:]:
        condition = exp.and_(condition, next_condition)
    return condition


__all__ = [
    "compile_state_current_summary_read",
    "compile_state_current_upserts_read",
    "read_state_current_upserts",
    "state_current_cursor",
    "state_current_identity",
    "state_current_snapshot_position",
    "state_current_summary",
    "validate_state_current_snapshot_position",
    "validate_stored_state_current_cursor",
]
