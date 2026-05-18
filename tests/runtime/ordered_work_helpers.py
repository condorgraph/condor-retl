from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Mapping
from datetime import date, datetime, time
from decimal import Decimal

import pyarrow as pa  # type: ignore[import-untyped]

from retl.backends.duckdb import DuckDBRuntimeStore
from retl.collect_identity import is_uuidv7
from retl.errors import DeclarationValidationError
from retl.stores.contracts import OrderedWorkInput, OrderedWorkRow


def append_ordered_work(
    store: DuckDBRuntimeStore,
    rows: Iterable[OrderedWorkInput],
) -> tuple[OrderedWorkRow, ...]:
    prepared = _prepare_ordered_work_rows(store, rows)
    if not prepared:
        return ()

    table = pa.Table.from_pydict(
        {
            "work_id": [row.work_id for row in prepared],
            "collect_id": [row.collect_id for row in prepared],
            "sequence_order": [row.sequence_order for row in prepared],
            "family": [row.family for row in prepared],
            "kind": [row.kind for row in prepared],
            "declaration_name": [row.declaration_name for row in prepared],
            "key_json": [_to_json(row.key, "key") for row in prepared],
            "target_json": [
                _to_json(row.target, "target") if row.target is not None else None
                for row in prepared
            ],
            "identifiers_json": [_to_json(row.identifiers, "identifiers") for row in prepared],
            "payload_json": [_to_json(row.payload, "payload") for row in prepared],
        }
    )
    view_name = f"test_ordered_work_{uuid.uuid4().hex}"
    connection = store._connection
    connection.register(view_name, table)
    connection.execute("begin transaction")
    try:
        connection.execute(
            f"""
            insert into {store.schema}.ordered_work (
                work_id,
                collect_id,
                sequence_order,
                family,
                kind,
                declaration_name,
                key_json,
                target_json,
                identifiers_json,
                payload_json
            )
            select
                work_id,
                collect_id,
                sequence_order,
                family,
                kind,
                declaration_name,
                key_json,
                target_json,
                identifiers_json,
                payload_json
            from {view_name}
            """
        )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise
    finally:
        connection.unregister(view_name)
    return prepared


def _prepare_ordered_work_rows(
    store: DuckDBRuntimeStore,
    rows: Iterable[OrderedWorkInput],
) -> tuple[OrderedWorkRow, ...]:
    prepared: list[OrderedWorkRow] = []
    next_orders: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, OrderedWorkInput):
            raise DeclarationValidationError("ordered work rows must be OrderedWorkInput values.")
        _validate_ordered_work_input(row)
        if row.collect_id not in next_orders:
            next_orders[row.collect_id] = store._next_sequence_order(row.collect_id)
        sequence_order = next_orders[row.collect_id]
        next_orders[row.collect_id] = sequence_order + 1
        prepared.append(
            OrderedWorkRow(
                work_id=str(uuid.uuid4()),
                collect_id=row.collect_id,
                sequence_order=sequence_order,
                family=row.family,
                kind=row.kind,
                declaration_name=row.declaration_name,
                key=dict(row.key),
                target=dict(row.target) if row.target is not None else None,
                identifiers=tuple(dict(identifier) for identifier in row.identifiers),
                payload=dict(row.payload),
            )
        )
    return tuple(prepared)


def _validate_ordered_work_input(row: OrderedWorkInput) -> None:
    if not is_uuidv7(row.collect_id):
        raise DeclarationValidationError("ordered work `collect_id` must be a UUIDv7 string.")
    if row.family != "state":
        raise DeclarationValidationError(
            "ordered work test helper is State-only; Event runtime uses source keyset range "
            "staging instead of Event ordered_work."
        )
    if row.kind not in ("upsert", "remove", "import"):
        raise DeclarationValidationError(
            "ordered work `kind` must be one of 'upsert', 'remove', or 'import'."
        )
    if row.family == "state" and row.kind == "import":
        raise DeclarationValidationError("State ordered work cannot use kind 'import'.")
    if not isinstance(row.declaration_name, str) or not row.declaration_name.strip():
        raise DeclarationValidationError("`declaration_name` must be a non-empty string.")


def _to_json(value: object, field_name: str) -> str:
    try:
        return json.dumps(
            _json_compatible(value),
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise DeclarationValidationError(
            f"ordered work `{field_name}` must be JSON serializable."
        ) from exc


def _json_compatible(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_compatible(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date | time):
        return value.isoformat()
    if isinstance(value, Decimal | uuid.UUID):
        return str(value)
    return value
