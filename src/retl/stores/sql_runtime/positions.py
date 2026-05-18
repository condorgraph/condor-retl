from __future__ import annotations

import json
from collections.abc import Mapping

from retl.errors import DeclarationValidationError
from retl.stores.contracts import (
    ScanPosition,
    scan_position_from_jsonable,
    scan_position_to_jsonable,
)


def scan_position_to_storage_json(position: ScanPosition) -> str:
    try:
        return json.dumps(
            scan_position_to_jsonable(position),
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise DeclarationValidationError("Destination progress position is invalid.") from exc


def scan_position_from_storage_json(
    value: object,
    *,
    field_name: str,
) -> ScanPosition | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DeclarationValidationError(f"Destination progress `{field_name}` must be JSON.")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise DeclarationValidationError(
            f"Destination progress `{field_name}` must be valid JSON."
        ) from exc
    if not isinstance(decoded, Mapping):
        raise DeclarationValidationError(
            f"Destination progress `{field_name}` must be a scan position object."
        )
    try:
        return scan_position_from_jsonable(decoded)
    except ValueError as exc:
        raise DeclarationValidationError(str(exc)) from exc


__all__ = [
    "scan_position_from_storage_json",
    "scan_position_to_storage_json",
]
