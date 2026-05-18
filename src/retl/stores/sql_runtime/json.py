from __future__ import annotations

import json
from typing import Any

from retl.errors import DeclarationValidationError


def to_json(value: object, field_name: str) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except TypeError as exc:
        raise DeclarationValidationError(
            f"ordered work `{field_name}` must be JSON-serializable."
        ) from exc


def from_json(value: str) -> Any:
    return json.loads(value)


def report_json(value: object, field_name: str) -> str:
    to_dict = getattr(value, "to_dict", None)
    if not callable(to_dict):
        raise DeclarationValidationError(f"`{field_name}` must expose to_dict().")
    try:
        return json.dumps(to_dict(), sort_keys=True, default=str)
    except TypeError as exc:
        raise DeclarationValidationError(f"`{field_name}` must be JSON-serializable.") from exc


__all__ = [
    "from_json",
    "report_json",
    "to_json",
]
