from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from retl.declarations import DestinationBinding, JSONValue
from retl.errors import DeclarationValidationError

DEFAULT_FILE_BATCH_MAX_ROWS = 10_000


@dataclass(frozen=True)
class FileConfig:
    output_dir: Path
    file_batch_max_rows: int = DEFAULT_FILE_BATCH_MAX_ROWS
    create_parent_dirs: bool = True


def file_config(binding: DestinationBinding) -> FileConfig:
    raw_output_dir = binding.config.get("output_dir")
    if not isinstance(raw_output_dir, str) or not raw_output_dir.strip():
        raise DeclarationValidationError("File destination config requires non-empty `output_dir`.")
    raw_file_batch_max_rows = binding.config.get("file_batch_max_rows", DEFAULT_FILE_BATCH_MAX_ROWS)
    file_batch_max_rows = _positive_int(
        raw_file_batch_max_rows,
        field_name="file_batch_max_rows",
    )
    create_parent_dirs = _bool(
        binding.config.get("create_parent_dirs", True),
        field_name="create_parent_dirs",
    )
    return FileConfig(
        output_dir=Path(raw_output_dir).expanduser(),
        file_batch_max_rows=file_batch_max_rows,
        create_parent_dirs=create_parent_dirs,
    )


def compact_json(value: object) -> str:
    return json.dumps(_plain_json(value), sort_keys=True, separators=(",", ":"), default=str)


def _positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, str):
        value = value.strip()
        if not value:
            raise DeclarationValidationError(
                f"File destination config `{field_name}` must be greater than 0."
            )
        try:
            parsed = int(value)
        except ValueError as exc:
            raise DeclarationValidationError(
                f"File destination config `{field_name}` must be an integer."
            ) from exc
        value = parsed
    if not isinstance(value, int) or isinstance(value, bool):
        raise DeclarationValidationError(
            f"File destination config `{field_name}` must be an integer."
        )
    if value <= 0:
        raise DeclarationValidationError(
            f"File destination config `{field_name}` must be greater than 0."
        )
    return value


def _bool(value: object, *, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise DeclarationValidationError(f"File destination config `{field_name}` must be a boolean.")


def _plain_json(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_plain_json(item) for item in value]
    return cast(JSONValue, value)


__all__ = [
    "DEFAULT_FILE_BATCH_MAX_ROWS",
    "FileConfig",
    "compact_json",
    "file_config",
]
