from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnarBatchBoundary:
    max_rows: int | None = None
    max_bytes: int | None = None

    def __post_init__(self) -> None:
        if self.max_rows is not None and self.max_rows <= 0:
            raise ValueError("`max_rows` must be greater than 0 when provided.")
        if self.max_bytes is not None and self.max_bytes <= 0:
            raise ValueError("`max_bytes` must be greater than 0 when provided.")


ColumnarBatchPolicy = ColumnarBatchBoundary


__all__ = ["ColumnarBatchBoundary", "ColumnarBatchPolicy"]
