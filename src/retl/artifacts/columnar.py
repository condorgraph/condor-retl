from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, TypeAlias

import pyarrow as pa

from retl.artifacts.batches import ColumnarBatchBoundary

ColumnarFormat: TypeAlias = Literal["arrow_ipc", "arrow_record_batches"]
ArtifactStorage: TypeAlias = Literal["deferred", "memory", "local_file"]


@dataclass(frozen=True)
class ColumnarArtifactRef:
    format: ColumnarFormat
    storage: ArtifactStorage = "deferred"
    batch_boundary: ColumnarBatchBoundary = field(default_factory=ColumnarBatchBoundary)
    schema_fingerprint: str | None = None
    row_count: int | None = None
    batch_count: int | None = None
    column_names: tuple[str, ...] = ()
    uri: str | None = None
    byte_size: int | None = None
    checksum_sha256: str | None = None
    table: pa.Table | None = field(default=None, repr=False, compare=False)

    @property
    def batch_policy(self) -> ColumnarBatchBoundary:
        return self.batch_boundary


def schema_fingerprint(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def sha256_checksum(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "ArtifactStorage",
    "ColumnarBatchBoundary",
    "ColumnarArtifactRef",
    "ColumnarFormat",
    "schema_fingerprint",
    "sha256_checksum",
]
