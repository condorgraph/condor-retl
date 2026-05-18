from __future__ import annotations

from dataclasses import dataclass

from retl.artifacts.columnar import ColumnarArtifactRef
from retl.stores.contracts import EventKeysetScanPosition


@dataclass(frozen=True)
class SourceWindowBatchMetadata:
    batch_count: int
    row_count: int
    max_rows: int | None
    max_bytes: int | None


@dataclass(frozen=True)
class SourceWindowMetadata:
    source_name: str
    source_identity: str
    sync_identity: str
    scan_after: EventKeysetScanPosition | None
    scan_upper_bound: EventKeysetScanPosition | None
    dry_run: bool
    row_count: int
    schema_fingerprint: str
    column_names: tuple[str, ...]
    cursor_column: str
    primary_key_column: str
    first_position: EventKeysetScanPosition | None
    last_position: EventKeysetScanPosition | None
    batch_metadata: SourceWindowBatchMetadata
    columnar_artifact: ColumnarArtifactRef


__all__ = ["SourceWindowBatchMetadata", "SourceWindowMetadata"]
