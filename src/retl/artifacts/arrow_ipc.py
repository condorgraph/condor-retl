from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.ipc as ipc

from retl.artifacts.batches import ColumnarBatchBoundary
from retl.artifacts.columnar import ColumnarArtifactRef, schema_fingerprint, sha256_checksum


def write_arrow_ipc_table(
    path: str | Path,
    *,
    table: pa.Table,
    batch_boundary: ColumnarBatchBoundary | None = None,
    batch_policy: ColumnarBatchBoundary | None = None,
) -> ColumnarArtifactRef:
    boundary = batch_policy or batch_boundary or ColumnarBatchBoundary()
    return write_arrow_ipc_batches(
        path,
        batches=table.to_batches(max_chunksize=boundary.max_rows),
        schema=table.schema,
        batch_boundary=boundary,
    )


def write_arrow_ipc_batches(
    path: str | Path,
    *,
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    batch_boundary: ColumnarBatchBoundary | None = None,
    batch_policy: ColumnarBatchBoundary | None = None,
) -> ColumnarArtifactRef:
    boundary = batch_policy or batch_boundary or ColumnarBatchBoundary()
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    row_count = 0
    batch_count = 0
    with target.open("wb") as sink:
        with ipc.new_file(sink, schema) as writer:
            for batch in batches:
                if not isinstance(batch, pa.RecordBatch):
                    raise TypeError("Arrow IPC writes require pyarrow.RecordBatch values.")
                if not batch.schema.equals(schema, check_metadata=True):
                    raise ValueError("Arrow IPC batches must share one schema.")
                writer.write_batch(batch)
                row_count += batch.num_rows
                batch_count += 1

    return ColumnarArtifactRef(
        format="arrow_ipc",
        storage="local_file",
        batch_boundary=boundary,
        schema_fingerprint=schema_fingerprint(schema),
        row_count=row_count,
        batch_count=batch_count,
        column_names=tuple(schema.names),
        uri=str(target),
        byte_size=target.stat().st_size,
        checksum_sha256=sha256_checksum(target),
    )


def iter_columnar_batches(artifact: ColumnarArtifactRef) -> Iterator[pa.RecordBatch]:
    if artifact.storage == "memory" and artifact.table is not None:
        yield from artifact.table.to_batches(max_chunksize=artifact.batch_boundary.max_rows)
        return
    if artifact.format != "arrow_ipc" or artifact.storage != "local_file" or artifact.uri is None:
        raise ValueError("Only local Arrow IPC artifacts can be iterated as batches.")
    with ipc.open_file(artifact.uri) as reader:
        _validate_reader(reader, artifact=artifact)
        for index in range(reader.num_record_batches):
            yield reader.get_batch(index)


def read_columnar_table(artifact: ColumnarArtifactRef) -> pa.Table:
    if artifact.storage == "memory" and artifact.table is not None:
        return artifact.table
    if artifact.format != "arrow_ipc" or artifact.storage != "local_file" or artifact.uri is None:
        raise ValueError("Only local Arrow IPC artifacts can be read as a table.")
    with ipc.open_file(artifact.uri) as reader:
        _validate_reader(reader, artifact=artifact)
        return reader.read_all()


def _validate_reader(reader: ipc.RecordBatchFileReader, *, artifact: ColumnarArtifactRef) -> None:
    if artifact.schema_fingerprint is not None:
        actual_schema_fingerprint = schema_fingerprint(reader.schema)
        if actual_schema_fingerprint != artifact.schema_fingerprint:
            raise ValueError("Arrow IPC schema fingerprint does not match artifact metadata.")
    if artifact.row_count is not None:
        actual_row_count = sum(
            reader.get_batch(index).num_rows for index in range(reader.num_record_batches)
        )
        if actual_row_count != artifact.row_count:
            raise ValueError("Arrow IPC row count does not match artifact metadata.")
    if artifact.batch_count is not None and reader.num_record_batches != artifact.batch_count:
        raise ValueError("Arrow IPC batch count does not match artifact metadata.")


__all__ = [
    "iter_columnar_batches",
    "read_columnar_table",
    "write_arrow_ipc_batches",
    "write_arrow_ipc_table",
]
