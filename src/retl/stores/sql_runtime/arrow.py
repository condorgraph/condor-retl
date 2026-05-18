from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]


@runtime_checkable
class _ArrowReaderResult(Protocol):
    def to_arrow_reader(self, *, batch_size: int | None = None) -> Any: ...


@runtime_checkable
class _ArrowBatchResult(Protocol):
    def fetch_arrow_batches(self) -> Iterable[Any]: ...


class _StreamingRecordBatchReader:
    def __init__(self, batches: Iterable[Any], *, schema: pa.Schema | None) -> None:
        self._batches = iter(batches)
        self._schema = schema

    @property
    def schema(self) -> pa.Schema:
        if self._schema is None:
            raise TypeError("empty Arrow batch stream did not expose a schema.")
        return self._schema

    def read_next_batch(self) -> pa.RecordBatch:
        batch = next(self._batches)
        _validate_record_batch(batch)
        if self._schema is None:
            self._schema = batch.schema
        return batch


def fetch_record_batch(result: object, *, batch_size: int) -> pa.RecordBatch:
    reader = _record_batch_reader(result, batch_size=batch_size)
    try:
        batch = reader.read_next_batch()
    except StopIteration:
        return empty_record_batch(reader.schema)
    _validate_record_batch(batch)
    return canonical_record_batch(batch)


def fetch_bounded_record_batch(result: object, *, row_limit: int) -> pa.RecordBatch:
    """Combine Arrow batches until a bounded SQL page is satisfied."""

    if not isinstance(row_limit, int) or isinstance(row_limit, bool) or row_limit <= 0:
        raise ValueError("bounded Arrow fetch `row_limit` must be an integer greater than 0.")
    reader = _record_batch_reader(result, batch_size=row_limit)
    batches: list[pa.RecordBatch] = []
    total_rows = 0
    while total_rows < row_limit:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        _validate_record_batch(batch)
        remaining = row_limit - total_rows
        if batch.num_rows > remaining:
            batch = batch.slice(0, remaining)
        if batch.num_rows:
            batches.append(batch)
            total_rows += batch.num_rows
    if not batches:
        return canonical_record_batch(empty_record_batch(reader.schema))
    table = pa.Table.from_batches(batches, schema=reader.schema).combine_chunks()
    return canonical_record_batch(table.to_batches(max_chunksize=max(1, table.num_rows))[0])


def fetch_all_record_batches(result: object) -> pa.RecordBatch:
    """Combine all batches from an already bounded result into one RecordBatch."""

    reader = _record_batch_reader(result)
    batches: list[pa.RecordBatch] = []
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        _validate_record_batch(batch)
        batches.append(batch)
    if not batches:
        return canonical_record_batch(empty_record_batch(reader.schema))
    table = pa.Table.from_batches(batches, schema=reader.schema).combine_chunks()
    return canonical_record_batch(table.to_batches(max_chunksize=max(1, table.num_rows))[0])


def _record_batch_reader(result: object, *, batch_size: int | None = None) -> Any:
    if isinstance(result, _ArrowReaderResult):
        if batch_size is None:
            return result.to_arrow_reader()
        return result.to_arrow_reader(batch_size=batch_size)
    if isinstance(result, _ArrowBatchResult):
        return _StreamingRecordBatchReader(
            result.fetch_arrow_batches(),
            schema=_schema_from_result(result),
        )
    raise TypeError("SQL result must expose to_arrow_reader(...) or fetch_arrow_batches().")


def _schema_from_result(result: object) -> pa.Schema | None:
    schema = getattr(result, "schema", None)
    if schema is None:
        return None
    if callable(schema):
        schema = schema()
    if not isinstance(schema, pa.Schema):
        raise TypeError("Arrow batch stream schema must be a pyarrow.Schema.")
    return schema


def _validate_record_batch(batch: object) -> None:
    if not isinstance(batch, pa.RecordBatch):
        raise TypeError(
            f"Arrow result batches must be pyarrow.RecordBatch values; got {type(batch).__name__}."
        )


def empty_record_batch(schema: pa.Schema) -> pa.RecordBatch:
    arrays = [pa.array([], type=field.type) for field in schema]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def canonical_record_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    fields = []
    seen: set[str] = set()
    for field in batch.schema:
        canonical_name = field.name.casefold()
        if canonical_name in seen:
            raise ValueError(
                "Arrow runtime batch contains columns that collide after case normalization."
            )
        seen.add(canonical_name)
        fields.append(field.with_name(canonical_name))
    return pa.RecordBatch.from_arrays(
        [batch.column(index) for index in range(batch.num_columns)],
        schema=pa.schema(fields, metadata=batch.schema.metadata),
    )


def drop_columns(batch: pa.RecordBatch, column_names: tuple[str, ...]) -> pa.RecordBatch:
    if not column_names:
        return batch
    keep = [
        batch.column(index)
        for index, field in enumerate(batch.schema)
        if field.name not in column_names
    ]
    schema = pa.schema(
        [field for field in batch.schema if field.name not in column_names],
        metadata=batch.schema.metadata,
    )
    return pa.RecordBatch.from_arrays(keep, schema=schema)


def rows_before_collect_id(payload: pa.RecordBatch, collect_id: str | None) -> int:
    if collect_id is None:
        return 0
    sequence_values = payload.column(payload.schema.get_field_index("collect_id"))
    matches = pc.indices_nonzero(
        pc.fill_null(
            pc.equal(
                sequence_values,
                pa.scalar(collect_id, type=sequence_values.type),
            ),
            False,
        )
    )
    if len(matches) > 0:
        return int(matches[0].as_py())
    return payload.num_rows


def first_int_value(payload: pa.RecordBatch, column_name: str) -> int | None:
    if payload.num_rows == 0:
        return None
    return int_value(payload, column_name, 0)


def last_int_value(payload: pa.RecordBatch, column_name: str) -> int | None:
    if payload.num_rows == 0:
        return None
    return int_value(payload, column_name, payload.num_rows - 1)


def first_string_value(payload: pa.RecordBatch, column_name: str) -> str | None:
    if payload.num_rows == 0:
        return None
    return string_value(payload, column_name, 0)


def last_string_value(payload: pa.RecordBatch, column_name: str) -> str | None:
    if payload.num_rows == 0:
        return None
    return string_value(payload, column_name, payload.num_rows - 1)


def int_value(payload: pa.RecordBatch, column_name: str, index: int) -> int:
    value = payload.column(payload.schema.get_field_index(column_name))[index].as_py()
    return int(value)


def string_value(payload: pa.RecordBatch, column_name: str, index: int) -> str:
    value = payload.column(payload.schema.get_field_index(column_name))[index].as_py()
    return str(value)


__all__ = [
    "canonical_record_batch",
    "drop_columns",
    "empty_record_batch",
    "fetch_all_record_batches",
    "fetch_bounded_record_batch",
    "fetch_record_batch",
    "first_int_value",
    "first_string_value",
    "int_value",
    "last_int_value",
    "last_string_value",
    "rows_before_collect_id",
    "string_value",
]
