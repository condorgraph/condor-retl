from __future__ import annotations

import json

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from retl.errors import DeclarationValidationError
from retl.stores.contracts import StateOrderedWorkScanPosition
from retl.stores.sql_runtime.arrow import (
    canonical_record_batch,
    drop_columns,
    empty_record_batch,
    fetch_all_record_batches,
    fetch_bounded_record_batch,
    fetch_record_batch,
    first_string_value,
    int_value,
    last_string_value,
    rows_before_collect_id,
    string_value,
)
from retl.stores.sql_runtime.json import report_json, to_json
from retl.stores.sql_runtime.positions import (
    scan_position_from_storage_json,
    scan_position_to_storage_json,
)


class _ArrowResult:
    def __init__(self, schema: pa.Schema, batches: list[pa.RecordBatch]) -> None:
        self._schema = schema
        self._batches = batches
        self.batch_sizes: list[int | None] = []

    def to_arrow_reader(self, *, batch_size: int | None = None) -> pa.RecordBatchReader:
        self.batch_sizes.append(batch_size)
        return pa.RecordBatchReader.from_batches(self._schema, self._batches)


class _StreamingArrowResult:
    def __init__(self, schema: pa.Schema, batches: list[object]) -> None:
        self.schema = schema
        self._batches = batches
        self.yielded_indexes: list[int] = []

    def fetch_arrow_batches(self) -> object:
        for index, batch in enumerate(self._batches):
            self.yielded_indexes.append(index)
            yield batch


class _DualArrowResult:
    def __init__(
        self,
        schema: pa.Schema,
        reader_batches: list[pa.RecordBatch],
        stream_batches: list[object],
    ) -> None:
        self._schema = schema
        self._reader_batches = reader_batches
        self._stream_batches = stream_batches
        self.batch_sizes: list[int | None] = []
        self.stream_requested = False

    def to_arrow_reader(self, *, batch_size: int | None = None) -> pa.RecordBatchReader:
        self.batch_sizes.append(batch_size)
        return pa.RecordBatchReader.from_batches(self._schema, self._reader_batches)

    def fetch_arrow_batches(self) -> object:
        self.stream_requested = True
        yield from self._stream_batches


class _Report:
    def to_dict(self) -> dict[str, object]:
        return {"status": "succeeded", "count": 2}


def test_json_helpers_preserve_storage_format_and_errors() -> None:
    assert to_json({"b": 2, "a": 1}, "payload") == '{"a":1,"b":2}'
    assert report_json(_Report(), "sync_report") == '{"count": 2, "status": "succeeded"}'

    with pytest.raises(
        DeclarationValidationError,
        match=r"ordered work `payload` must be JSON-serializable.",
    ):
        to_json(object(), "payload")

    with pytest.raises(
        DeclarationValidationError,
        match=r"`sync_report` must expose to_dict\(\).",
    ):
        report_json({"status": "succeeded"}, "sync_report")


def test_scan_position_storage_json_round_trips_and_rejects_invalid_json() -> None:
    position = StateOrderedWorkScanPosition(
        collect_id="00000000-0003-7000-8000-000000000000", sequence_order=7
    )

    encoded = scan_position_to_storage_json(position)

    assert (
        encoded
        == '{"collect_id":"00000000-0003-7000-8000-000000000000","family":"state","mode":"ordered_work","sequence_order":7}'
    )
    assert scan_position_from_storage_json(encoded, field_name="position_json") == position
    assert scan_position_from_storage_json(None, field_name="position_json") is None

    with pytest.raises(
        DeclarationValidationError,
        match=r"Destination progress `position_json` must be valid JSON.",
    ):
        scan_position_from_storage_json("{", field_name="position_json")

    with pytest.raises(
        DeclarationValidationError,
        match=r"Destination progress `position_json` must be a scan position object.",
    ):
        scan_position_from_storage_json("[]", field_name="position_json")

    with pytest.raises(
        DeclarationValidationError,
        match=r"ordered_work State scan positions require collect_id and sequence_order.",
    ):
        scan_position_from_storage_json(
            json.dumps({"family": "state", "mode": "ordered_work"}),
            field_name="position_json",
        )


def test_arrow_helpers_handle_empty_batches_and_value_extraction() -> None:
    schema = pa.schema(
        [
            pa.field("collect_id", pa.string()),
            pa.field("sequence_order", pa.int64()),
            pa.field("identity_json", pa.string()),
        ],
        metadata={b"source": b"test"},
    )
    empty = empty_record_batch(schema)

    assert empty.num_rows == 0
    assert empty.schema == schema
    assert first_string_value(empty, "collect_id") is None
    assert last_string_value(empty, "collect_id") is None
    assert rows_before_collect_id(empty, "00000000-0005-7000-8000-000000000000") == empty.num_rows

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(
                [
                    "00000000-0001-7000-8000-000000000000",
                    "00000000-0002-7000-8000-000000000000",
                    "00000000-0002-7000-8000-000000000000",
                    "00000000-0003-7000-8000-000000000000",
                ],
                type=pa.string(),
            ),
            pa.array([0, 0, 1, 0], type=pa.int64()),
            pa.array(["a", "b", "c", "d"], type=pa.string()),
        ],
        schema=schema,
    )

    assert first_string_value(batch, "collect_id") == "00000000-0001-7000-8000-000000000000"
    assert last_string_value(batch, "collect_id") == "00000000-0003-7000-8000-000000000000"
    assert int_value(batch, "sequence_order", 2) == 1
    assert string_value(batch, "identity_json", 2) == "c"
    assert rows_before_collect_id(batch, None) == 0
    assert rows_before_collect_id(batch, "00000000-0002-7000-8000-000000000000") == 1
    assert rows_before_collect_id(batch, "00000000-0004-7000-8000-000000000000") == batch.num_rows

    dropped = drop_columns(batch, ("sequence_order",))
    assert dropped.column_names == ["collect_id", "identity_json"]
    assert dropped.schema.metadata == schema.metadata
    assert drop_columns(batch, ()) is batch


def test_arrow_helpers_canonicalize_runtime_batch_column_names() -> None:
    schema = pa.schema(
        [
            pa.field("COLLECT_ID", pa.string()),
            pa.field("SEQUENCE_ORDER", pa.int64()),
        ]
    )
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["00000000-0001-7000-8000-000000000000"], type=pa.string()),
            pa.array([0], type=pa.int64()),
        ],
        schema=schema,
    )

    canonical = canonical_record_batch(batch)

    assert canonical.column_names == ["collect_id", "sequence_order"]
    assert canonical.column("collect_id").to_pylist() == ["00000000-0001-7000-8000-000000000000"]


def test_arrow_helpers_reject_case_colliding_runtime_batch_columns() -> None:
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["00000000-0001-7000-8000-000000000000"], type=pa.string()),
            pa.array(["00000000-0002-7000-8000-000000000000"], type=pa.string()),
        ],
        names=["COLLECT_ID", "collect_id"],
    )

    with pytest.raises(ValueError, match="collide after case normalization"):
        canonical_record_batch(batch)


def test_arrow_fetch_helpers_return_empty_batch_for_empty_reader() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    result = _ArrowResult(schema=schema, batches=[])

    assert fetch_record_batch(result, batch_size=10).schema == schema
    assert fetch_record_batch(result, batch_size=10).num_rows == 0
    assert fetch_all_record_batches(result).schema == schema
    assert fetch_all_record_batches(result).num_rows == 0


def test_fetch_record_batch_consumes_only_first_streaming_only_batch() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    batches = [
        pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], schema=schema),
        pa.RecordBatch.from_arrays([pa.array([3, 4], type=pa.int64())], schema=schema),
    ]
    result = _StreamingArrowResult(schema=schema, batches=batches)

    fetched = fetch_record_batch(result, batch_size=10)

    assert fetched.column(0).to_pylist() == [1, 2]
    assert result.yielded_indexes == [0]


def test_fetch_bounded_record_batch_combines_streaming_batches_to_row_limit() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    batches = [
        pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], schema=schema),
        pa.RecordBatch.from_arrays([pa.array([3, 4], type=pa.int64())], schema=schema),
        pa.RecordBatch.from_arrays([pa.array([5, 6], type=pa.int64())], schema=schema),
    ]
    result = _StreamingArrowResult(schema=schema, batches=batches)

    fetched = fetch_bounded_record_batch(result, row_limit=5)

    assert fetched.column(0).to_pylist() == [1, 2, 3, 4, 5]
    assert result.yielded_indexes == [0, 1, 2]


def test_fetch_bounded_record_batch_reads_until_stream_exhaustion() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    batches = [
        pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], schema=schema),
        pa.RecordBatch.from_arrays([pa.array([3], type=pa.int64())], schema=schema),
    ]

    fetched = fetch_bounded_record_batch(
        _StreamingArrowResult(schema=schema, batches=batches),
        row_limit=10,
    )

    assert fetched.column(0).to_pylist() == [1, 2, 3]


def test_fetch_record_batch_passes_batch_size_to_arrow_reader() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    result = _ArrowResult(
        schema=schema,
        batches=[
            pa.RecordBatch.from_arrays(
                [pa.array([1, 2], type=pa.int64())],
                schema=schema,
            ),
        ],
    )

    fetched = fetch_record_batch(result, batch_size=2)

    assert fetched.num_rows == 2
    assert result.batch_sizes == [2]


def test_fetch_bounded_record_batch_passes_row_limit_to_arrow_reader() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    result = _ArrowResult(
        schema=schema,
        batches=[
            pa.RecordBatch.from_arrays(
                [pa.array([1, 2], type=pa.int64())],
                schema=schema,
            ),
            pa.RecordBatch.from_arrays(
                [pa.array([3, 4], type=pa.int64())],
                schema=schema,
            ),
        ],
    )

    fetched = fetch_bounded_record_batch(result, row_limit=3)

    assert fetched.column(0).to_pylist() == [1, 2, 3]
    assert result.batch_sizes == [3]


def test_fetch_record_batch_prefers_arrow_reader_when_both_surfaces_exist() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    reader_batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2], type=pa.int64())],
        schema=schema,
    )
    stream_batch = pa.RecordBatch.from_arrays(
        [pa.array([3, 4], type=pa.int64())],
        schema=schema,
    )
    result = _DualArrowResult(
        schema=schema,
        reader_batches=[reader_batch],
        stream_batches=[stream_batch],
    )

    fetched = fetch_record_batch(result, batch_size=2)

    assert fetched.column(0).to_pylist() == [1, 2]
    assert result.batch_sizes == [2]
    assert result.stream_requested is False


def test_fetch_record_batch_rejects_non_record_batch_stream_values() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    result = _StreamingArrowResult(
        schema=schema,
        batches=[pa.table({"collect_id": [1]})],
    )

    with pytest.raises(
        TypeError,
        match=(
            r"Arrow result batches must be pyarrow.RecordBatch values; "
            r"got Table\."
        ),
    ):
        fetch_record_batch(result, batch_size=10)


def test_fetch_record_batch_returns_empty_batch_for_empty_stream_with_schema() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    result = _StreamingArrowResult(schema=schema, batches=[])

    fetched = fetch_record_batch(result, batch_size=10)

    assert fetched.schema == schema
    assert fetched.num_rows == 0


def test_arrow_fetch_all_returns_non_empty_reader_batch() -> None:
    schema = pa.schema([pa.field("collect_id", pa.int64())])
    batches = [
        pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], schema=schema),
        pa.RecordBatch.from_arrays([pa.array([3, 4], type=pa.int64())], schema=schema),
    ]

    combined = fetch_all_record_batches(_ArrowResult(schema=schema, batches=batches))

    assert combined.num_rows == 4
    assert combined.column(0).to_pylist() == [1, 2, 3, 4]
