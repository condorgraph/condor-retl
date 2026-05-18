from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa  # type: ignore[import-untyped]

from retl.backends.duckdb import DuckDBConnection
from retl.backends.duckdb.connection import DuckDBResult


class _StreamingDuckDBResult:
    custom_attribute = "delegated"

    def __init__(self) -> None:
        self.schema = pa.schema([pa.field("id", pa.int64())])
        self.rows_per_batch: list[int] = []
        self.fetch_arrow_table_count = 0

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> _StreamingDuckDBResult:
        return self

    def fetchone(self) -> tuple[int]:
        return (1,)

    def fetchall(self) -> list[tuple[int]]:
        return [(1,), (2,)]

    def __iter__(self) -> object:
        return iter([(1,), (2,)])

    def fetch_record_batch(self, rows_per_batch: int = 1_000_000) -> pa.RecordBatchReader:
        self.rows_per_batch.append(rows_per_batch)
        batches = [
            pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int64())], schema=self.schema),
            pa.RecordBatch.from_arrays([pa.array([3, 4], type=pa.int64())], schema=self.schema),
        ]
        return pa.RecordBatchReader.from_batches(self.schema, batches)

    def fetch_arrow_table(self) -> pa.Table:
        self.fetch_arrow_table_count += 1
        raise AssertionError("DuckDBResult should not materialize a full Arrow table.")

    def close(self) -> None:
        return None


def test_duckdb_result_wrapper_streams_record_batches_without_arrow_table_materialization() -> None:
    raw_result = _StreamingDuckDBResult()

    result = DuckDBConnection(connection=raw_result).execute("select id from rows")

    assert isinstance(result, DuckDBResult)
    assert result.fetchone() == (1,)
    assert result.fetchall() == [(1,), (2,)]
    assert list(result) == [(1,), (2,)]
    assert result.custom_attribute == "delegated"

    reader = result.to_arrow_reader(batch_size=2)

    assert reader.schema == raw_result.schema
    assert reader.read_next_batch().column(0).to_pylist() == [1, 2]
    assert reader.read_next_batch().column(0).to_pylist() == [3, 4]
    assert raw_result.rows_per_batch == [2]
    assert raw_result.fetch_arrow_table_count == 0


def test_duckdb_connection_delegates_driver_specific_connection_methods() -> None:
    class _RawConnection:
        def __init__(self) -> None:
            self.registered: list[tuple[str, object]] = []
            self.unregistered: list[str] = []

        def register(self, name: str, relation: object) -> None:
            self.registered.append((name, relation))

        def unregister(self, name: str) -> None:
            self.unregistered.append(name)

    raw_connection = _RawConnection()
    connection = DuckDBConnection(connection=raw_connection)
    relation = object()

    connection.register("runtime_view", relation)
    connection.unregister("runtime_view")

    assert raw_connection.registered == [("runtime_view", relation)]
    assert raw_connection.unregistered == ["runtime_view"]
