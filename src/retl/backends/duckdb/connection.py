from __future__ import annotations

import importlib
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from retl.errors import RetlError


class DuckDBConnectionError(RetlError):
    """Raised when a DuckDB SQL connection cannot be opened."""


class DuckDBConnection:
    """Small adapter from DuckDB's Python connection to RETL's SQL protocol."""

    def __init__(
        self,
        database: str | Path = ":memory:",
        *,
        read_only: bool = False,
        config: Mapping[str, object] | None = None,
        connection: Any | None = None,
    ) -> None:
        if connection is not None:
            self._connection = connection
            return

        duckdb = _duckdb_driver()
        driver_config = cast(Any, dict(config or {}))
        self._connection = duckdb.connect(
            str(database),
            read_only=read_only,
            config=driver_config,
        )

    @property
    def raw_connection(self) -> Any:
        return self._connection

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        return _wrap_duckdb_result(self._connection.execute(sql, tuple(parameters)))

    def executemany(self, sql: str, parameters: Sequence[Sequence[object]]) -> Any:
        return _wrap_duckdb_result(
            self._connection.executemany(sql, tuple(tuple(row) for row in parameters))
        )

    def close(self) -> None:
        self._connection.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class DuckDBResult:
    """Normalize DuckDB query results behind RETL's SQL/Arrow result contract."""

    def __init__(self, result: Any) -> None:
        self._result = result

    @property
    def raw_result(self) -> Any:
        return self._result

    def fetchone(self) -> Any:
        return self._result.fetchone()

    def fetchall(self) -> Any:
        return self._result.fetchall()

    def __iter__(self) -> Iterator[Any]:
        return iter(self._result)

    def to_arrow_reader(self, *, batch_size: int | None = None) -> Any:
        method = _duckdb_arrow_reader_method(self._result)
        if method is None:
            raise TypeError("DuckDB result does not expose a streaming Arrow reader API.")
        return _call_duckdb_arrow_reader(method, batch_size=batch_size)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._result, name)


def _wrap_duckdb_result(result: Any) -> Any:
    if _duckdb_arrow_reader_method(result) is None:
        return result
    return DuckDBResult(result)


def _duckdb_arrow_reader_method(result: Any) -> Any | None:
    for name in ("to_arrow_reader", "arrow", "fetch_record_batch"):
        method = getattr(result, name, None)
        if callable(method):
            return method
    return None


def _call_duckdb_arrow_reader(method: Any, *, batch_size: int | None) -> Any:
    if batch_size is None:
        return method()
    try:
        return method(batch_size=batch_size)
    except TypeError as keyword_error:
        try:
            return method(batch_size)
        except TypeError as positional_error:
            raise keyword_error from positional_error


def _duckdb_driver(
    import_module: Callable[[str], Any] = importlib.import_module,
) -> Any:
    try:
        return import_module("duckdb")
    except ImportError as exc:
        raise DuckDBConnectionError(
            "DuckDB SQL connections require the optional `duckdb` dependency."
        ) from exc


__all__ = ["DuckDBConnection", "DuckDBConnectionError", "DuckDBResult", "_duckdb_driver"]
