from __future__ import annotations

import importlib
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from retl.errors import RetlError


class PostgreSqlConnectionError(RetlError):
    """Raised when a PostgreSQL SQL connection cannot be opened or used."""


class PostgreSqlConnection:
    """Adapter from Psycopg's cursor API to RETL's SQL connection protocol."""

    def __init__(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        sslmode: str | None = None,
        connect_timeout: int | None = None,
        autocommit: bool | None = None,
        connect_kwargs: Mapping[str, object] | None = None,
        connection: Any | None = None,
        connector: Any | None = None,
    ) -> None:
        self._connect_config = _connection_config(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            sslmode=sslmode,
            connect_timeout=connect_timeout,
            connect_kwargs=connect_kwargs,
        )
        self._closed = False
        if connection is not None:
            self._connection = connection
            self._apply_connection_autocommit(autocommit)
            return

        psycopg = connector if connector is not None else _psycopg_driver()
        try:
            self._connection = psycopg.connect(**self._connect_config)
            self._apply_connection_autocommit(autocommit)
        except Exception as exc:
            raise PostgreSqlConnectionError(
                "PostgreSQL SQL connection could not be opened with "
                f"{_redacted_mapping_repr(self._connect_config)}."
            ) from exc

    @property
    def raw_connection(self) -> Any:
        return self._connection

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] = (),
    ) -> PostgreSqlCursorResult:
        self._raise_if_closed()
        cursor = self._cursor()
        try:
            cursor.execute(sql, _normalize_parameters(parameters))
        except Exception as exc:
            _close_cursor(cursor)
            raise PostgreSqlConnectionError("PostgreSQL SQL execution failed.") from exc
        return PostgreSqlCursorResult(cursor)

    def executemany(
        self,
        sql: str,
        parameters: Sequence[Sequence[object] | Mapping[str, object]],
    ) -> PostgreSqlCursorResult:
        self._raise_if_closed()
        cursor = self._cursor()
        rows = tuple(_normalize_parameters(row) for row in parameters)
        try:
            cursor.executemany(sql, rows)
        except Exception as exc:
            _close_cursor(cursor)
            raise PostgreSqlConnectionError("PostgreSQL SQL batch execution failed.") from exc
        return PostgreSqlCursorResult(cursor)

    def commit(self) -> None:
        self._raise_if_closed()
        self._connection.commit()

    def rollback(self) -> None:
        self._raise_if_closed()
        self._connection.rollback()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._connection.close()
        except Exception as exc:
            raise PostgreSqlConnectionError("PostgreSQL SQL connection close failed.") from exc

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return (
            f"{type(self).__name__}("
            f"config={_redacted_mapping_repr(self._connect_config)}, state={state!r})"
        )

    def _cursor(self) -> Any:
        try:
            return self._connection.cursor()
        except Exception as exc:
            raise PostgreSqlConnectionError("PostgreSQL SQL cursor could not be opened.") from exc

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise PostgreSqlConnectionError("PostgreSQL SQL connection is closed.")

    def _apply_connection_autocommit(self, autocommit: bool | None) -> None:
        if autocommit is None:
            return
        try:
            self._connection.autocommit = autocommit
        except Exception as exc:
            raise PostgreSqlConnectionError(
                "PostgreSQL SQL connection autocommit configuration failed."
            ) from exc


class PostgreSqlCursorResult:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    @property
    def raw_cursor(self) -> Any:
        return self._cursor

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> Any:
        return self._cursor.fetchall()

    def fetch_arrow_all(self) -> Any:
        rows = self.fetchall()
        import pyarrow as pa  # type: ignore[import-untyped]

        names = _cursor_column_names(self._cursor)
        columns = {name: [row[index] for row in rows] for index, name in enumerate(names)}
        return pa.table(columns)

    def fetch_arrow_batches(self) -> Iterable[Any]:
        return self.fetch_arrow_all().to_batches()

    def to_arrow_reader(self, *, batch_size: int | None = None) -> Any:
        table = self.fetch_arrow_all()
        return _ArrowTableReader(table, batch_size=batch_size)

    def close(self) -> None:
        _close_cursor(self._cursor)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class _ArrowTableReader:
    def __init__(self, table: Any, *, batch_size: int | None) -> None:
        self._schema = table.schema
        max_chunksize = max(1, batch_size) if batch_size is not None else None
        self._batches = iter(table.to_batches(max_chunksize=max_chunksize))

    @property
    def schema(self) -> Any:
        return self._schema

    def read_next_batch(self) -> Any:
        return next(self._batches)


def _psycopg_driver() -> Any:
    try:
        return importlib.import_module("psycopg")
    except ImportError as exc:
        raise PostgreSqlConnectionError(
            "PostgreSQL backend requires the optional `psycopg` dependency. "
            "Install RETL with the `postgresql` extra."
        ) from exc


def _connection_config(
    *,
    host: str | None,
    port: int | None,
    dbname: str | None,
    user: str | None,
    password: str | None,
    sslmode: str | None,
    connect_timeout: int | None,
    connect_kwargs: Mapping[str, object] | None,
) -> dict[str, object]:
    config: dict[str, object] = {}
    if connect_kwargs is not None:
        config.update(connect_kwargs)
    config.update(
        _without_none(
            {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": password,
                "sslmode": sslmode,
                "connect_timeout": connect_timeout,
            }
        )
    )
    return config


def _without_none(values: Mapping[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in values.items() if value is not None}


def _normalize_parameters(
    parameters: Sequence[object] | Mapping[str, object],
) -> Sequence[object] | Mapping[str, object]:
    if isinstance(parameters, Mapping):
        return dict(parameters)
    return tuple(parameters)


def _cursor_column_names(cursor: Any) -> list[str]:
    description = getattr(cursor, "description", None) or ()
    return [str(column[0]) for column in description]


def _close_cursor(cursor: Any) -> None:
    close = getattr(cursor, "close", None)
    if close is not None:
        close()


def _redacted_mapping_repr(values: Mapping[str, object]) -> str:
    redacted: dict[str, object] = {}
    for key, value in values.items():
        if key.lower() in {"password"}:
            redacted[key] = "[redacted]"
        else:
            redacted[key] = value
    return repr(redacted)


__all__ = ["PostgreSqlConnection", "PostgreSqlConnectionError"]
