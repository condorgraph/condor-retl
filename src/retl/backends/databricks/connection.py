from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from retl.errors import RetlError


class DatabricksConnectionError(RetlError):
    """Raised when a Databricks SQL connection cannot be opened or used."""


class DatabricksConnection:
    """Adapter from Databricks SQL Connector cursors to RETL's SQL protocol."""

    def __init__(
        self,
        *,
        server_hostname: str | None = None,
        http_path: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        session_configuration: Mapping[str, object] | None = None,
        connect_kwargs: Mapping[str, object] | None = None,
        connection: Any | None = None,
        connector: Any | None = None,
    ) -> None:
        self._connect_config = _connection_config(
            server_hostname=server_hostname,
            http_path=http_path,
            catalog=catalog,
            schema=schema,
            session_configuration=session_configuration,
            connect_kwargs=connect_kwargs,
        )
        self._closed = False
        if connection is not None:
            self._connection = connection
            return

        databricks_sql = connector if connector is not None else _databricks_sql_module()
        try:
            self._connection = databricks_sql.connect(**self._connect_config)
            if hasattr(self._connection, "autocommit"):
                self._connection.autocommit = False
        except Exception as exc:
            raise DatabricksConnectionError(
                "Databricks SQL connection could not be opened with "
                f"{_redacted_mapping_repr(self._connect_config)}."
            ) from exc

    @property
    def raw_connection(self) -> Any:
        return self._connection

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] = (),
    ) -> DatabricksCursorResult:
        self._raise_if_closed()
        cursor = self._cursor()
        params = _normalize_parameters(parameters)
        try:
            cursor.execute(sql, params)
        except TypeError:
            try:
                cursor.execute(sql, parameters=params)
            except Exception as exc:
                _close_cursor(cursor)
                raise DatabricksConnectionError("Databricks SQL execution failed.") from exc
        except Exception as exc:
            _close_cursor(cursor)
            raise DatabricksConnectionError("Databricks SQL execution failed.") from exc
        return DatabricksCursorResult(cursor)

    def executemany(
        self,
        sql: str,
        parameters: Sequence[Sequence[object] | Mapping[str, object]],
    ) -> DatabricksCursorResult:
        self._raise_if_closed()
        cursor = self._cursor()
        rows = [_normalize_parameters(row) for row in parameters]
        try:
            cursor.executemany(sql, rows)
        except Exception as exc:
            _close_cursor(cursor)
            raise DatabricksConnectionError("Databricks SQL batch execution failed.") from exc
        return DatabricksCursorResult(cursor)

    def commit(self) -> None:
        self._raise_if_closed()
        if hasattr(self._connection, "commit"):
            self._connection.commit()
            return
        self.execute("commit")

    def rollback(self) -> None:
        self._raise_if_closed()
        if hasattr(self._connection, "rollback"):
            self._connection.rollback()
            return
        self.execute("rollback")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._connection.close()
        except Exception as exc:
            raise DatabricksConnectionError("Databricks SQL connection close failed.") from exc

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
            raise DatabricksConnectionError("Databricks SQL cursor could not be opened.") from exc

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise DatabricksConnectionError("Databricks SQL connection is closed.")


class DatabricksCursorResult:
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
        if hasattr(self._cursor, "fetchall_arrow"):
            return self._cursor.fetchall_arrow()
        if hasattr(self._cursor, "fetch_arrow_all"):
            return self._cursor.fetch_arrow_all()
        batches = tuple(self.fetch_arrow_batches())
        return _arrow_table_from_batches(batches)

    def fetch_arrow_batches(self) -> Iterable[Any]:
        if hasattr(self._cursor, "fetchmany_arrow"):
            while True:
                batch = self._cursor.fetchmany_arrow()
                if batch is None or getattr(batch, "num_rows", 0) == 0:
                    return
                yield batch
        elif hasattr(self._cursor, "fetch_arrow_batches"):
            yield from self._cursor.fetch_arrow_batches()
        else:
            yield from self.fetch_arrow_all().to_batches()

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


def _databricks_sql_module(
    import_module: Callable[[str], Any] = importlib.import_module,
) -> Any:
    try:
        return import_module("databricks.sql")
    except ImportError as exc:
        raise DatabricksConnectionError(
            "Databricks SQL connections require the optional `databricks` dependency. "
            "Install it with `retl[databricks]`."
        ) from exc


def _connection_config(
    *,
    server_hostname: str | None,
    http_path: str | None,
    catalog: str | None,
    schema: str | None,
    session_configuration: Mapping[str, object] | None,
    connect_kwargs: Mapping[str, object] | None,
) -> dict[str, object]:
    config: dict[str, object] = {}
    for key, value in (
        ("server_hostname", server_hostname),
        ("http_path", http_path),
        ("catalog", catalog),
        ("schema", schema),
    ):
        if value is not None:
            config[key] = value
    if session_configuration is not None:
        config["session_configuration"] = dict(session_configuration)
    if connect_kwargs is not None:
        config.update(dict(connect_kwargs))
    return config


def _normalize_parameters(
    parameters: Sequence[object] | Mapping[str, object],
) -> list[object] | dict[str, object]:
    if isinstance(parameters, Mapping):
        return dict(parameters)
    if isinstance(parameters, str | bytes | bytearray):
        raise TypeError("Databricks SQL parameters must be a sequence or mapping, not bytes/text.")
    return list(parameters)


def _arrow_table_from_batches(batches: Sequence[Any]) -> Any:
    import pyarrow as pa  # type: ignore[import-untyped]

    return pa.Table.from_batches(list(batches))


def _close_cursor(cursor: Any) -> None:
    if hasattr(cursor, "close"):
        cursor.close()


_SENSITIVE_CONFIG_KEYS = frozenset(
    {
        "access_token",
        "client_secret",
        "credentials_provider",
        "password",
        "token",
    }
)


def _redacted_mapping_repr(mapping: Mapping[str, object]) -> str:
    return repr(_redact_mapping(mapping))


def _redact_mapping(mapping: Mapping[str, object]) -> dict[str, object]:
    redacted: dict[str, object] = {}
    for key, value in mapping.items():
        if _is_sensitive_config_key(key):
            redacted[key] = "<redacted>"
        elif isinstance(value, Mapping):
            redacted[key] = _redact_mapping(value)
        else:
            redacted[key] = value
    return redacted


def _is_sensitive_config_key(key: str) -> bool:
    normalized = key.casefold().replace("_", "").replace("-", "")
    return (
        key.casefold() in _SENSITIVE_CONFIG_KEYS
        or normalized in _SENSITIVE_CONFIG_KEYS
        or "secret" in normalized
        or normalized.endswith("token")
    )


__all__ = [
    "DatabricksConnection",
    "DatabricksConnectionError",
    "DatabricksCursorResult",
    "_databricks_sql_module",
]
