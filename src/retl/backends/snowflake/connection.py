from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from retl.errors import RetlError


class SnowflakeConnectionError(RetlError):
    """Raised when a Snowflake SQL connection cannot be opened or used."""


class SnowflakeConnection:
    """Adapter from Snowflake's cursor API to RETL's SQL connection protocol."""

    def __init__(
        self,
        *,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        authenticator: str | None = None,
        autocommit: bool | None = None,
        session_parameters: Mapping[str, object] | None = None,
        connect_kwargs: Mapping[str, object] | None = None,
        paramstyle: str | None = "numeric",
        connection: Any | None = None,
        connector: Any | None = None,
    ) -> None:
        self._connect_config = _connection_config(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            authenticator=authenticator,
            autocommit=autocommit,
            session_parameters=session_parameters,
            connect_kwargs=connect_kwargs,
        )
        self._closed = False

        if connection is not None:
            self._connection = connection
            self._apply_connection_autocommit(autocommit)
            return

        snowflake_connector = connector if connector is not None else _snowflake_connector()
        if paramstyle is not None:
            snowflake_connector.paramstyle = paramstyle

        try:
            self._connection = snowflake_connector.connect(**self._connect_config)
        except Exception as exc:
            raise SnowflakeConnectionError(
                "Snowflake SQL connection could not be opened with "
                f"{_redacted_mapping_repr(self._connect_config)}."
            ) from exc

    @property
    def raw_connection(self) -> Any:
        return self._connection

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] = (),
        *,
        statement_parameters: Mapping[str, object] | None = None,
    ) -> SnowflakeCursorResult:
        self._raise_if_closed()
        cursor = self._cursor()
        params = _normalize_parameters(parameters)
        try:
            if statement_parameters is None:
                cursor.execute(sql, params=params)
            else:
                cursor.execute(
                    sql,
                    params=params,
                    _statement_params=dict(statement_parameters),
                )
        except Exception as exc:
            _close_cursor(cursor)
            raise SnowflakeConnectionError("Snowflake SQL execution failed.") from exc
        return SnowflakeCursorResult(cursor)

    def executemany(
        self,
        sql: str,
        parameters: Sequence[Sequence[object] | Mapping[str, object]],
    ) -> SnowflakeCursorResult:
        self._raise_if_closed()
        cursor = self._cursor()
        rows = tuple(_normalize_parameters(row) for row in parameters)
        try:
            cursor.executemany(sql, rows)
        except Exception as exc:
            _close_cursor(cursor)
            raise SnowflakeConnectionError("Snowflake SQL batch execution failed.") from exc
        return SnowflakeCursorResult(cursor)

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
            raise SnowflakeConnectionError("Snowflake SQL connection close failed.") from exc

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
            raise SnowflakeConnectionError("Snowflake SQL cursor could not be opened.") from exc

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise SnowflakeConnectionError("Snowflake SQL connection is closed.")

    def _apply_connection_autocommit(self, autocommit: bool | None) -> None:
        if autocommit is None or not hasattr(self._connection, "autocommit"):
            return
        try:
            self._connection.autocommit(autocommit)
        except Exception as exc:
            raise SnowflakeConnectionError(
                "Snowflake SQL connection autocommit configuration failed."
            ) from exc


class SnowflakeCursorResult:
    """Cursor result wrapper with a DuckDB-like Arrow reader surface."""

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
        if hasattr(self._cursor, "fetch_arrow_all"):
            table = self._cursor.fetch_arrow_all()
            if table is not None and (
                _arrow_table_has_columns(table) or not self._cursor_description
            ):
                return table
            return _empty_arrow_table_from_cursor_description(self._cursor)
        batches = tuple(self.fetch_arrow_batches())
        return _arrow_table_from_batches(batches)

    def fetch_arrow_batches(self) -> Iterable[Any]:
        if hasattr(self._cursor, "fetch_arrow_batches"):
            return self._cursor.fetch_arrow_batches()
        table = self.fetch_arrow_all()
        return table.to_batches()

    def to_arrow_reader(self, *, batch_size: int | None = None) -> Any:
        if hasattr(self._cursor, "fetch_arrow_batches"):
            return _ArrowBatchStreamReader(
                self._cursor.fetch_arrow_batches(),
                batch_size=batch_size,
                empty_schema_factory=self._empty_arrow_schema,
            )
        table = self.fetch_arrow_all()
        return _ArrowTableReader(table, batch_size=batch_size)

    def close(self) -> None:
        _close_cursor(self._cursor)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    def _empty_arrow_schema(self) -> Any:
        return self.fetch_arrow_all().schema

    @property
    def _cursor_description(self) -> Any:
        return getattr(self._cursor, "description", None)


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


class _ArrowBatchStreamReader:
    def __init__(
        self,
        chunks: Iterable[Any],
        *,
        batch_size: int | None,
        empty_schema_factory: Callable[[], Any],
    ) -> None:
        self._batch_size = batch_size
        self._batches = iter(_record_batches_from_arrow_chunks(chunks, batch_size=batch_size))
        self._empty_schema_factory = empty_schema_factory
        self._pending: list[Any] = []
        self._schema: Any | None = None

    @property
    def schema(self) -> Any:
        if self._schema is None:
            self._ensure_schema()
        return self._schema

    def read_next_batch(self) -> Any:
        if self._batch_size is not None:
            return self._read_next_sized_batch()
        if self._pending:
            return self._pending.pop(0)
        batch = next(self._batches)
        self._schema = batch.schema
        return batch

    def _read_next_sized_batch(self) -> Any:
        target_rows = max(1, self._batch_size or 1)
        batches: list[Any] = []
        row_count = 0
        while row_count < target_rows:
            try:
                batch = self._pending.pop(0) if self._pending else next(self._batches)
            except StopIteration:
                break
            batches.append(batch)
            row_count += batch.num_rows
            self._schema = batch.schema
        if not batches:
            raise StopIteration
        return _record_batch_from_batches(batches)

    def _ensure_schema(self) -> None:
        try:
            batch = next(self._batches)
        except StopIteration:
            self._schema = self._empty_schema_factory()
            return
        self._schema = batch.schema
        self._pending.append(batch)


def _snowflake_connector(
    import_module: Callable[[str], Any] = importlib.import_module,
) -> Any:
    try:
        return import_module("snowflake.connector")
    except ImportError as exc:
        raise SnowflakeConnectionError(
            "Snowflake SQL connections require the optional `snowflake` dependency. "
            "Install it with `retl[snowflake]`."
        ) from exc


def _connection_config(
    *,
    account: str | None,
    user: str | None,
    password: str | None,
    warehouse: str | None,
    database: str | None,
    schema: str | None,
    role: str | None,
    authenticator: str | None,
    autocommit: bool | None,
    session_parameters: Mapping[str, object] | None,
    connect_kwargs: Mapping[str, object] | None,
) -> dict[str, object]:
    config: dict[str, object] = {}
    for key, value in (
        ("account", account),
        ("user", user),
        ("password", password),
        ("warehouse", warehouse),
        ("database", database),
        ("schema", schema),
        ("role", role),
        ("authenticator", authenticator),
        ("autocommit", autocommit),
    ):
        if value is not None:
            config[key] = value
    if session_parameters is not None:
        config["session_parameters"] = dict(session_parameters)
    if connect_kwargs is not None:
        config.update(dict(connect_kwargs))
    return config


def _normalize_parameters(
    parameters: Sequence[object] | Mapping[str, object],
) -> tuple[object, ...] | dict[str, object]:
    if isinstance(parameters, Mapping):
        return dict(parameters)
    if isinstance(parameters, str | bytes | bytearray):
        raise TypeError("Snowflake SQL parameters must be a sequence or mapping, not bytes/text.")
    return tuple(parameters)


def _arrow_table_from_batches(batches: Sequence[Any]) -> Any:
    import pyarrow as pa  # type: ignore[import-untyped]

    record_batches: list[Any] = []
    for batch in batches:
        if hasattr(batch, "to_batches"):
            record_batches.extend(batch.to_batches())
        else:
            record_batches.append(batch)
    return pa.Table.from_batches(record_batches)


def _arrow_table_has_columns(table: Any) -> bool:
    schema = getattr(table, "schema", None)
    names = getattr(schema, "names", None)
    return bool(names)


def _record_batches_from_arrow_chunks(
    chunks: Iterable[Any],
    *,
    batch_size: int | None,
) -> Iterable[Any]:
    max_chunksize = max(1, batch_size) if batch_size is not None else None
    for chunk in chunks:
        if hasattr(chunk, "to_batches"):
            yield from chunk.to_batches(max_chunksize=max_chunksize)
            continue
        yield from _split_record_batch(chunk, max_chunksize=max_chunksize)


def _split_record_batch(batch: Any, *, max_chunksize: int | None) -> Iterable[Any]:
    if max_chunksize is None or batch.num_rows <= max_chunksize:
        yield batch
        return
    for offset in range(0, batch.num_rows, max_chunksize):
        yield batch.slice(offset, max_chunksize)


def _record_batch_from_batches(batches: Sequence[Any]) -> Any:
    if len(batches) == 1:
        return batches[0]

    import pyarrow as pa  # type: ignore[import-untyped]

    table = pa.Table.from_batches(batches, schema=batches[0].schema).combine_chunks()
    return table.to_batches(max_chunksize=max(1, table.num_rows))[0]


def _empty_arrow_table_from_cursor_description(cursor: Any) -> Any:
    import pyarrow as pa  # type: ignore[import-untyped]

    fields = [
        pa.field(str(column[0]), pa.null()) for column in getattr(cursor, "description", ()) or ()
    ]
    return pa.Table.from_batches([], schema=pa.schema(fields))


def _close_cursor(cursor: Any) -> None:
    if hasattr(cursor, "close"):
        cursor.close()


_SENSITIVE_CONFIG_KEYS = frozenset(
    {
        "password",
        "passcode",
        "passcode_in_password",
        "private_key",
        "private_key_file",
        "private_key_file_pwd",
        "token",
        "oauth_token",
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
        or "password" in normalized
        or "privatekey" in normalized
        or normalized.endswith("token")
    )


__all__ = [
    "SnowflakeConnection",
    "SnowflakeConnectionError",
    "SnowflakeCursorResult",
    "_snowflake_connector",
]
