from __future__ import annotations

import datetime as dt
import importlib
import re
from collections.abc import Mapping, Sequence
from typing import Any

from retl.errors import RetlError


class BigQueryConnectionError(RetlError):
    """Raised when a BigQuery SQL connection cannot be opened or used."""


class BigQueryConnection:
    """Adapter from Google BigQuery clients to RETL's SQL connection protocol."""

    def __init__(
        self,
        *,
        project: str,
        location: str | None = None,
        client: Any | None = None,
        read_client: Any | None = None,
        client_kwargs: Mapping[str, object] | None = None,
        bigquery_module: Any | None = None,
        bigquery_storage_module: Any | None = None,
        use_session: bool = False,
    ) -> None:
        self.project = project
        self.location = location
        self._closed = False
        self._bigquery = bigquery_module
        self._bigquery_storage = bigquery_storage_module
        self._use_session = use_session
        self._session_id: str | None = None
        if client is not None:
            self._client = client
        else:
            module = self._bigquery_module() if self._bigquery is None else self._bigquery
            kwargs = dict(client_kwargs or {})
            try:
                self._client = module.Client(project=project, location=location, **kwargs)
            except Exception as exc:
                raise BigQueryConnectionError(
                    "BigQuery client could not be opened for project "
                    f"{project!r} and location {location!r}."
                ) from exc
        if read_client is not None:
            self._read_client = read_client
        else:
            self._read_client = None

    @property
    def raw_client(self) -> Any:
        return self._client

    @property
    def raw_read_client(self) -> Any:
        if self._read_client is None:
            module = (
                self._bigquery_storage_module()
                if self._bigquery_storage is None
                else self._bigquery_storage
            )
            self._read_client = module.BigQueryReadClient()
        return self._read_client

    def execute(
        self,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object] = (),
    ) -> BigQueryQueryResult:
        self._raise_if_closed()
        module = self._bigquery_module() if self._bigquery is None else self._bigquery
        try:
            job_config = self._query_job_config(module, sql=sql, parameters=parameters)
            job = self._client.query(sql, job_config=job_config)
            rows = job.result()
            self._capture_session_id(job)
        except Exception as exc:
            raise BigQueryConnectionError("BigQuery SQL execution failed.") from exc
        return BigQueryQueryResult(
            rows=rows, job=job, read_client_factory=lambda: self.raw_read_client
        )

    def executemany(
        self,
        sql: str,
        parameters: Sequence[Sequence[object] | Mapping[str, object]],
    ) -> BigQueryQueryResult:
        result: BigQueryQueryResult | None = None
        for row in parameters:
            result = self.execute(sql, row)
        if result is None:
            return self.execute(sql, ())
        return result

    def commit(self) -> None:
        self.execute("commit transaction")

    def rollback(self) -> None:
        self.execute("rollback transaction")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for client in (self._client, self._read_client):
            if client is not None and hasattr(client, "close"):
                client.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return (
            f"{type(self).__name__}(project={self.project!r}, "
            f"location={self.location!r}, state={state!r})"
        )

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise BigQueryConnectionError("BigQuery SQL connection is closed.")

    def _query_job_config(
        self,
        module: Any,
        *,
        sql: str,
        parameters: Sequence[object] | Mapping[str, object],
    ) -> Any:
        job_config = module.QueryJobConfig(
            query_parameters=_query_parameters(module, sql, parameters)
        )
        if not self._use_session:
            return job_config
        if self._session_id is None:
            job_config.create_session = True
            return job_config
        job_config.connection_properties = [
            module.ConnectionProperty(key="session_id", value=self._session_id)
        ]
        return job_config

    def _capture_session_id(self, job: Any) -> None:
        session_info = getattr(job, "session_info", None)
        session_id = getattr(session_info, "session_id", None)
        if session_id:
            self._session_id = str(session_id)

    @staticmethod
    def _bigquery_module(import_module: Any = importlib.import_module) -> Any:
        return _bigquery_module(import_module=import_module)

    @staticmethod
    def _bigquery_storage_module(import_module: Any = importlib.import_module) -> Any:
        return _bigquery_storage_module(import_module=import_module)


class BigQueryQueryResult:
    def __init__(self, *, rows: Any, job: Any, read_client_factory: Any) -> None:
        self._rows = rows
        self._job = job
        self._read_client_factory = read_client_factory

    @property
    def raw_rows(self) -> Any:
        return self._rows

    @property
    def raw_job(self) -> Any:
        return self._job

    def fetchone(self) -> Any:
        rows = self.fetchall()
        if not rows:
            return None
        return rows[0]

    def fetchall(self) -> list[Any]:
        if hasattr(self._rows, "to_dataframe_iterable"):
            return [tuple(row.values()) if isinstance(row, Mapping) else row for row in self._rows]
        return list(self._rows)

    def fetch_arrow_all(self) -> Any:
        if hasattr(self._rows, "to_arrow"):
            return self._rows.to_arrow(bqstorage_client=self._read_client_factory())
        if hasattr(self._job, "to_arrow"):
            return self._job.to_arrow(bqstorage_client=self._read_client_factory())
        import pyarrow as pa  # type: ignore[import-untyped]

        return pa.Table.from_pylist([])

    def fetch_arrow_batches(self) -> Any:
        return self.fetch_arrow_all().to_batches()

    def to_arrow_reader(self, *, batch_size: int | None = None) -> Any:
        table = self.fetch_arrow_all()
        return _ArrowTableReader(table, batch_size=batch_size)

    def close(self) -> None:
        if hasattr(self._rows, "close"):
            self._rows.close()


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


def _bigquery_module(import_module: Any = importlib.import_module) -> Any:
    try:
        return import_module("google.cloud.bigquery")
    except ImportError as exc:
        raise BigQueryConnectionError(
            "BigQuery SQL connections require the optional `bigquery` dependency. "
            "Install it with `retl[bigquery]`."
        ) from exc


def _bigquery_storage_module(import_module: Any = importlib.import_module) -> Any:
    try:
        return import_module("google.cloud.bigquery_storage_v1")
    except ImportError as exc:
        raise BigQueryConnectionError(
            "BigQuery Arrow reads require the optional `bigquery` dependency. "
            "Install it with `retl[bigquery]`."
        ) from exc


def _query_parameters(
    bigquery_module: Any,
    sql: str,
    parameters: Sequence[object] | Mapping[str, object],
) -> list[Any]:
    items: tuple[tuple[str | None, object], ...]
    if isinstance(parameters, Mapping):
        items = tuple(parameters.items())
    else:
        if isinstance(parameters, str | bytes | bytearray):
            raise TypeError("BigQuery SQL parameters must be a sequence or mapping.")
        names = _parameter_names(sql)
        items = tuple(
            (names[index - 1] if index <= len(names) else None, value)
            for index, value in enumerate(parameters, start=1)
        )
    return [
        bigquery_module.ScalarQueryParameter(name, _bigquery_type(value), value)
        for name, value in items
    ]


def _bigquery_type(value: object) -> str:
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, bytes):
        return "BYTES"
    if isinstance(value, dt.datetime):
        return "TIMESTAMP"
    if isinstance(value, dt.date):
        return "DATE"
    return "STRING"


def _parameter_names(sql: str) -> tuple[str, ...]:
    names: list[str] = []
    for match in re.finditer(r"@([A-Za-z_][A-Za-z0-9_]*)", sql):
        name = match.group(1)
        if name not in names:
            names.append(name)
    return tuple(names)


__all__ = [
    "BigQueryConnection",
    "BigQueryConnectionError",
    "BigQueryQueryResult",
    "_bigquery_module",
    "_bigquery_storage_module",
]
