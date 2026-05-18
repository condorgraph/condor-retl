"""DuckDB backend package exports."""

from __future__ import annotations

from retl.backends.duckdb.backend import DuckDBSqlBackend
from retl.backends.duckdb.connection import DuckDBConnection, DuckDBConnectionError
from retl.backends.duckdb.dialect import DUCKDB_DIALECT, DuckDBSqlDialect
from retl.backends.duckdb.source import (
    DuckDBSourceAdapter,
    DuckDBSourceBackend,
    duckdb,
)
from retl.backends.duckdb.source import (
    duckdb as source,
)
from retl.backends.duckdb.store import DuckDBRuntimeStore, DuckDbRuntimeStore

__all__ = [
    "DUCKDB_DIALECT",
    "DuckDBConnection",
    "DuckDBConnectionError",
    "DuckDBRuntimeStore",
    "DuckDBSourceAdapter",
    "DuckDBSourceBackend",
    "DuckDBSqlBackend",
    "DuckDBSqlDialect",
    "DuckDbRuntimeStore",
    "duckdb",
    "source",
]
