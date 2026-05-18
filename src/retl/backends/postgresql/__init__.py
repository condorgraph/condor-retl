"""PostgreSQL backend package exports."""

from __future__ import annotations

from retl.backends.postgresql.auth import (
    POSTGRESQL_AUTH_MODES,
    POSTGRESQL_PASSWORD_AUTH,
    PostgreSqlBackendAuth,
)
from retl.backends.postgresql.backend import PostgreSqlBackend
from retl.backends.postgresql.connection import PostgreSqlConnection, PostgreSqlConnectionError
from retl.backends.postgresql.dialect import POSTGRESQL_DIALECT, PostgreSqlDialect
from retl.backends.postgresql.schema import (
    initialize_postgresql_runtime_schema,
    restore_next_attempt_number,
)
from retl.backends.postgresql.source import (
    PostgreSqlSourceAdapter,
    PostgreSqlSourceBackend,
    postgresql,
)
from retl.backends.postgresql.source import (
    postgresql as source,
)
from retl.backends.postgresql.store import PostgreSqlRuntimeStore

__all__ = [
    "POSTGRESQL_AUTH_MODES",
    "POSTGRESQL_DIALECT",
    "POSTGRESQL_PASSWORD_AUTH",
    "PostgreSqlBackend",
    "PostgreSqlBackendAuth",
    "PostgreSqlConnection",
    "PostgreSqlConnectionError",
    "PostgreSqlDialect",
    "PostgreSqlRuntimeStore",
    "PostgreSqlSourceAdapter",
    "PostgreSqlSourceBackend",
    "initialize_postgresql_runtime_schema",
    "postgresql",
    "restore_next_attempt_number",
    "source",
]
