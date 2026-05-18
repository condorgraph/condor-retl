"""Snowflake backend package exports."""

from __future__ import annotations

from retl.backends.snowflake.auth import (
    SNOWFLAKE_AUTH_MODES,
    SNOWFLAKE_KEY_PAIR_AUTH,
    SNOWFLAKE_PASSWORD_AUTH,
    SnowflakeBackendAuth,
)
from retl.backends.snowflake.backend import SnowflakeSqlBackend
from retl.backends.snowflake.connection import SnowflakeConnection, SnowflakeConnectionError
from retl.backends.snowflake.dialect import SNOWFLAKE_DIALECT, SnowflakeSqlDialect
from retl.backends.snowflake.schema import (
    initialize_snowflake_runtime_schema,
    restore_next_attempt_number,
)
from retl.backends.snowflake.source import (
    SnowflakeSourceAdapter,
    SnowflakeSourceBackend,
    snowflake,
)
from retl.backends.snowflake.source import (
    snowflake as source,
)
from retl.backends.snowflake.store import SnowflakeRuntimeStore

__all__ = [
    "SNOWFLAKE_DIALECT",
    "SNOWFLAKE_AUTH_MODES",
    "SNOWFLAKE_KEY_PAIR_AUTH",
    "SNOWFLAKE_PASSWORD_AUTH",
    "SnowflakeBackendAuth",
    "SnowflakeConnection",
    "SnowflakeConnectionError",
    "SnowflakeRuntimeStore",
    "SnowflakeSourceAdapter",
    "SnowflakeSourceBackend",
    "SnowflakeSqlDialect",
    "SnowflakeSqlBackend",
    "snowflake",
    "source",
    "initialize_snowflake_runtime_schema",
    "restore_next_attempt_number",
]
