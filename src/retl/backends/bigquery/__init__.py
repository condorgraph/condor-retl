"""BigQuery backend package exports."""

from __future__ import annotations

from retl.backends.bigquery.auth import (
    BIGQUERY_APPLICATION_DEFAULT_AUTH,
    BIGQUERY_AUTH_MODES,
    BIGQUERY_SERVICE_ACCOUNT_FILE_AUTH,
    BIGQUERY_SERVICE_ACCOUNT_JSON_AUTH,
    BigQueryBackendAuth,
)
from retl.backends.bigquery.backend import BigQuerySqlBackend
from retl.backends.bigquery.connection import BigQueryConnection, BigQueryConnectionError
from retl.backends.bigquery.dialect import BIGQUERY_DIALECT, BigQuerySqlDialect
from retl.backends.bigquery.schema import (
    initialize_bigquery_runtime_schema,
    restore_next_attempt_number,
)
from retl.backends.bigquery.source import (
    BigQuerySourceAdapter,
    BigQuerySourceBackend,
    bigquery,
)
from retl.backends.bigquery.source import (
    bigquery as source,
)
from retl.backends.bigquery.store import BigQueryRuntimeStore

__all__ = [
    "BIGQUERY_APPLICATION_DEFAULT_AUTH",
    "BIGQUERY_AUTH_MODES",
    "BIGQUERY_DIALECT",
    "BIGQUERY_SERVICE_ACCOUNT_FILE_AUTH",
    "BIGQUERY_SERVICE_ACCOUNT_JSON_AUTH",
    "BigQueryBackendAuth",
    "BigQueryConnection",
    "BigQueryConnectionError",
    "BigQueryRuntimeStore",
    "BigQuerySourceAdapter",
    "BigQuerySourceBackend",
    "BigQuerySqlBackend",
    "BigQuerySqlDialect",
    "bigquery",
    "source",
    "initialize_bigquery_runtime_schema",
    "restore_next_attempt_number",
]
