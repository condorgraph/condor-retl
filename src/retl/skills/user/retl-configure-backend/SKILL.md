---
name: retl-configure-backend
description: Configure RETL SQL backends, Source and Runtime relation spaces, naming defaults, and runtime-store creation in an end-user project.
---

# Configure A RETL Backend

Use this skill when creating or changing RETL backend config, Source backends,
runtime stores, runtime-operation commands, or generated project examples.

## Core Rules

1. Use `[backends.<backend>]` for SQL backend config. Do not generate separate
   `[sources.<backend>]` and `[runtime.<backend>]` namespaces.
2. A RETL SQL backend is one coherent execution context with a read-only Source
   relation space and a RETL-owned Runtime relation space.
3. Source and Runtime relation spaces must be distinct.
4. Runtime relation spaces are RETL-owned. Use `retl` as the default runtime
   schema or dataset name, adapted to backend casing conventions.
5. Source relation spaces belong to the user's data model. Do not invent source
   project, catalog, database, schema, or dataset names for non-local examples.
6. Create Source adapters and runtime stores from the same backend object when
   the backend exposes both roles.

When generating a project, write the selected backend's `[backends.<backend>]`
placeholder block into committed `retl.toml` unless an equivalent block already
exists. Keep required unknown operator values as `REPLACE_ME`; use concrete
safe local defaults only for local engines where the path or schema is
repo-local and non-secret. Do not leave backend config as comments when the user
has selected a backend.

## Backend Defaults

### DuckDB

DuckDB uses one physical `.duckdb` file. Split Source and Runtime by schema, not
by database file.

```toml
[backends.duckdb]
database = "data/warehouse.duckdb"
source_schema = "main"
runtime_schema = "retl"
```

```python
from retl.backends.duckdb import DuckDBSqlBackend


backend = DuckDBSqlBackend(
    database="data/warehouse.duckdb",
    source_schema="main",
    runtime_schema="retl",
)
source_backend = backend.source_backend()
runtime_store = backend.runtime_store()
```

Never generate a DuckDB config with one source database file and one runtime
database file for executable SQL collect.

### PostgreSQL

PostgreSQL uses one server/database connection. Split Source and Runtime by
schema.

```toml
[backends.postgresql]
host = "localhost"
port = 5432
database = "app"
source_schema = "public"
runtime_schema = "retl"
sslmode = "require"
```

Use `sslmode = "verify-full"` when PostgreSQL certificate and hostname
validation are configured. Use `sslmode = "disable"` only for local or sandbox
databases that intentionally do not use TLS.

### Snowflake

Snowflake uses one account and warehouse. Source and Runtime database/schema
pairs must be explicit and distinct.

```toml
[backends.snowflake]
account = "REPLACE_ME"
warehouse = "REPLACE_ME"
source_database = "REPLACE_ME"
source_schema = "PUBLIC"
runtime_database = "REPLACE_ME"
runtime_schema = "RETL"
```

### BigQuery

BigQuery uses one project/location client context. Source and Runtime
project/dataset pairs must be explicit and distinct. Do not guess the source
project or dataset.

```toml
[backends.bigquery]
project = "REPLACE_ME"
source_project = "REPLACE_ME"
source_dataset = "REPLACE_ME"
runtime_project = "REPLACE_ME"
runtime_dataset = "retl"
location = "US"
```

### Databricks

Databricks uses one workspace and SQL warehouse. Source and Runtime
catalog/schema pairs must be explicit and distinct. Do not guess the source
catalog or schema.

```toml
[backends.databricks]
server_hostname = "REPLACE_ME"
http_path = "REPLACE_ME"
source_catalog = "REPLACE_ME"
source_schema = "REPLACE_ME"
runtime_catalog = "REPLACE_ME"
runtime_schema = "retl"
```

## Validation

Add or update tests that import the configured backend, construct the Source
backend and runtime store, and prove a dry-run path before live destination
mutation. For DuckDB, verify generated config has one `database` value and
distinct `source_schema` and `runtime_schema` values.
