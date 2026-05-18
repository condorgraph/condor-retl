# Active Tests Root

This directory is the active repository-local proof root for the State/Event
rewrite.

## Snowflake Live Sandbox

`tests/backends/sandbox/test_snowflake_live_sandbox.py` is marked
`live_sandbox` and skips by default unless Snowflake sandbox configuration is
provided through environment variables.

Required:

- `BACKENDS__SNOWFLAKE__ACCOUNT`
- `BACKENDS__SNOWFLAKE__WAREHOUSE`
- `BACKENDS__SNOWFLAKE__SOURCE_DATABASE`
- `BACKENDS__SNOWFLAKE__RUNTIME_DATABASE`

Optional:

- `BACKENDS__SNOWFLAKE__AUTH_MODE`, defaults to `password`
- for password auth, provide `BACKENDS__SNOWFLAKE__PASSWORD__USER` and
  `BACKENDS__SNOWFLAKE__PASSWORD__PASSWORD`
- `BACKENDS__SNOWFLAKE__PASSWORD__ROLE`
- for key-pair auth, set `BACKENDS__SNOWFLAKE__AUTH_MODE=key_pair` and provide
  `BACKENDS__SNOWFLAKE__KEY_PAIR__USER` plus
  exactly one of `BACKENDS__SNOWFLAKE__KEY_PAIR__PRIVATE_KEY` or
  `BACKENDS__SNOWFLAKE__KEY_PAIR__PRIVATE_KEY_PATH`; optional key-pair fields are
  `BACKENDS__SNOWFLAKE__KEY_PAIR__PRIVATE_KEY_PASSPHRASE` and
  `BACKENDS__SNOWFLAKE__KEY_PAIR__ROLE`

The live Snowflake sandbox creates unique source and runtime schemas with a
test-only prefix, then drops those schemas with `cascade` during teardown. It
does not use `BACKENDS__SNOWFLAKE__SOURCE_SCHEMA` or
`BACKENDS__SNOWFLAKE__RUNTIME_SCHEMA`; those optional backend config values are
for normal local scripts and default to `PUBLIC` and `RETL`. Run it explicitly
with:

```bash
make test-sandbox-snowflake
```

## BigQuery Live Sandbox

`tests/backends/sandbox/test_bigquery_live_sandbox.py` is marked
`live_sandbox` and skips by default unless BigQuery sandbox configuration is
provided through environment variables.

Required:

- `RETL_BIGQUERY_PROJECT` or `BACKENDS__BIGQUERY__PROJECT`

Optional:

- `RETL_BIGQUERY_LOCATION`, defaults to `US`
- `RETL_BIGQUERY_SANDBOX_DATASET_PREFIX`, defaults to `retl_live`

The live BigQuery sandbox creates unique source and runtime datasets with a
test-only prefix, then drops those datasets with `cascade` during teardown. The
Make target uses application-default BigQuery credentials and falls back to
`gcloud config get-value project` when no project env var is set. Run it
explicitly with:

```bash
make test-sandbox-bigquery
```

## Databricks Live Sandbox

`tests/backends/sandbox/test_databricks_live_sandbox.py` is marked
`live_sandbox` and skips by default unless Databricks sandbox configuration is
provided through environment variables.

Required:

- `BACKENDS__DATABRICKS__SERVER_HOSTNAME`
- `BACKENDS__DATABRICKS__HTTP_PATH`
- `BACKENDS__DATABRICKS__SOURCE_CATALOG`
- `BACKENDS__DATABRICKS__SOURCE_SCHEMA`
- `BACKENDS__DATABRICKS__RUNTIME_CATALOG`
- `BACKENDS__DATABRICKS__RUNTIME_SCHEMA`
- `BACKENDS__DATABRICKS__PAT__TOKEN`

The live Databricks sandbox creates unique source and runtime schemas in the
configured Unity Catalog, then drops those schemas with `cascade` during
teardown. Run it explicitly with:

```bash
make test-sandbox-databricks
```

## PostgreSQL Live Sandbox

`tests/backends/sandbox/test_postgresql_live_sandbox.py` is marked
`live_sandbox` and skips by default unless PostgreSQL sandbox configuration is
provided through environment variables.

Required:

- `BACKENDS__POSTGRESQL__HOST`
- `BACKENDS__POSTGRESQL__PORT`
- `BACKENDS__POSTGRESQL__DATABASE`
- `BACKENDS__POSTGRESQL__PASSWORD__USER`
- `BACKENDS__POSTGRESQL__PASSWORD__PASSWORD`

Optional:

- `BACKENDS__POSTGRESQL__AUTH_MODE`, defaults to `password`
- `RETL_POSTGRESQL_SANDBOX_SCHEMA_PREFIX`, defaults to `retl_live`

The live PostgreSQL sandbox creates unique source and runtime schemas with a
test-only prefix, then drops those schemas with `cascade` during teardown. It
does not use fixed `BACKENDS__POSTGRESQL__SOURCE_SCHEMA` or
`BACKENDS__POSTGRESQL__RUNTIME_SCHEMA` values from local scripts. For the
default local Docker database, load `local/env/.env.postgresql-sandbox` and run:

```bash
make test-sandbox-postgresql
```
