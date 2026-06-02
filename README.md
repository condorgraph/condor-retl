# condor-retl

Sync warehouse data to downstream partners from plain Python.

## Quick Start

Install the core library with a source backend and destination connector:

```sh
pip install "condor-retl[duckdb]" condor-retl-meta
```

Declare the rows to send and run the sync:

```python
import retl
from retl.backends.duckdb import DuckDBSqlBackend


db = DuckDBSqlBackend("warehouse.duckdb", source_schema="main", runtime_schema="retl")

source = retl.source(
    name="newsletter_customers",
    backend=db.source_backend(),
    query="""
    select customer_id, email
    from customers
    where email is not null
    """,
)

audience = retl.state(
    name="newsletter_audience",
    source=source,
    key={"customer_id": "customer_id"},
    identifiers=[{"type": "email", "value": "email"}],
    target=retl.target("newsletter_customers"),
)

meta = retl.destinations.load(
    "retl/meta",
    binding_name="meta_primary",
    credential_namespace="destinations.meta",
    config_namespace="destinations.meta",
)

sync = retl.sync(
    name="newsletter_to_meta",
    declaration=audience,
    destination=meta,
    surface="custom_audiences",
)

runner = retl.runner(
    name="newsletter_to_meta",
    runtime_store=db.runtime_store(),
)

result = runner.run(sync, dry_run=True)

print(result.to_text())
```

Change `dry_run=True` to `dry_run=False` when the plan looks right.

Use a different source backend or partner connector when you need one:

```sh
pip install "condor-retl[snowflake]"
pip install "condor-retl[bigquery]"
pip install "condor-retl[databricks]"
pip install "condor-retl[postgresql]"

pip install condor-retl-klaviyo
pip install condor-retl-google-ads-data-manager
pip install condor-retl-bing-ads
pip install condor-retl-tiktok-ads
```

The distribution is `condor-retl`; the Python package is `retl`.

## Configuration

By default, RETL reads config and secrets from environment variables. The Meta
example above expects:

```sh
export DESTINATIONS__META__ACCESS_TOKEN="..."
export DESTINATIONS__META__AD_ACCOUNT_ID="act_..."
```

## What You Declare

- `source`: the SQL rows to read
- `state`: current facts, such as audience membership or profile attributes
- `event`: occurred facts, such as purchases or signups
- `sync`: one state or event declaration bound to one partner surface
- `destination`: the partner connector and account configuration

RETL runs syncs through durable `collect -> stage -> reconcile -> sync` phases,
so dry runs, retries, and operator evidence use the same declarations as
production sends.

## Partners

First-party connector packages currently cover:

| Package | Connector ref |
| --- | --- |
| `condor-retl-meta` | `retl/meta` |
| `condor-retl-klaviyo` | `retl/klaviyo` |
| `condor-retl-google-ads-data-manager` | `retl/google-ads-data-manager` |
| `condor-retl-bing-ads` | `retl/bing-ads` |
| `condor-retl-tiktok-ads` | `retl/tiktok-ads` |
| `condor-retl-file` | `retl/file` |
