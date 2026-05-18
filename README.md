# condor-retl

`condor-retl` is a Python reverse ETL library for declaring syncs from
warehouse-shaped rows into destination-facing mutations.

Users write Python declarations for five primitives:

- **Source**: a SQL-backed set of source rows
- **State**: desired current facts such as profile attributes or audience
  membership
- **Event**: occurred facts such as purchases, signups, or lifecycle events
- **Sync**: one State or Event declaration bound to one destination surface
- **Destination Surface**: a connector-owned API contract such as Meta Custom
  Audiences or Klaviyo profiles

The runtime executes those declarations through durable `collect -> stage ->
reconcile -> sync` phases with retry-aware ledger state, dry-run planning, and
bounded operator evidence.

This project is in the initial `0.1.x` alpha line. APIs are intended to be
usable, but the package is still early and may change before a stable release.

## User Documentation

The Docusaurus user-docs source lives in
[`docs-site/`](./docs-site/). From that directory, run:

```sh
npm install
npm run dev
npm run build
```

The root [`docs/`](./docs/) tree remains the repository control-plane,
contributor, and implementation-contract documentation surface.

## Installation

Install the core package:

```sh
pip install condor-retl
```

Install the source backend extras you need:

```sh
pip install "condor-retl[duckdb]"
pip install "condor-retl[snowflake]"
pip install "condor-retl[bigquery]"
pip install "condor-retl[databricks]"
pip install "condor-retl[postgresql]"
```

Install destination connectors as separate packages:

```sh
pip install condor-retl-meta
pip install condor-retl-klaviyo
pip install condor-retl-google-ads-data-manager
pip install condor-retl-bing-ads
pip install condor-retl-tiktok-ads
```

The distribution package is named `condor-retl`; the Python import package is
`retl`:

```python
import retl
```

## AI Skills

Install RETL's user-facing AI skills into a project:

```sh
retl install-skills .
```

By default this installs the same packaged skills into `.agents/skills/` and
`.claude/skills/` in the target project.

The installed `retl-start-project` skill guides an AI agent to create the
right local project shape for the user's actual source backend, destination,
and operating model.

## Quickstart

This is the smallest useful shape for sending one email audience to Meta. The
SQL only needs an `email` column; `retl.target(...)` names the logical Meta
Custom Audience for every row.
`dry_run=True` plans the Meta request without writing to Meta; change it to
`False` when you are ready to send.

```python
import retl
from retl.backends.duckdb import DuckDBSqlBackend


db = DuckDBSqlBackend(
    database="warehouse.duckdb",
    source_schema="main",
    runtime_schema="retl",
)

audience = retl.state(
    name="newsletter_audience",
    source=retl.source(
        name="newsletter_customers",
        mode="snapshot",
        backend=db.source_backend(),
        query="""
        select email
        from customers
        where email is not null
        """,
    ),
    key={"customer_id": "email"},
    target=retl.target("newsletter_customers"),
    identifiers=[{"type": "email", "value": "email"}],
)

meta = retl.destinations.load(
    "retl/meta",
    binding_name="meta_primary",
    credential_namespace="destinations.meta",
    config_namespace="destinations.meta",
)

result = retl.runner(
    name="send_newsletter_audience",
    runtime_store=db.runtime_store(),
).run(
    retl.sync(
        name="newsletter_audience_to_meta",
        declaration=audience,
        destination=meta,
        surface="custom_audiences",
    ),
    dry_run=True,
)
print(result.to_text())
```

For local development, the default config and secret resolvers read environment
variables. The example above expects values such as:

```sh
export DESTINATIONS__META__ACCESS_TOKEN="..."
export DESTINATIONS__META__AD_ACCOUNT_ID="act_..."
```

## Core Model

RETL separates source shaping from destination delivery.

Source SQL owns joins, aggregation, dedupe, filtering, and other grain changes.
RETL declarations map already-shaped rows into State or Event intent.

State is for desired current facts:

```python
customer_state = retl.state(
    name="customer_state",
    source=customers,
    key={"customer_id": "customer_id"},
    identifiers=[{"type": "email", "value": "email"}],
    payload={"plan": "plan"},
)
```

Event is for occurred facts from checkpointed sources:

```python
purchase_events = retl.event(
    name="purchase",
    source=purchases,
    key={"purchase_id": "purchase_id"},
    occurred_at="purchased_at",
    identifiers=[{"type": "email", "value": "email"}],
    payload={"order_total": "order_total", "currency": "currency"},
)
```

Identifier mappings support one value or a list of values:

```python
identifiers=[
    {"type": "email", "value": "primary_email"},
    {"type": "email", "values": "all_emails"},
]
```

Both forms produce flat canonical Identifier objects shaped like
`{"type": "...", "value": "..."}`.

## Execution

The normal execution entrypoints are:

- `runner.run(sync)` for one Sync
- `runner.run(sync, dry_run=True)` for planning without irreversible
  destination writes or progress advancement
- `runner.run_many([sync_a, sync_b])` for an explicit Sync set that can share
  collect and stage work
- `runner.dismiss_unresolved(sync)` for intentionally dismissing unresolved
  destination batches in one Sync destination scope

Runtime recovery is ledger-first. Later runs retry old `pending` batches and
retryable `failed` batches before scanning new work. Terminal outcomes such as
`succeeded`, `accepted`, and `skipped` are not retried automatically.

## Destination Connectors

First-party partner destination connectors are published separately from the
core runtime. They expose connector refs through the `retl.destinations` entry
point group, then user code loads them by ref:

```python
retl.destinations.load("retl/meta", binding_name="meta_primary")
```

Core RETL also ships two built-in local connectors, `retl/mock` and
`retl/reference`. These are proof and authoring surfaces for tests, examples,
and new connector development. They are not partner integrations and are not
the way operators install a production destination. `retl/mock` is the core
runtime test double for synthetic destination outcomes.

Current first-party connector packages:

| Package | Connector ref | Surfaces |
| --- | --- | --- |
| `condor-retl-meta` | `retl/meta` | Meta Custom Audiences and Conversions API events |
| `condor-retl-klaviyo` | `retl/klaviyo` | Klaviyo profiles and list memberships |
| `condor-retl-google-ads-data-manager` | `retl/google-ads-data-manager` | Google Ads Customer Match and Data Manager events |
| `condor-retl-bing-ads` | `retl/bing-ads` | Microsoft Advertising Customer Match Customer Lists |
| `condor-retl-tiktok-ads` | `retl/tiktok-ads` | TikTok Ads Custom Audience membership |
The `retl/reference-http` connector remains a repo-local reference package for
tests and connector authors. It is the package-shaped proof path for HTTP
request planning, dry runs, injected transport, receipts, and destination batch
ledger behavior, and is not published to PyPI.

Connector READMEs contain partner-specific auth, config, target, sandbox, and
surface details.

## Configuration And Secrets

Public config and secret material use stable dotted logical names. By default,
the environment resolver maps names to uppercase environment variables with
double underscores:

```python
retl.config["destinations.meta.ad_account_id"]
retl.secrets["destinations.meta.access_token"]
```

becomes:

```sh
DESTINATIONS__META__AD_ACCOUNT_ID=act_123
DESTINATIONS__META__ACCESS_TOKEN=...
```

Local scripts can configure TOML-backed resolvers without changing destination
declarations:

```python
retl.configure(
    config_resolver=retl.ChainedConfigResolver(
        retl.TomlConfigResolver("retl.local.toml"),
        retl.EnvironmentConfigResolver(),
    ),
    secret_resolver=retl.TomlSecretResolver("retl.local.toml"),
)
```

## Development

This repository uses [`uv`](https://docs.astral.sh/uv/) for the local
environment, dependency lock, builds, and publishing.

Set up the development environment:

```sh
make dev
```

Run the default verification baseline:

```sh
make check
```

`make check` runs formatting, linting, typechecking, tests, repository skeleton
validation, and architecture validation.

If package names or workspace members changed, resync before checking:

```sh
uv sync --all-extras --group dev
make check
```

Build the core package:

```sh
make build-library
```

Build one destination connector package:

```sh
make build-destination-connector PACKAGE=meta
```

## Documentation

Start with the durable docs index:

- [Docs Index](docs/index.md)
- [Product](docs/product.md)
- [Runtime](docs/runtime.md)
- [Recovery](docs/recovery.md)
- [Destinations](docs/destinations.md)
- [Examples](docs/examples.md)

Repository policy, architecture, and agent workflow details live in
[Control Plane](docs/control-plane.md).

## License

Unless a package-level license file or package metadata says otherwise, this
repository is licensed under Elastic-2.0.

The core `condor-retl` package is licensed under the
[Elastic License 2.0](LICENSE-Elastic-2.0.txt). First-party destination
connector packages distributed by Condor are licensed under the
Apache License 2.0; the authoritative license for any connector is the
`LICENSE` and `pyproject.toml` metadata shipped with that package.
