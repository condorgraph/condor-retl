# Examples

These examples illustrate the active State/Event model. They are intentionally
compact and omit field-level schemas that are better owned by code.

## AI Skills

```bash
retl install-skills my-retl-project
```

The default skill destinations are `my-retl-project/.agents/skills/` and
`my-retl-project/.claude/skills/`. Use `--destination` to choose a single
custom project-local path. Existing changed skill files are overwritten from
the packaged copy.

Use the installed `retl-start-project` skill when an AI agent should create a
new RETL setup. The skill chooses a one-file or organized project shape based
on the user's existing repository, source backend, destination, and production
needs. Use the installed `retl-configure-backend` skill for SQL backend config
and Source/Runtime relation-space naming.

## Minimal State Sync

```python
import retl


customers = retl.source(
    name="customers",
    mode="snapshot",
    query="""
    select
        customer_id,
        email,
        plan
    from mart.customers
    """,
)

customer_state = retl.state(
    name="customer_state",
    source=customers,
    key={"customer_id": "customer_id"},
    identifiers=[{"type": "email", "value": "email"}],
    payload={"plan": "plan"},
)

braze = retl.destinations.load(
    "retl/braze",
    binding_name="braze_primary",
    credential_namespace="destinations.braze",
)

runner = retl.runner(name="crm_to_lifecycle")

runner.run(
    retl.sync(
        name="braze_customer_profiles",
        declaration=customer_state,
        destination=braze,
        surface="user_profile",
    )
)
```

For local development, set `DESTINATIONS__BRAZE__API_KEY` in the process
environment. In production, configure a read-only secret backend for the same
logical name; the destination declaration stays unchanged. When a test or
embedding process has already loaded secret material, use
`retl.secrets.literal(value)` as the explicit process-local credential value.
Explicit credential entries may still be supplied and override a namespace
field by field.

Local scripts may use explicit TOML-backed resolvers instead of environment
variables:

```toml
[destinations.braze]
api_key = "braze-secret"
account_id = "workspace-123"
```

```python
import retl

retl.configure(
    config_resolver=retl.ChainedConfigResolver(
        retl.TomlConfigResolver("retl.local.toml"),
        retl.EnvironmentConfigResolver(),
    ),
    secret_resolver=retl.TomlSecretResolver("retl.local.toml"),
)
```

`api_key` is still secret material only because the binding references
`retl.secrets["destinations.braze.api_key"]` or uses
`credential_namespace="destinations.braze"`; RETL does not infer credentials
from public config names.

State collect records current state and ordered work for inserts, changes, and
missing rows. Missing-row work is sent only when the Sync's removal policy and
the selected surface allow `remove`.

## Identifier Mappings

Use `value` when one source column contains one Identifier value:

```python
identifiers=[{"type": "email", "value": "email"}]
```

Use `values` when one source column contains an array/list of Identifier
values:

```python
customers = retl.source(
    name="customers",
    mode="snapshot",
    query="""
    select
        customer_id,
        emails,
        primary_phone,
        plan
    from mart.customers
    """,
)

customer_state = retl.state(
    name="customer_state",
    source=customers,
    key={"customer_id": "customer_id"},
    identifiers=[
        {"type": "email", "values": "emails"},
        {"type": "phone_e164", "value": "primary_phone"},
    ],
    payload={"plan": "plan"},
)
```

The canonical output is still a flat Identifier array, for example:

```json
[
  {"type": "email", "value": "a@example.test"},
  {"type": "email", "value": "b@example.test"},
  {"type": "phone_e164", "value": "+15550101000"}
]
```

List-valued Identifier items are sorted during SQL collect for stable
fingerprints. Null or empty source lists emit no Identifier objects, non-list
values fail collect, and duplicate list items remain duplicate Identifier
objects in this slice. Blank string source values are emitted as scalar values;
use Source SQL or connector/runtime validation when they should be rejected.

## Runtime Logging

Operators can opt in to RETL runtime logs with `retl.configure_logging(...)`.
Text output is useful for local runs and shells:

```python
import retl


retl.configure_logging(level="INFO", format="text")

runner.run(
    retl.sync(
        name="braze_customer_profiles",
        declaration=customer_state,
        destination=braze,
        surface="user_profile",
    )
)
```

JSON output is useful when a process supervisor or log collector expects
structured records:

```python
import retl


retl.configure_logging(level="INFO", format="json")
```

RETL logs use Python's standard `logging` package under the `retl` logger
namespace, with runtime records emitted by child loggers such as
`retl.runtime.executor`. Embedding applications may still configure handlers,
levels, filters, and propagation through ordinary Python logging.

Logs are live diagnostics for runner execution, phase boundaries, destination
submission, failures, progress decisions, and report persistence. They are not
durable authority. Use `runs`, compact Sync Report indexes, destination batch
ledgers, destination scan progress, receipts where applicable, and Target
Registry rows where applicable for audit, recovery, retry decisions, and
delivery evidence.

Log records expose stable ids, statuses, phases, and bounded counts where
available. They must not be used to inspect raw Source rows, canonical Payload
values, identifiers, request bodies, credentials, Authorization headers,
cookies, private keys, auth-bearing URLs, unbounded partner responses, or full
validation blobs. Operator diagnostics in logs should stay redacted and bounded
so they point to the corresponding report or ledger row without
becoming another copy of sensitive data.

## Operator Progress Output

The `retl.console` namespace provides optional human-facing console renderers
such as `retl.console.text(...)` and `retl.console.null()`. Runners accept
`console=...` during construction. Runtime execution emits bounded console
callbacks for operator progress, and console output is rendered from bounded
runtime events and counters, not from formatted log lines.

Operator progress can run alongside structured JSON logs without turning those
logs into the console UI:

```python
import retl


retl.configure_logging(level="INFO", format="json")

from retl.backends.duckdb import DuckDBSqlBackend


sql_backend = DuckDBSqlBackend(
    database=".retl/warehouse.duckdb",
    source_schema="source",
    runtime_schema="retl",
)

runner = retl.runner(
    name="crm_to_lifecycle",
    runtime_store=sql_backend.runtime_store(),
    console=retl.console.text(),
)

runner.run(
    retl.sync(
        name="braze_customer_profiles",
        declaration=customer_state,
        destination=braze,
        surface="user_profile",
    )
)
```

The console renderer writes a bounded operator summary such as:

```text
RETL run crm_to_lifecycle
run_id=0b2dd8d2-67ec-495c-af99-ec5d0e56c8f5 dry_run=false sync_count=1 collect_group_count=1

source group state:customer_state:customers:<source_identity>
  collect    succeeded  rows=2 collect_id=1 source=customers mode=snapshot

braze_customer_profiles -> braze_primary/user_profile
  stage      succeeded  rows=2 page=1 mode=pending progress_before=0
  reconcile  succeeded  operations=2 upserts=2 removes=0 imports=0 pages=1
  sync       confirmed  request_batches=0 destination_batches=1 attempted=2 confirmed=2 accepted=0
  progress   allowed    advanced=true allowed=true planned_batches=1 expected_batches=0
  summary    succeeded  operations=2 destination_batches=1 confirmed=2 accepted=0 progress_advanced=true report=sync-report:9088de7bef74db7a
  report     succeeded  report_reference=sync-report:9088de7bef74db7a

Run succeeded: syncs=1 succeeded=1 failed=0 partial=0 planned=0 confirmed=2 accepted=0 progress_advanced=true
Run index: run-index:85fb038e8a52774f
```

Use logs for live diagnostics and use `runs`, compact Sync Report indexes,
destination batch ledgers, destination scan progress, receipts, and Target
Registry rows for durable evidence, recovery, retry decisions, audit, and
delivery investigation.

Console output follows the same redaction and data minimization boundary as
logs and reports. It should show stable references, phase statuses, and counts
rather than raw Source rows, Payload values, identifiers, request bodies,
credentials, auth-bearing URLs, partner account URLs, unbounded partner
responses, or validation blobs.

## Runtime Operation CLI

Inspect a local DuckDB runtime store without importing a declaration script:

```bash
uv run retl operations inspect-runtime \
  --backend duckdb \
  --database .retl/state.duckdb \
  --schema retl
```

Inspect one destination scope with explicit scope fields:

```bash
uv run retl operations inspect-destination-scope \
  --backend duckdb \
  --database .retl/state.duckdb \
  --sync-name braze_customer_profiles \
  --destination-name braze_primary \
  --surface user_profile \
  --family state \
  --declaration-name customer_state
```

Snowflake runtime operations reconstruct the backend from public config and
credential namespaces:

```bash
uv run retl operations inspect-runtime \
  --backend snowflake \
  --namespace backends.snowflake \
  --auth-mode key_pair \
  --credential-namespace backends.snowflake.key_pair
```

The command reads public Snowflake settings from `backends.snowflake`, creates
credential `SecretRef` values from the credential namespace, and opens a fresh
runtime store for the invocation. Output is compact JSON and does not include
resolved secret values.

BigQuery runtime operations reconstruct the backend from public config and
application default credentials or a service-account credential namespace:

```bash
uv run retl operations inspect-runtime \
  --backend bigquery \
  --bigquery-namespace backends.bigquery \
  --auth-mode application_default
```

Skip a known-bad Event source-keyset range with explicit scalar kinds:

```bash
uv run retl operations skip-event-keyset-range \
  --backend duckdb \
  --database .retl/state.duckdb \
  --sync-name purchase_events \
  --destination-name braze_primary \
  --surface purchase_event \
  --family event \
  --declaration-name purchase \
  --first-cursor-kind string \
  --first-cursor-value 2026-01-01T00:00:00 \
  --first-primary-key-kind string \
  --first-primary-key-value purchase_1 \
  --last-cursor-kind string \
  --last-cursor-value 2026-01-02T00:00:00 \
  --last-primary-key-kind string \
  --last-primary-key-value purchase_2 \
  --upper-cursor-kind string \
  --upper-cursor-value 2026-01-02T00:00:00 \
  --upper-primary-key-kind string \
  --upper-primary-key-value purchase_2
```

Use `--lower-cursor-kind`, `--lower-cursor-value`,
`--lower-primary-key-kind`, and `--lower-primary-key-value` together when the
Event skip has an exclusive lower bound. Every cursor bound in one Event skip
range must use the same scalar kind, and every primary-key bound in that range
must use the same scalar kind. `skip-ordered-work-range` remains the State-only
command for collect-id and sequence-order ranges.

## Source Backend Boundary

Production Sources use SQL backend placement. Executable SQL collect runs
against one backend execution context with a read-only Source relation space
and a RETL-owned Runtime relation space. For DuckDB, both spaces live in one
physical database and are separated by schema; examples must not configure an
independent DuckDB Source database plus a separate DuckDB Runtime database.

The DuckDB execution contract is:

```text
SQL backend execution context: DuckDB database retl.duckdb
  Source relation space: schema mart, read-only from RETL
  Runtime relation space: schema retl_runtime, read/write and RETL-owned
```

Snowflake uses the same Source/Runtime contract inside one Snowflake account
and warehouse execution context, with explicit database/schema placement for
each relation space:

```python
from retl.backends.snowflake import SnowflakeBackendAuth, SnowflakeSqlBackend


sql_backend = SnowflakeSqlBackend(
    account="acme-prod",
    warehouse="RETL_WH",
    source_database="ANALYTICS",
    source_schema="MART",
    runtime_database="RETL",
    runtime_schema="RUNTIME",
    auth=SnowflakeBackendAuth.from_namespace(
        auth_mode="password",
        credential_namespace="backends.snowflake.password",
    ),
)

runner = retl.runner(
    name="snowflake_to_lifecycle",
    runtime_store=sql_backend.runtime_store(),
)
```

User-facing Snowflake scripts should normally read public backend settings from
`backends.snowflake` config and credential refs from the selected auth
namespace:

```python
sql_backend = SnowflakeSqlBackend.from_config(
    namespace="backends.snowflake",
    auth_mode="password",
    credential_namespace="backends.snowflake.password",
)
```

The `snowflake` optional dependency is required only when opening a Snowflake
connection. Backend construction, relation-space validation, and default tests
must remain importable without the Snowflake driver installed.

BigQuery uses the same Source/Runtime contract inside one Google Cloud
project/location client context, with explicit project/dataset placement for
each relation space:

```python
from retl.backends.bigquery import BigQuerySqlBackend


sql_backend = BigQuerySqlBackend.from_config(
    namespace="backends.bigquery",
    auth_mode="service_account_json",
    credential_namespace="backends.bigquery.service_account",
)
```

BigQuery SQL execution uses `google.cloud.bigquery.Client`; Arrow reads use
`google.cloud.bigquery_storage_v1.BigQueryReadClient`. The `bigquery` optional
dependency is required only when opening a BigQuery connection.

Fixture helpers such as `collect_fixture_snapshot(...)` and
`collect_fixture_checkpointed(...)` stay in tests and examples that need
deterministic in-memory Arrow tables.

## Resend-All Behavior

When explicit resend-all behavior is requested, stage reads current state as
bounded upsert work for the Sync. Resend-all is not a separate collect path, it
does not delete pending ordered work, and a successful resend-all execution does
not replay historical diffs. RETL scans live current state in deterministic
canonical key order and tracks that work with a current-snapshot scan position.

```python
runner.run(
    retl.sync(
        name="braze_customer_profiles_resend",
        declaration=customer_state,
        destination=braze,
        surface="user_profile",
    ),
    resend_all=True,
)
```

`resend_all=True` is State-only. Event Syncs reject it before mutation because
Events are occurred facts rather than current-state objects.

## Targeted State Sync

Targets route State rows to destination-specific objects. A State declaration
can omit Target, read a logical target from a source column, or use
`retl.target(...)` to send every row to one fixed logical target. If the
selected surface supports managed targets, RETL resolves or creates those
targets before submitting row mutations. The runner's runtime store is the
default durable Target Registry, so a target resolved on one run can be reused
by later runners pointing at the same runtime store.

```python
from retl.backends.duckdb import DuckDBSqlBackend


sql_backend = DuckDBSqlBackend(
    database=".retl/state.duckdb",
    source_schema="source",
    runtime_schema="retl",
)

audience_rows = retl.source(
    name="customer_audience_rows",
    mode="snapshot",
    backend=sql_backend.source_backend(),
    query="""
    select
        customer_id,
        email,
        audience_key
    from mart.customer_audience_rows
    """,
)

audience_state = retl.state(
    name="customer_audience_state",
    source=audience_rows,
    key={"customer": "customer_id"},
    target="audience_key",
    identifiers=[{"type": "email", "value": "email"}],
    payload={},
)

runner = retl.runner(
    name="audience_syncs",
    runtime_store=sql_backend.runtime_store(),
)

runner.run(
    retl.sync(
        name="braze_audience_membership",
        declaration=audience_state,
        destination=braze,
        surface="subscription_group_membership",
    )
)
```

For Meta Custom Audiences, a State target such as `audience_key` can be a
dynamic Customer File Custom Audience name. When the `custom_audiences` surface
is used without an explicit `TargetMapping`, RETL looks for or creates the Meta
audience before sending membership rows, then persists the resulting audience id
in the runtime Target Registry.

When all rows belong to one fixed Meta audience, keep the audience name out of
Source SQL and declare it explicitly:

```python
newsletter_audience = retl.state(
    name="newsletter_audience",
    source=retl.source(
        name="newsletter_customers",
        mode="snapshot",
        backend=sql_backend.source_backend(),
        query="""
        select customer_id, email
        from mart.newsletter_customers
        """,
    ),
    key={"customer_id": "customer_id"},
    target=retl.target("newsletter_customers"),
    identifiers=[{"type": "email", "value": "email"}],
)
```

When `audience_key` disappears for one customer and target, State collect emits
remove work only for that targeted State identity. The Sync's removal policy
decides whether that remove work is sent or skipped for the destination.

## Single Source, Multiple Destinations

For State declarations, `run_many` can share ordered work production, then
stages, reconciles, and submits independently for each Sync. Event Syncs plan
destination-scoped source keyset ranges instead of sharing durable ordered
work.

```python
klaviyo = retl.destinations.load(
    "retl/klaviyo",
    binding_name="klaviyo_primary",
    credential_namespace="destinations.klaviyo",
)

runner.run_many(
    [
        retl.sync(
            name="braze_customer_profiles",
            declaration=customer_state,
            destination=braze,
            surface="user_profile",
        ),
        retl.sync(
            name="klaviyo_profile_properties",
            declaration=customer_state,
            destination=klaviyo,
            surface="profile_properties",
        ),
    ]
)
```

Both Syncs can share the same Source collection and State ordered work. Each
Sync keeps separate destination scan progress, delivery outcome handling,
terminal failure handling, report output, and recovery behavior. When explicit
resend-all behavior is requested for a Sync, that Sync stages current state as
upserts without advancing normal progress:

```python
runner.run_many(
    [
        retl.sync(
            name="braze_customer_profiles_resend",
            declaration=customer_state,
            destination=braze,
            surface="user_profile",
        ),
        retl.sync(
            name="klaviyo_profile_properties_resend",
            declaration=customer_state,
            destination=klaviyo,
            surface="profile_properties",
        ),
    ],
    resend_all=True,
)
```

Runner reconcile batching and connector payload batching stay separate:

```python
from retl.destinations.request_batch import RequestBatchingPolicy

runner = retl.runner(
    name="crm_to_lifecycle",
    reconcile_batch_max_rows=10_000,
)

braze = retl.destinations.load(
    "retl/braze",
    binding_name="braze_primary",
    credentials={"api_key": retl.secrets["destinations.braze.api_key"]},
)

connector_payload_batches = RequestBatchingPolicy(max_rows=500)
```

Sync Reports expose compact runtime-store index fields such as `report_id`, run
and attempt ids, progress scope, phase status, counts, destination batch IDs,
and bounded failure reasons. Examples and local scripts should use those report
fields to query SQL runtime-store tables instead of loading full Source rows,
full current-state mirrors, ordered work tables, operation tables, raw request
bodies, or rich page payloads from reports.

## Event Sync

Events require a checkpointed Source and import bounded source windows ordered
by a source-native cursor and primary key.

```python
purchases = retl.source(
    name="purchase_events",
    mode="checkpointed",
    query="""
    select
        purchase_id,
        email,
        purchased_at,
        order_total,
        currency
    from mart.purchase_events
    """,
    checkpoint={
        "cursor": "purchased_at",
        "primary_key": "purchase_id",
        "cursor_type": "string",
        "primary_key_type": "string",
    },
)

purchase_events = retl.event(
    name="purchase",
    source=purchases,
    key={"purchase": "purchase_id"},
    occurred_at="purchased_at",
    identifiers=[{"type": "email", "value": "email"}],
    payload={"order_total": "order_total", "currency": "currency"},
)

runner.run(
    retl.sync(
        name="braze_purchase_events",
        declaration=purchase_events,
        destination=braze,
        surface="purchase_event",
    )
)
```

The authored Event Source SQL describes the base replayable relation. RETL
applies the checkpoint keyset predicate and `(cursor, primary_key)` ordering
when it plans and replays source ranges.

Event Syncs do not use State removal policy, resend-all staging, or Target
routing.

## Run Options

Run-level options control execution behavior without changing the declaration
or destination binding.

```python
runner.run(
    retl.sync(
        name="braze_customer_profiles",
        declaration=customer_state,
        destination=braze,
        surface="user_profile",
    ),
    dry_run=True,
)
```

`dry_run=True` may collect, stage, reconcile, preflight, and translate, but it
does not make irreversible destination writes or advance destination progress.

When a destination submission returns definitive success, runtime records the
batch as `succeeded`. When the destination accepts work without definitive
success evidence, runtime records the batch as `accepted`. Both are resolved
outcomes for runtime retry behavior.

Runtime recovery is ledger-first. A later run retries old `pending` batches and
retryable `failed` batches for the Sync destination scope before scanning new
work. Destination progress advances after durable batch ledger coverage exists
for scanned work; destination outcomes do not hold the scan cursor back.

Operators can permanently dismiss actionable unresolved batches for one Sync
destination scope:

```python
runner.operations.dismiss_unresolved(
    retl.sync(
        name="braze_customer_profiles",
        declaration=customer_state,
        destination=braze,
        surface="user_profile",
    )
)
```

Dismissed batches become `skipped` ledger rows and are not retried by later
runs. More generally, `skipped` records terminal ledger coverage for a batch or
range intentionally not sent or retried.

## Sync-Level Policies

```python
sync = retl.sync(
    name="braze_customer_profiles",
    declaration=customer_state,
    destination=braze,
    surface="user_profile",
    on_failure="continue_on_any",
)
```

Defaults are optimized for the common case:

- State collect produces ordered upsert and remove work
- Sync removal policy controls whether remove work is sent or skipped; exact
  public option names and values are intentionally not locked by this contract
  slice
- synchronous or definitive success records `succeeded`; non-definitive
  accepted delivery records `accepted`
- `on_failure="continue_on_any"` so failed request batches do not prevent later
  selected request batches or progress-allowed staged pages from running, and
  failed destination batches do not block progress once durable ledger evidence
  exists
- old `pending` and retryable `failed` destination batches are retried before
  new scan work; `skipped` batches are terminal
