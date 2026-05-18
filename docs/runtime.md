# Runtime

RETL runtime turns authored **State** and **Event** declarations into
destination-facing work. Users execute through `runner.run(...)` and
`runner.run_many(...)`; `collect`, `stage`, `reconcile`, and `sync` are
internal phase contracts.

The data-plane contract is a bounded work runtime. Source reads first produce
RETL-owned current state, ordered State work, or bounded Event keyset work in
SQL/columnar storage. Python and destination connectors consume bounded pages
only after the runtime has reduced source rows to canonical work.

## Execution Shape

Every run follows the same high-level flow:

1. `collect` reads a State snapshot or Event source keyset window and produces
   declaration-scoped Runtime relation evidence.
2. `stage` reads bounded work pages for one Sync from either pending ordered
   work after destination progress or current state as upserts for explicit
   resend-all behavior.
3. `reconcile` packages staged work into destination-neutral State Operation or
   Event Import pages.
4. `sync` splits reconciled pages into destination batches, submits those
   batches through the selected **Destination Surface**, records the destination
   batch ledger, and advances the destination scan cursor after durable batch
   ledger state exists for the scanned work.

For State declarations, `run_many` may share collect output for the same Source
and declaration. Event declarations plan destination-scoped source keyset
ranges instead of sharing durable collect output. Progress belongs to one Sync,
destination, surface, declaration family, and declaration name because
destination application, delivery outcomes, failures, and recovery are
destination-specific.

The runtime returns a `RunResult` with grouped upstream results, compact
per-Sync results, and report references:

- `runner_name`, `status`, and `dry_run`
- `source_groups` for grouped collect evidence
- `declaration_stages` for grouped stage evidence
- `syncs` for per-Sync `SyncResult` records
- `sync_reports` and `report_references`, one report per Sync; each report
  includes the unique SQL `sync_reports.report_id`
- `run_index` and `run_index_reference` for `run_many`
- aggregate `progress_advanced` and `irreversible_writes` flags

`RunResult.to_text()` returns a printable summary that combines the in-memory
Run Index, concise Sync Result lines, and each Sync Report with stable
redaction applied by the underlying report renderers.

DuckDB runtime stores must persist `runs`, declaration provenance, each compact
Sync Report index, destination progress, receipts where applicable, Target
Registry rows where applicable, and the destination batch ledger as durable
runtime evidence. The report rows keep redacted compact JSON and query columns
for run id, attempt id, Sync, status, counts, failure category, HTTP status,
latest redacted error detail, and progress advancement. Destination batch
ledger rows are the durable current-state delivery, retry, and outcome state.
The Run Index is public/in-memory rendering only and is not a runtime-store
table.

DuckDB runtime stores are also the default durable Target Registry for runner
execution. Target registry rows are scoped by binding name, destination ref,
surface, and logical Target, and store only the resolved remote target id,
display name, bounded metadata, and source. Explicit Target Mappings still win
over registry records. Dry runs may report planned managed Target creation, but
they do not create remote destination objects or write target registry rows.
Connectors that need destination auth for managed Target lookup or creation
receive a resolved-auth managed-target client at the sync boundary before
target resolution runs.

DuckDB runtime stores also persist execution and declaration provenance:

- `runs` records one row for each `runner.run(...)` or `runner.run_many(...)`
  invocation, keyed by `run_id`, with runner name, best-effort script path,
  best-effort script content hash, final status, `dry_run`, start time, and
  completion timestamp when the run reaches a terminal state.
- `declarations` records authored State/Event metadata keyed by declaration
  name and `declaration_version_id`. Declaration name is the continuity
  identity for progress. The version id is an audit fingerprint of the current
  declaration shape and does not reset progress by itself.

Persisted declaration metadata is sanitized canonical JSON. It may include
source identity, source query hash, field mappings, checkpoint mapping, and
credential-presence style evidence, but it must not include raw credential
values, access tokens, Authorization headers, cookies, private keys, client
secrets, or auth-bearing URLs.

Destination binding construction may expand `credential_namespace` into
`SecretRef` values and `config_namespace` into public config values before a
Sync is submitted. Namespace expansion is validation and reference construction
only: credential secrets are still resolved only at the sync submission
boundary, after dry-run request planning has remained auth-free.
If the active resolvers are backed by TOML, this timing does not change:
public config strings may be read while constructing bindings, but TOML secret
values remain behind `SecretRef` resolution until the runtime auth boundary.

Each `SyncResult` includes `sync_name`, declaration identity fields,
destination binding name when available, selected `surface`, run options, and
`collect`, `stage`, `reconcile`, and `sync` phase statuses. It also carries the
attempt id, report reference, destination submission counts, delivery outcome
decision reason, commit decisions, progress evidence, and Event keyset evidence
when Event work is involved.

## SQL Backend Relation Spaces

Executable SQL collect uses one SQL backend execution context for each collect.
That context contains two relation spaces:

- **Source relation space**: read-only from RETL's perspective. It contains the
  authored Source relations and is not mutated by RETL runtime code.
- **Runtime relation space**: read/write and RETL-owned. It contains current
  state, ordered work, Event import work, destination progress, destination
  batch ledgers, run/declaration provenance, reports, receipts, recovery
  markers, and Target Registry rows.

`collect` is the only runtime phase that may read Source relations and write
Runtime relations in the same execution. After collect, `stage`, `reconcile`,
`sync`, recovery, report, progress, receipt, and registry behavior must operate
from Runtime relations only. They must not re-read Source relations to rebuild
work, confirm state, repair progress, or resolve destination delivery state.

For DuckDB, executable SQL collect uses one physical DuckDB database with
separate Source and Runtime schemas. DuckDB cross-database Source reads through
`ATTACH` are outside the runtime contract, and the Source schema and Runtime
schema must be distinct. Generated SQL that writes runtime-owned tables must
qualify those tables to the Runtime schema.

User-facing DuckDB examples and skills must generate a single backend namespace
with one database path and distinct schemas:

```toml
[backends.duckdb]
database = "data/warehouse.duckdb"
source_schema = "main"
runtime_schema = "retl"
```

For Snowflake, executable SQL collect uses one Snowflake account and warehouse
execution context with explicit Source and Runtime database/schema relation
spaces. The Source database/schema is read-only from RETL's perspective; the
Runtime database/schema is read/write and RETL-owned. The Source and Runtime
database/schema pair must be distinct, and runtime-owned tables must be
qualified to the Runtime relation space. RETL does not provision Snowflake
accounts, warehouses, databases, or source schemas as part of the runtime
contract.

Snowflake backend authentication uses shared native Auth Modes from
`retl.auth`. Public settings such as account, warehouse, source database,
runtime database, optional source schema, and optional runtime schema belong
under a `backends.snowflake` config namespace. When omitted, `source_schema`
defaults to `PUBLIC` and `runtime_schema` defaults to `RETL`; RETL does not
derive Snowflake schemas from a prefix. Credential values belong under the
selected credential namespace, such as `backends.snowflake.password` or
`backends.snowflake.key_pair`, and resolve only when a Snowflake source or
runtime connection opens. Snowflake key-pair auth accepts exactly one private
key source: inline `private_key` material or `private_key_path`, which is read
from disk at connection-open time. Runtime stores, reports, traces, and
evidence must not persist resolved Snowflake passwords, private keys,
passphrases, or other secret material.

For BigQuery, executable SQL collect uses one Google Cloud project/location
client context with explicit Source and Runtime project/dataset relation
spaces. The Source project/dataset is read-only from RETL's perspective; the
Runtime project/dataset is read/write and RETL-owned. The Source and Runtime
project/dataset pair must be distinct, and runtime-owned tables must be
qualified to the Runtime relation space. RETL does not provision Google Cloud
projects, billing, permissions, or source datasets as part of the runtime
contract. BigQuery project IDs may use Google-native hyphenated names such as
`example-analytics-project`; datasets and RETL-owned table names remain simple SQL
identifiers.

BigQuery backend execution uses the official Google Python clients:
`google.cloud.bigquery.Client` for query execution, DDL/DML, jobs, and table
metadata, and `google.cloud.bigquery_storage_v1.BigQueryReadClient` for
high-throughput Arrow reads through the BigQuery Storage API. BigQuery backend
authentication uses application default credentials by default or a
service-account JSON credential through a selected credential namespace.
Credential values belong under namespaces such as
`backends.bigquery.service_account` and resolve only when a BigQuery source or
runtime connection opens. Runtime stores, reports, traces, and evidence must
not persist resolved service-account JSON, private keys, OAuth tokens, or other
secret material.

For Databricks, executable SQL collect uses one workspace plus one SQL
warehouse execution context with explicit Unity Catalog Source and Runtime
catalog/schema relation spaces. The Source catalog/schema is read-only from
RETL's perspective; the Runtime catalog/schema is read/write and RETL-owned.
The Source and Runtime catalog/schema pair must be distinct, and
runtime-owned tables must be fully qualified to the Runtime relation space.
RETL supports only Unity Catalog managed Delta tables in the first Databricks
backend slice; `hive_metastore`, jobs compute, and schema/catalog switching are
outside the contract. Databricks backend authentication uses PAT credentials
for local/dev/sandbox usage or OAuth M2M credentials for production/unattended
usage. Credential values belong under namespaces such as `backends.databricks.pat`
or `backends.databricks.oauth_m2m` and resolve only when a Databricks source or
runtime connection opens. Runtime stores, reports, traces, and evidence must
not persist resolved Databricks tokens, client secrets, OAuth material, or other
secret material.

For PostgreSQL, executable SQL collect uses one PostgreSQL server/database
connection context with explicit Source and Runtime schema relation spaces. The
Source schema is read-only from RETL's perspective; the Runtime schema is
read/write and RETL-owned. The Source and Runtime schemas must be distinct, and
runtime-owned tables must be qualified to the Runtime schema. RETL does not
provision PostgreSQL servers, databases, roles, extensions, or source schemas
as part of the runtime contract. PostgreSQL backend authentication uses
password credentials under namespaces such as `backends.postgresql.password`,
resolved only when a PostgreSQL source or runtime connection opens. Runtime
stores, reports, traces, and evidence must not persist resolved PostgreSQL
passwords or other secret material. PostgreSQL backend config defaults to
`sslmode = "require"` so password-backed connections are encrypted unless an
operator explicitly opts out; use `sslmode = "verify-full"` when certificate
and hostname validation are configured.

User-facing backend config defaults are:

| Backend | Config namespace | Source defaults | Runtime defaults |
| --- | --- | --- | --- |
| DuckDB | `[backends.duckdb]` | `database = "data/warehouse.duckdb"`, `source_schema = "main"` | same database, `runtime_schema = "retl"` |
| PostgreSQL | `[backends.postgresql]` | `source_schema = "public"`, `sslmode = "require"` | same database, `runtime_schema = "retl"` |
| Snowflake | `[backends.snowflake]` | `source_schema = "PUBLIC"` | `runtime_schema = "RETL"` |
| BigQuery | `[backends.bigquery]` | explicit `project`, `source_project`, and `source_dataset` | explicit `runtime_project`, `runtime_dataset = "retl"` |
| Databricks | `[backends.databricks]` | explicit `source_catalog` and `source_schema` | explicit `runtime_catalog`, `runtime_schema = "retl"` |

Generated config must use `[backends.<backend>]` for SQL backend configuration.
It must not generate separate `[sources.<backend>]` and `[runtime.<backend>]`
namespaces for executable SQL collect.

Runtime-operation CLI commands reconstruct runtime stores from repeatable
backend inputs. DuckDB operations use an explicit `--database` path and
optional `--schema` for the runtime store. Snowflake operations use
`--namespace`, `--auth-mode`, and optional `--credential-namespace`; the CLI
constructs `SnowflakeSqlBackend.from_config(namespace=..., auth_mode=...,
credential_namespace=...)` and then calls `runtime_store()` for that invocation.
BigQuery operations use `--backend bigquery`, `--bigquery-namespace`,
`--auth-mode`, and optional `--credential-namespace`; the CLI constructs
`BigQuerySqlBackend.from_config(...)` and then calls `runtime_store()` for that
invocation. Databricks operations use `--backend databricks`,
`--databricks-namespace`, `--auth-mode`, and optional
`--credential-namespace`; the CLI constructs
`DatabricksSqlBackend.from_config(...)` and then calls `runtime_store()` for
that invocation. PostgreSQL operations use `--backend postgresql`,
`--postgresql-namespace`, `--auth-mode`, and optional
`--credential-namespace`; the CLI constructs
`PostgreSqlBackend.from_config(...)` and then calls `runtime_store()` for that
invocation. The CLI never accepts a live Snowflake, BigQuery, Databricks, or
PostgreSQL connection object, and output must not serialize resolved warehouse
credentials.

Scoped skip commands keep State and Event progress authorities separate.
`skip-ordered-work-range` is State-only and accepts collect-id plus
sequence-order bounds. `skip-event-keyset-range` is Event-only and accepts
first, last, upper, and optional exclusive lower source-keyset positions. Each
Event position supplies `cursor` and `primary-key` scalar kind/value pairs with
kinds `null`, `boolean`, `integer`, `number`, or `string`; the CLI validates the
kind explicitly and does not infer scalar types from shell strings. Within one
Event source range, every supplied cursor bound must use the same scalar kind
and every supplied primary-key bound must use the same scalar kind. Destination
batch storage persists one `event_cursor_kind` and one
`event_primary_key_kind` for the whole Event source range, so mixed-kind Event
range bounds are invalid.

The DuckDB contract does not include a legacy fallback, shim, alias, adapter
bridge, or deprecation window for the old independently configured DuckDB Source
database plus DuckDB Runtime database pairing. Code or configuration that
depends on that old pairing must fail loudly rather than being automatically
converted to the current placement model.

Future SQL backends may place Source and Runtime relation spaces in separate
databases or catalogs only when that separation is native to one coherent SQL
backend execution context. RETL must not synthesize cross-backend collect by
joining independently configured Source and Runtime databases behind a
compatibility layer.

Generated SQL for SQL backends is built through RETL-owned SQL contracts backed
by SQLGlot expression trees and dialect rendering. SQLGlot is the generated SQL
AST layer for SELECTs, predicates, projections, ordering, source-query wrapping,
and other query construction where it applies. RETL still owns parameter
allocation, relation-space validation, runtime table ownership, connection and
transaction behavior, progress, and destination ledger semantics. Runtime
values must remain bound as driver parameters where supported; SQLGlot parsing
or rendering must not inline runtime values into generated SQL. Backend-specific
differences that SQLGlot does not abstract cleanly, such as JSON construction,
hashing, canonical key scalar rendering, temp table behavior, DDL, or
upsert/merge behavior, must be explicit backend capabilities rather than
scattered string patches.

## Collect

`collect` executes a Source according to its explicit **Source Mode**.

Runtime executes collect inside the SQL backend's single execution context,
reading only the Source relation space and writing only the Runtime relation
space. Shared runtime modules depend on SQL backend placement and runtime-store
contracts. Backend specifics such as driver clients, connection behavior,
dialect capabilities, DDL, and runtime-store wiring stay behind the selected
backend implementation.

For `mode="snapshot"`, collect produces State current state and ordered State
work for the declaration. State declarations require snapshot sources.

State collect is always a producer. It records the source/declaration current
state for the collect and emits collect-scoped ordered work rows for new,
changed, and missing State identities:

- `upsert` work for inserted or changed State identities
- `remove` work for identities that disappear from the authoritative snapshot

When one collect emits both operation kinds, all `remove` work is ordered before
all `upsert` work within that collect. Within each operation kind, ordered work
is grouped by logical Target, then by canonical State identity. Runtime staging
must preserve this order so destination request planning can process a
contiguous run of records for the same operation and Target instead of
interleaving targets one record at a time.

Target projection preserves the declaration form at the Source SQL boundary:
`target="audience_key"` reads a source column and serializes canonical
`target_json`, while `target=retl.target("newsletter_customers")` serializes
the same canonical `{"value": ...}` shape without requiring a source column.
Static and column target declarations are distinct in declaration provenance so
changing the static logical target changes the State declaration identity.

Whether a given Sync later sends or skips `remove` work is controlled by Sync
removal policy during reconcile. Collect does not branch by destination or by a
resend-all request.

For `mode="checkpointed"`, Event planning and replay read bounded Source SQL
windows using a source-native keyset such as `(cursor_value,
primary_key_value)`. Event declarations require checkpointed sources.
Checkpointed sources declare the cursor and primary-key column names plus their
scalar types, so runtime SQL can carry explicit Event position columns without
backend-specific row type inspection.

Source Mode is not inferred from downstream declarations. Core RETL validates
that State uses snapshot sources and Event uses checkpointed sources before
runtime work proceeds.

SQL backend placement prepares read-only Source relation handles for collect.
It does not mutate Source relations or own RETL operational state. Collect uses
those handles to read Source relations and write Runtime relations. Current
State, State ordered work rows, Event range ledger evidence, destination scan
progress, recovery markers, reports, receipts, and Target registries are
Runtime relation responsibilities even when Source and Runtime relation spaces
share the same physical database.

Arrow is not the normal collect handoff for SQL-capable runner execution.
Collect keeps source-row reduction in the backend and records results in
Runtime relations. The database-to-Arrow boundary starts when stage-facing
runtime-store reads pull bounded `RecordBatch` pages from those Runtime
relations. Backend packages own connector-specific result fetching; shared SQL
runtime helpers own bounded Arrow page consumption. This keeps native
connectors, ADBC, Flight SQL, BigQuery Storage Read API, and local engines
behind the same stage-read contract without making ADBC a mandatory dependency.

## Ordered Work

Ordered work rows are declaration-scoped State runtime records produced by
collect. They are grouped by a monotonic `collect_id` assigned by the
runtime and ordered within that collect by deterministic `sequence_order`.
Incremental State ordering authority is `(collect_id, sequence_order)`.

For the same declaration, source state, and progress boundary, collect must
produce the same State ordered-work order. Stable destination batch identity
depends on this order: State batches should keep records for the same operation
and logical Target together where batch-size limits allow, with `remove` rows
before `upsert` rows for a collect. The destination batch ledger must not try
to repair nondeterministic input order after reconciliation or request batching.

State ordered work records carry canonical State identity, Target when present,
Identifiers, Payload, operation kind, fingerprint evidence, source/declaration
current-state reference, `collect_id`, and `sequence_order`.

Event work uses source-native keyset ordering, usually `(cursor_value,
primary_key_value)`, for destination scan progress and replay. Event collect
evidence may carry a `collect_id` for reporting and audit, but Event rows are
not written to `ordered_work`; source keyset ranges in the destination batch
ledger are the replay authority.

Ordered work is collect-scoped. It is retained until all relevant destination
scan cursors and unresolved destination batch ledger rows allow compaction
under explicit retention policy.

## Stage

`stage` produces bounded work pages for one Sync. State staging does not re-read
Source relations. Event staging reads the checkpointed Source SQL range selected
from destination progress or a stored destination batch ledger range. Stage does
not perform destination diffing.

Normal staging reads work after that Sync's destination scan cursor. The cursor
is scoped to `(Sync, destination, surface, declaration family, declaration
name)`.

For State, normal incremental staging reads ordered work after a structured
`ordered_work` scan position containing `collect_id` and
`sequence_order`. This is also the normal path for a destination scope with no
progress: it stages retained ordered work from the beginning of the available
ordered-work stream. That default is the right initial-run behavior
because the first collect already emits durable ordered upserts for current
source rows.

Explicit `resend_all=True` uses a separate `current_snapshot` scan mode over
live current state in deterministic canonical key order. Current-snapshot scans
send upserts only. They do not read ordered work, replay historical diff rows,
reconstruct removes or intermediate State transitions, or synthesize
`sequence_order`; they carry a structured canonical key position. Resend-all is
an explicit current-state send, not a baseline-and-join operation. It does not
establish or advance the normal `ordered_work` lower bound for a later
incremental run. Live current-state scanning is an accepted consistency gap for
this runtime contract: rows that change while the scan is in progress may be
sent with current values or picked up by later incremental work depending on
timing.

A State destination added after history already exists should use explicit
`resend_all=True` when it needs a current baseline. The runtime does not
promise automatic current-state bootstrap for destination scopes with no
progress, and it does not automatically hand off from a current-state resend to
ordered-work progress. Operators adding a later State destination must choose
between retained ordered-work replay, explicit current-state resend, and any
separate scoped skip, reset, or rebaseline operation needed to avoid later
historical replay.

For Event, stage reads Event Import work from Source SQL after the destination's
source-native scan cursor and records the planned `(lower, upper]` keyset range
on destination batch rows. Source Checkpoint is not a separate active Event
runtime progress table.

Staging validates declaration shape. Destination Surface validation is separate:
it checks whether the staged declaration can be sent through the selected
destination-specific surface.

Heavy data shaping belongs upstream of RETL. Source SQL or upstream modeling
should perform joins, aggregation, grain changes, and partner-specific field
preparation before rows reach the Destination Connector.

During SQL collect, Identifier mappings compile into one canonical
`identifiers_json` array. Scalar mappings such as
`{"type": "email", "value": "email"}` emit one `{"type": "email", "value": ...}`
object from the named source column. List-valued mappings such as
`{"type": "email", "values": "emails"}` expand one source array/list column
into zero or more objects of the same canonical shape. The runtime does not
store nested list values inside an Identifier object.

List-valued Identifier expansion is SQL-backend work for executable SQL
collect. DuckDB and Snowflake dialect helpers own backend-specific list
expansion and JSON construction. Expanded list items are sorted by canonical
scalar value for stable State fingerprints and destination batch identity.
Null source lists and empty lists emit no Identifier objects; non-list values
for `values` fail collect with a clear runtime error. Duplicate list items are
not deduped in this slice. Blank string source values are emitted as scalar
Identifier values rather than filtered. Source lists must use `values`; a
scalar `value` mapping over a source list is not supported.

Destination request item counting is separate from canonical Identifier
construction. A list-valued Identifier still becomes a flat sequence of normal
Identifier objects on the RETL work record. The selected Destination Surface and
connector own whether those Identifier objects render as one partner row, many
partner rows, or another partner-specific request item shape.

## Reconcile

`reconcile` is per Sync. It packages staged work into destination-neutral pages
for the selected Destination Surface.

For State, reconcile produces **State Operation** pages:

- `upsert` for pending State upsert work or resend-all current-state upserts
- `remove` for pending State remove work when Sync removal policy sends removes

Sync removal policy controls missing-row behavior for that Sync, destination,
and surface. In the current declaration model, the Sync-selected State surface
is the policy source: surfaces that support `remove` receive remove operations,
while upsert-only surfaces suppress removes. When policy skips removes,
reconcile suppresses remove operations for that Sync without deleting ordered
work rows that may still be needed by other Syncs.

For Event, reconcile produces **Event Import** pages. Events are occurred facts,
not desired current state, and therefore do not use removal policy or Target
routing.

Runner-level reconcile batch defaults control the destination-neutral work
batches produced by this phase for both State Operations and Event imports:

```python
retl.runner(name="crm_to_lifecycle", reconcile_batch_max_rows=10_000)
```

The default is 1,000 rows per reconcile work batch with no byte limit. This is
separate from Source read batching and from destination request payload limits,
which remain connector-owned.

The runtime has three distinct batching layers:

- Source read batching controls how upstream rows are collected into current
  State or checkpointed Event windows.
- Runner reconcile batching controls destination-neutral State Operation or
  Event Import work pages.
- Destination request batching controls partner request payloads after
  reconcile, using connector-owned policies such as
  `RequestBatchingPolicy.max_rows`.

Destination request batching must preserve the reconciled row order for each
Sync and must not change destination scan progress or retention semantics.
By default one reconciled work record is one destination request item. A
connector may provide a columnar request item count hook over the Arrow
reconcile page before `DestinationWorkRecord` materialization when one RETL
work record can render multiple partner request items. The planner sums those
counts for `RequestBatchingPolicy.max_rows`, while source ranges, progress
ranges, and destination batch identity continue to cover whole RETL work
records. A single work record whose rendered request item count exceeds the
surface limit fails planning rather than being split across destination
batches. `RequestBatchingPolicy.max_bytes` remains an additional rendered body
limit and does not split a single work record.

Runtime owns iteration across planned destination request batches. Connector
hooks submit and classify one selected request batch at a time; they do not
decide that a failed batch stops later selected batches in the page. The
Sync's `on_failure` policy controls whether runtime continues to the next
request batch after terminal, retryable, or pre-acceptance failure evidence.

After reconcile, runtime and connector request planning produce the third
runtime grain: the **destination batch**. The runtime grains are:

- **ordered work**: incremental State progress input, ordered by
  `(collect_id, sequence_order)`
- **reconcile page**: destination-neutral State Operation or Event Import
  packaging
- **destination batch**: durable delivery, retry, and outcome unit for one
  Sync, destination, surface, declaration continuity identity, reconcile scope,
  and connector request batch

Destination batch identity includes the Sync, destination, surface, declaration
continuity identity, explicit source range coordinates, request batch index,
payload fingerprint, and a redacted target/request fingerprint. Incremental
State batches include ordered-work coordinates; State current-snapshot batches
include canonical key ranges; Event batches include source-native keyset
ranges. It must exclude raw request bodies and secret-bearing material.

## Target Resolution

If State includes a **Target**, Target is part of State identity and work scope
by default. Missing-row removal is scoped to that full identity, so a record
removed from one target produces removal work only for that target.

Target resolution happens before mutation submission. Runtime resolves logical
Targets in this order:

1. explicit Target Mappings on the destination binding
2. existing Target Registry records
3. managed find-or-create when the selected Destination Surface supports
   managed targets

Sync execution must not create targets inline per row. The logical Target value
is the default destination display name for Managed Target creation unless
optional target metadata provides another display name.

## Sync

`sync` submits reconciled work through the selected Destination Surface.

The Destination Surface declares supported operations and a successful
`delivery_outcome` of `accepted` or `succeeded`. Runtime validates incompatible
combinations before submission, such as
a Sync removal policy that sends removals through a surface that only supports
upsert. Runtime also validates declaration Identifier types against the
selected surface's accepted Identifier set and explicit `any_of` or `all_of`
Identifier requirements before connector submission hooks can mutate the
destination.

Destination auth resolves at the sync boundary after auth mode selection and
required credential-field validation. Runner execution uses the active secret
resolver automatically; users do not load secrets into `Runner`. Runtime
re-resolves `SecretRef` values through the active secret resolver for fresh
workers, replay, restore, and recovery. `SecretLiteral` values are accepted only
as explicit process-local credential material. Resolved credential values are
process-local submission inputs only. Connector hooks may translate them into
SDK clients, driver connection arguments, request headers, cookies, query
parameters, or token exchange calls, but runtime evidence may persist only
redacted auth evidence. Resolved secret values, tokens, auth headers, cookies,
private keys, client secrets, and auth-bearing query strings are ephemeral and
must not be stored on Destination Binding, Sync Report, receipts, manifests,
traces, logs, diagnostics, or rendered Run Index output.
TOML-backed secret resolution is a provider choice, not a persistence surface:
runtime must treat values returned by `TomlSecretResolver` exactly like values
returned by environment or embedding-owned secret resolvers.

OAuth token exchange and JWT signing also happen at the sync boundary through
connector-owned hooks. Runtime may call those hooks to obtain ephemeral bearer
auth, but it must persist only redacted auth evidence.

Surfaces with `delivery_outcome="succeeded"` return definitive success or
failure evidence during submission. Surfaces with
`delivery_outcome="accepted"` return accepted delivery evidence, such as queued
or started remote work, without final success confirmation. Remote tracking and
later accepted-batch finalization are outside this runtime contract.

HTTP-backed connector packages may use the shared destination HTTP toolkit at
this boundary. Runtime validates the selected Destination Surface and resolves
Auth Modes before calling connector-owned submission hooks. Those hooks receive
ephemeral resolved auth and reconciled State/Event work, execute only through
connector-owned or injected transport, and return bounded Destination
Submission Evidence for receipts, accepted delivery, retryable failures,
non-retryable failures, and pre-acceptance failures. Dry Run plans
toolkit-backed requests but must not invoke transport or persist receipts.
DuckDB stores compact submission diagnostics on Sync Report rows and current
delivery state on destination batch ledger rows, including pre-acceptance
failure category, HTTP status, counts, latest redacted error detail, retry
eligibility, and progress decisions when present.

Every executed destination batch is represented in the destination batch ledger
before or at submission time. Each attempt updates that batch with attempt
number, lifecycle status, counts, redacted partner diagnostics, and retry
eligibility. Destination batch statuses are `pending`, `accepted`, `succeeded`,
`failed`, and `skipped`. `succeeded` means synchronous or definitive success.
`accepted` means non-definitive accepted delivery. The runtime treats both
`accepted` and `succeeded` as resolved. `skipped` is terminal ledger coverage
for a batch or range intentionally not sent or retried. Failed retry behavior is
metadata on a `failed` batch, not a separate lifecycle status.

`pending` rows are durable planned-batch coverage, not an in-flight attempt
log. Planned rows may remain at `attempt_count=0`; `attempt_count` reflects
submission touches and outcomes that have been recorded back into the current
batch row. Because the simplified model does not persist a separate in-flight
attempt event before transport submission, a crash after RETL submits a batch
but before it records the outcome may leave the row retryable as `pending` or
with its previous current state. Duplicate delivery is the accepted
crash-recovery tradeoff for removing the historical attempt stream.

Within a single runner attempt, runtime-store reads establish an attempt-local
snapshot of the relevant ledger or SQL state. After the runtime reads ledger
records, it should carry those records forward in memory and use them as the
authoritative working set until it writes the next durable ledger update. The
runtime must not repeatedly re-query the same ledger rows inside the same phase
merely to reconfirm state it already read or just wrote. Re-reads are reserved
for explicit phase boundaries, retry or recovery entrypoints, conflict
detection, or places where another process may have intentionally changed the
durable state.

## Progress And Checkpoints

**Destination progress** is the per `(Sync, destination, surface, declaration
family, declaration name)` scan cursor used by stage and sync. The runtime
builds this scope from a real Sync declaration and destination binding.

The scan cursor records how far source work has been durably converted into
destination batch ledger rows for that same scope. It advances after durable
batch ledger coverage exists for the scanned page. Destination outcomes do not
hold the scan cursor back; pending, accepted, succeeded, failed, and skipped
outcomes are represented in the destination batch ledger. Do not store a
separate complete cursor. Complete-through, unresolved, and retryable summaries
are derived from destination batch rows when needed.

State scan progress has two modes. Incremental State uses structured
`ordered_work` positions compared by `(collect_id, sequence_order)`.
Resend-all uses structured `current_snapshot` positions compared by
deterministic canonical key order. Event scan progress uses source-native
keyset positions, usually `(cursor_value,
primary_key_value)`. The destination batch ledger stores Event lower and upper
keyset bounds and is the replay authority for retry. `collect_id` remains
provenance for collect evidence, reporting, and audit context; it is not
destination progress.

Retry is destination-scoped and bounded once per overall run before new scan
work for that destination scope. Retry selection reads old `pending` rows and
`failed` rows whose retry metadata allows automatic retry, up to the retry
limit. `accepted`, `succeeded`, and `skipped` rows are terminal for automatic
retry. After the bounded retry slice has been attempted, the destination
continues scanning after its scan cursor.

During current-run submission, RETL may also make a small conservative retry
for the same already planned destination request batch when the first outcome
is clearly retryable. The retry unit is the existing request plan and
destination batch identity: runtime does not rerun collect, stage, reconcile,
target resolution, or request planning for an in-run retry. In-run retry is
bounded by configured total attempts, maximum single `Retry-After`, cumulative
sleep budget for the Sync destination scope, and small jittered exponential
backoff when no short `Retry-After` is present. Long retry windows, exhausted
budgets, ambiguous commit evidence, and non-idempotent or unclear outcomes are
recorded as durable retryable failed ledger evidence for a later run instead
of holding the run open.

After a staged page has durable destination batch ledger coverage, runtime uses
the same `on_failure` policy to decide whether to drain the next staged page.
`continue_on_any` continues when progress is allowed and a next cursor exists,
even when the completed page contains failed destination batch evidence.
`stop_on_terminal` stops page draining after non-retryable failure evidence,
and `stop_on_any` stops after any destination failure evidence. Runtime still
stops when progress is blocked, no next cursor exists, or durable evidence
could not be recorded.

Destination progress is never shared by `run_many`. State Syncs can share
collect output only when the shared artifact does not depend on per-Sync runtime
state. Event Syncs plan source keyset ranges from destination-scoped progress.
Destination progress remains isolated even when work production is shared.

Changing declaration SQL, mappings, payload shape, or script content creates
new declaration version evidence when the canonical declaration fingerprint
changes, but it does not reset destination progress. Progress resets require
explicit operator intent or a new progress scope identity, such as a different
Sync, destination, surface, family, or declaration name.

## Retention And Cleanup

Retention is explicit runtime policy. Collect-scoped ordered work rows and the
current-state evidence they reference must remain available until relevant
destination scan cursors and unresolved destination batch ledger evidence allow
compaction.

Cleanup computes retention watermarks from relevant scan cursors plus
destination batch ledger evidence. Work at or below a safe watermark may be
compacted only according to explicit retention rules. Cleanup must not delete
pending ordered work, current state needed for current-snapshot staging,
destination progress, receipts needed for delivery evidence, unresolved
destination batch ledger records, or Target Registry records.

Runtime cleanup has separate safe and destructive surfaces:

- `cleanup-ordered-work` applies the retention watermark and unresolved-ledger
  blocker rules. It may accept a requested collect boundary or age boundary,
  reports both the requested and safe collect boundaries, and supports dry-run.
- `delete-ordered-work` is destructive ordered-work deletion for a declaration
  family. It is not retention cleanup and requires explicit force intent.
- `cleanup-cursors` TTL-deletes pagination cursor tokens from
  `pending_work_cursors` and `state_current_cursors`. Cursor rows are not
  retry, progress, current-state, or target authority.
- `cleanup-evidence` TTL-deletes diagnostic `runs` and `sync_reports` rows
  selected by age and optional scope filters. It must not alter destination
  progress, destination batches, current state, ordered work, or Target
  Registry rows.

Ordered-work cleanup can limit historical replay for State destinations added
after history already exists. Cleanup protects retained ordered work needed by
known destination scopes, but it does not preserve an indefinite history for
future scopes that have no progress yet. A late-added State destination that
needs a current baseline should use explicit `resend_all=True`; that path reads
`state_current` as upserts and is not a substitute for historical ordered-work
replay. Because current-snapshot progress is separate from ordered-work
progress, a later normal run for that destination may still see retained
ordered work unless the operator also performs an explicit scoped skip, reset,
or rebaseline operation.

## Runtime Logging

Runtime logs are live operator diagnostics for runner execution, phase
boundaries, destination compatibility, target resolution, request planning,
destination submission outcomes, failure classification, progress decisions, report
persistence, and recovery investigation. They use the standard Python
`logging` package under the `retl` logger namespace. Runtime modules should use
normal child loggers such as `retl.runtime.executor`.

RETL is a library. Importing `retl` must not configure root logging, add root
handlers, change root levels, or emit logs by default. Applications embedding
RETL retain normal Python logging control. RETL may provide an opt-in logging
setup helper for operator convenience, with both human-readable text formatting
and parseable JSON formatting.

Logs are not durable runtime authority. `runs`, compact Sync Report indexes,
destination batch ledger rows, destination scan progress, receipts, and Target
Registry records remain the authoritative surfaces for audit, replay, retry
selection, recovery, progress advancement, and destination delivery outcomes. A
log record may point to durable evidence with stable ids and bounded context,
but it must not replace that evidence or introduce a separate recovery truth.

Runtime log context should prefer stable, bounded fields when available:
`run_id`, `attempt_id`, `sync_name`, `declaration_name`,
`destination_binding_name`, `surface`, `phase`, `event`, `status`, and
aggregate counts. Logs must follow the same redaction and data minimization
boundary as state, reports, traces, and inspection artifacts. They must not
include raw source rows, raw canonical Payload values, raw identifiers, raw
request bodies, credentials, secrets, Authorization headers, cookies, private
keys, client secrets, auth-bearing URLs, unbounded partner responses,
account-level partner URLs, or full validation blobs. Partner diagnostics in
logs must be redacted, bounded, and sufficient only to find the corresponding
report or ledger row.

## Operator Console Progress

Operator console progress is a live, human-facing convenience for runner
execution. It is optional and quiet by default for library callers. The
`retl.console` namespace exposes renderers such as `retl.console.text(...)`
and `retl.console.null()`, and `retl.runner(...)` / `Runner(...)` accept an
optional `console=...` construction argument.

Console renderers consume bounded runtime events and counters, not formatted
log lines. Runner execution emits console callbacks for runner start and
completion, collect, stage, reconcile, destination submission, progress commit
decisions, report persistence, and Run Index references with stable ids, phase
statuses, and aggregate counts. Text console runs also emit bounded
destination-batch ledger update lines as current batch rows change, including
batch index, row count, status, completion state, retry eligibility, and
redacted partner diagnostics.

Console output is not durable runtime authority and must not become a second
copy of authoritative evidence. Runtime JSON or text logs, Sync Reports,
destination batch ledger rows, destination scan progress records, receipts, and
Target Registry records remain the separate diagnostic, audit, replay, retry,
recovery, progress, and delivery surfaces.

Console progress follows the same redaction and data minimization boundary as
logs and reports. It must not include raw source rows, raw canonical Payload
values, raw identifiers, raw request bodies, credentials, secrets,
Authorization headers, cookies, private keys, client secrets, auth-bearing
URLs, account-level partner URLs, unbounded partner responses, or full
validation blobs.

## Reports

Every Sync should produce one **Sync Report**. A Sync Report is a compact
runtime-store index for one declaration, destination, and Destination Surface.
It contains raw stable references, scopes, statuses, counts, and bounded reason
fields that let an operator or AI agent query SQL runtime-store authority.

Sync Reports may include:

- run id, attempt id, unique SQL `report_id`, stable report reference, Sync
  name, declaration identity, and destination binding/surface scope
- collect, stage, reconcile, and sync phase status
- page count, staged row count, operation/import counts, request-batch count,
  destination-batch count, and destination batch ids
- destination submission outcome counts, bounded failure category, HTTP status,
  and latest redacted error summary/detail
- Target resolution counts and progress scope/before/after/decision references
- dry-run, `on_failure`, progress advancement, and retention watermark facts

Sync Reports must not embed full stage pages, reconcile pages, destination
request bodies, raw receipts, raw partner responses, arbitrary evidence
objects, columnar samples, or replay transcripts. Page-by-page timeline detail
belongs in structured logs. Durable delivery, retry, target, receipt, and
progress truth belongs in SQL runtime-store tables.

DuckDB-backed runtime stores must persist report and ledger evidence in
operator-facing tables tied back to the run and declaration metadata tables:

- `sync_reports` stores one redacted compact Sync Report JSON document per run
  and attempt, keyed by `report_id` (`run_id:attempt_id:sync_name`), plus query
  columns for common failure investigation and declaration references. Failed
  reports may include `last_error_detail`, a redacted diagnostic string capped
  at 4096 characters for schema, auth, configuration, trace, or partner
  validation context.
- the destination batch ledger stores one durable delivery/retry/outcome record
  per destination batch, with stable identity, attempt state, lifecycle status,
  retry metadata, latest redacted diagnostics, and explicit source range
  coordinates.

Reports and destination batch rows carry `run_id` and stable declaration
references so operators can join runtime evidence back to the invocation that
created it and the authored declaration version observed during execution.

Pre-acceptance failures are durable report and ledger evidence when they are
tied to a planned destination batch. They are failed outcomes with retry
metadata.

For shared HTTP destination response classification, auth/access responses
`401`, `403`, and `407` are pre-acceptance failures. `429`, `408`, `425`,
`5xx`, and `599` are retryable failures and may carry retry-after evidence.
Other `4xx` responses, including `400`, `404`, and `422`, are non-retryable
failed submitted units by default. Runtime records that failed evidence at
destination batch grain, including whether the batch remains automatically
retryable.

Report data-plane references are index based. Reports carry `report_id`,
compact counts, progress positions, destination batch IDs, and bounded failure
reasons.
Destination batch rows carry request-batch identity, row counts, checksums,
payload fingerprints, status, retry metadata, and latest redacted diagnostics.
Structured logs carry page-level phase timing and counts. Partner detail is
sensitive operational evidence: it is sanitized for obvious auth and secret
material and capped before report and DuckDB persistence. Reports must not
store raw full Source rows, full current-state mirrors, full ordered-work
tables, full operation tables, full request bodies, auth-bearing values, raw
full partner response bodies, or stage/reconcile page payloads.

`run_many` produces a thin in-memory **Run Index** to connect shared upstream
work to per-Sync reports. Detailed destination evidence belongs in SQL
runtime-store tables and structured logs, not in a persisted Run Index or a
rich report object.

## Dry Run

Dry Run may collect, stage, reconcile, preflight, and translate records into
destination-shaped work. It must not make irreversible destination writes and
must not advance destination scan progress.

Dry Run should prove that the plan, declaration translation, target resolution,
and destination surface compatibility are coherent without pretending that
remote mutation succeeded.

Dry Run still produces Sync Reports and destination-shaped planning evidence,
but commit decisions must block destination progress advancement, receipts, and
retention compaction.

## Ledger-First Recovery

RETL has one recovery behavior. Destination progress records scanned work that
has durable destination batch ledger coverage; it does not mean every
destination effect succeeded.

A later run starts from the destination batch ledger for that Sync destination
scope. Before scanning new work, it retries old `pending` batches and
`failed` batches whose `retry_eligible` metadata and retry count allow another
attempt. It does not retry `accepted`, `succeeded`, or `skipped` batches.
This next-run sweep remains the durable recovery path after in-run retry is
exhausted or skipped due to long retry-after evidence or budget limits.

Operators can permanently dismiss actionable unresolved batches for one Sync
destination scope with `runner.operations.dismiss_unresolved(sync)`.
Dismissal maps matching `pending` and `failed` ledger rows to `skipped`
without deleting evidence and without affecting other Sync scopes. The direct
`runner.dismiss_unresolved(sync)` helper may remain only as delegation to the
operations surface.

Runtime operations also expose bounded inspection, scoped skip ranges, safe
ordered-work cleanup, destructive ordered-work deletion, cursor cleanup, scoped
reset and rebaseline helpers, Target Registry reset, and diagnostic run/report
cleanup. Reset, rebaseline, and hard delete operations mutate existing
authority tables and must account for shared collect output before deletion.
Retention remains authoritative: if ordered work needed for replay or skip
evidence has already been compacted, operations report the limit rather than
reconstructing work. `run_id` identifies diagnostic run evidence only; it is
not a restore point or rollback boundary.

## Failed Destination Batches

`on_failure` controls request-batch continuation, staged-page continuation, and
whether failed destination batches block Sync completion and destination
progress. It has exactly three values:

- `continue_on_any`: default; runtime keeps attempting later selected request
  batches after terminal, retryable, and pre-acceptance failures. Failed
  destination batches do not block progress once durable ledger evidence exists,
  and later staged pages continue when progress is allowed.
- `stop_on_terminal`: runtime continues after retryable failures when progress
  rules allow it, but non-retryable failed batches stop request-batch and
  staged-page continuation and block progress.
- `stop_on_any`: any failed destination batch stops request-batch and
  staged-page continuation and blocks progress.

Failed batches are still reported with aggregate counts, retry metadata, and
bounded, redacted samples so users and agents can understand what happened.

When non-retryable destination evidence applies to a destination batch in a
non-dry-run pending Sync, runtime records failed evidence at destination batch
grain before the attempt completes. Retryability is metadata on the failed
batch. Operator cleanup uses `runner.operations.dismiss_unresolved(sync)` to map
actionable unresolved `pending` and `failed` rows in that Sync destination
scope to `skipped` without deleting batch evidence.
