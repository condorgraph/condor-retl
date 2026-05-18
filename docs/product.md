# Product

RETL declares reverse ETL work from source-shaped rows into destination-facing
mutations. The active public model is organized around five primitives:
**Source**, **State**, **Event**, **Sync**, and **Destination Surface**.

## Root API Surface

The stable public Python API is the `import retl` root export surface. Users
should import `retl` and access authoring constructors, registries, namespaces,
and common public types from that package root:

- `retl.ChainedConfigResolver`
- `retl.ChainedSecretResolver`
- `retl.Checkpoint`
- `retl.ConfigResolutionError`
- `retl.CredentialValue`
- `retl.Declaration`
- `retl.DeclarationValidationError`
- `retl.DestinationBinding`
- `retl.EnvironmentConfigResolver`
- `retl.Event`
- `retl.FailureHandlingMode`
- `retl.FieldMapping`
- `retl.Identifier`
- `retl.MappingConfigResolver`
- `retl.RetlError`
- `retl.RetlRuntimeNotImplementedError`
- `retl.RunResult`
- `retl.RunStatus`
- `retl.Runner`
- `retl.SecretLiteral`
- `retl.SecretRef`
- `retl.Source`
- `retl.SourceMode`
- `retl.State`
- `retl.StateOperation`
- `retl.StateTarget`
- `retl.StaticTarget`
- `retl.Sync`
- `retl.TomlConfigResolver`
- `retl.TomlSecretResolver`
- `retl.auth`
- `retl.config`
- `retl.console`
- `retl.configure`
- `retl.configure_logging`
- `retl.configured_secret_resolver`
- `retl.destinations`
- `retl.event`
- `retl.runner`
- `retl.secrets`
- `retl.source`
- `retl.sources`
- `retl.state`
- `retl.sync`
- `retl.target`

Users execute through `runner.run(...)` and `runner.run_many(...)`.
Runtime phases are implementation contracts, not root public functions or
direct root submodules. Canonical implementation modules live under their
ownership packages such as runtime, declarations, artifacts, state runtime,
events, sources, destinations, sync runtime, and stores.
Shared auth helpers are available as `retl.auth.*` after `import retl` and as
the canonical `import retl.auth` submodule. Auth helpers define credential
field contracts and runtime resolution behavior used by destinations and
backend-native connectors.
Public config resolves through the configured resolver protocol owned by
`retl.config`. The default resolver reads environment variables,
`retl.TomlConfigResolver` reads nested TOML scalar values as strings, and
`retl.ChainedConfigResolver` composes ordered providers with first-hit
precedence. Secret material resolves through the configured secret resolver and
the environment fallback exposed by `retl.configured_secret_resolver()`;
`retl.TomlSecretResolver` is an explicit read-only provider and does not change
authoring APIs or persistence rules.
Advanced source backend contracts live under `retl.sources`. Runtime result
and report internals live under the runtime results and reports modules.
Runtime operations and store contracts are available through explicit submodule
imports from their owning packages; they are not root-package exports.
Snowflake backend users configure public connection settings under
`backends.snowflake` and provide credentials through the selected native auth
credential namespace. Scripts must not embed Snowflake passwords, private keys,
or raw connection JSON.
BigQuery backend users configure public connection settings under
`backends.bigquery` and use application default credentials or provide
service-account credentials through the selected credential namespace. Scripts
must not embed service-account JSON, private keys, OAuth tokens, or other
Google credential material directly in declarations or runtime store
construction.

The `retl.console` namespace exposes optional, human-facing console renderers,
including `retl.console.text(...)` and `retl.console.null()`. Runners accept an
optional `console=...` construction argument. Runner execution emits bounded
runtime events and counters to the selected console renderer for operator
feedback. Console output does not replace logs, reports, ledgers, Run Indexes,
or destination progress records as live diagnostic output. Durable runtime
authority remains in `runs`, compact Sync Report indexes, destination batch
ledgers, destination progress, receipts where applicable, and Target Registry
rows where applicable.

`retl.destinations.load(...)` accepts explicit `credentials={...}` and
`config={...}` mappings, plus optional `credential_namespace` and
`config_namespace` arguments. Credential namespaces create `SecretRef` values
for the selected Auth Mode's required fields. Config namespaces read only fields
declared by the connector as namespace-loadable public config. Explicit mapping
entries override namespace-derived values field by field, and Target mappings
remain explicit. First-party partner connectors bake their production API
origins into connector code; users configure generic or private HTTP origins
through connectors built for that purpose, such as `retl/reference-http`.

## AI-Assisted Project Setup

Users may author declarations directly through `import retl`. RETL also ships
end-user AI skills for assisted setup and maintenance.

`retl install-skills [path]` installs the packaged end-user RETL AI skills into
the project-local `.agents/skills/` and `.claude/skills` directories by
default. These are product skills shipped with the `condor-retl` wheel. They
are separate from this repository's contributor skills under `.agents/skills/`,
which are used for developing RETL itself.

The `retl-start-project` skill is the setup path for new user projects. It
guides an AI agent to inspect the existing repository, ask for missing source
and destination details, choose a one-file or organized layout, and create
dry-run-first code and tests for the user's actual database and destination.
The `retl-configure-backend` skill is the setup path for SQL backend config,
Source and Runtime relation-space naming, and runtime-store construction.

Existing installed skill files are refreshed from the packaged copy by default;
unchanged files are idempotent.

## Source

A **Source** is a reusable declaration for reading rows from an upstream system.
It has an explicit **Source Mode**:

- `snapshot`: reads a point-in-time set of rows and treats that set as source
  authority for one run.
- `checkpointed`: reads a bounded Event window using source-native cursor and
  primary-key ordering.

Checkpointed Event retry depends on source retention. RETL stores the planned
source keyset range in the destination batch ledger and retries by re-reading
that Source SQL range; operators must keep source rows replayable for unresolved
Event ranges or explicitly skip/reset the affected ledger evidence.

Source mode is not inferred from downstream usage. RETL validates compatibility
when a declaration references a source.

```python
customers = retl.source(
    name="customers",
    mode="snapshot",
    query="select customer_id, email, plan from mart.customers",
)
```

## State

**State** is a desired current fact keyed by one or more logical fields. A State
declaration references a `snapshot` Source and describes how source rows become
state records.

State records contain:

- `key`: logical identity for the fact
- `identifiers`: destination-usable subject identities
- `payload`: user-defined data
- `target`: optional destination-facing routing key, either from a source
  column or from a static `retl.target(...)` declaration

```python
customer_state = retl.state(
    name="customer_state",
    source=customers,
    key={"customer": "customer_id"},
    identifiers=[{"type": "email", "value": "email"}],
    payload={"plan": "plan"},
)
```

State collect always records current state and ordered work for inserts,
changes, and missing rows. Each Sync's State Operations decide later whether
missing-row `remove` work is sent to its destination surface or skipped.

## Event

**Event** is a typed occurred fact keyed by event identity. An Event declaration
references a `checkpointed` Source and imports rows from source windows rather
than diffing desired current state.

Event records contain:

- `key`: stable event identity
- `type`: fixed by the Event declaration
- `occurred_at`: required event timestamp
- `identifiers`: destination-usable subject identities
- `payload`: user-defined event data

```python
purchase_events = retl.event(
    name="purchase",
    source=purchases,
    key={"purchase": "purchase_id"},
    occurred_at="purchased_at",
    identifiers=[{"type": "email", "value": "email"}],
    payload={"order_total": "order_total", "currency": "currency"},
)
```

Event does not use core Target routing. Destination connectors route events
from the Event declaration and destination binding.

## Identifier Mappings

State and Event declarations use the same Identifier mapping forms:

- `{"type": "email", "value": "email"}` is a scalar mapping. It reads one
  source column value and emits one canonical Identifier object for that row.
- `{"type": "email", "values": "emails"}` is a list-valued mapping. It reads
  one source array/list column and emits zero or more canonical Identifier
  objects of the same type for that row.

`value` and `values` are mutually exclusive in one Identifier mapping.
`identifiers_json` remains a flat array of `{"type": ..., "value": ...}`
objects; list-valued mappings do not put a nested list inside an Identifier
`value`.

Null list-valued source values and empty lists emit no Identifier objects.
Non-list values for `values` are runtime collect errors. Duplicate list items
remain duplicate canonical Identifier objects in this slice. Source SQL remains
responsible for dedupe when a use case requires it.
Blank string source values are scalar Identifier values, not list absence.
`value` with a list source value is not a supported list-valued declaration
shape; use `values` for source lists.

## Target

A **Target** is an optional destination-facing routing key for State. It is
separate from the State Key because target resolution may require destination
objects such as lists, audiences, segments, accounts, or subscription groups.

When Target is present, it is part of State identity and work scope by default.
If a row disappears for one target, RETL removes only that targeted State
identity when the Sync's State Operations allow removals.

Destination Surfaces define what Target means. The same State shape can route
to a list surface, subscription group surface, account surface, or another
destination-owned endpoint contract when the connector declares support.

Omit `target` for targetless State surfaces such as profile-property syncs. Use
`target="audience_key"` when each source row selects its logical destination
target from a column. Use `target=retl.target("newsletter_customers")` when
every row in the State declaration routes to one fixed logical target.

## Sync

A **Sync** binds exactly one State or Event declaration to one destination and
one Destination Surface.

Sync-level options include:

- `operations`: State operation kinds the Sync may produce, defaulting to
  `("upsert", "remove")`
- explicit resend-all execution when the destination should receive current
  State as upserts without advancing normal ordered-work progress
- `on_failure`: whether terminal or retryable destination failures stop
  request-batch and staged-page continuation or block completion and progress

Progress belongs to one Sync, destination, surface, declaration family, and
declaration name. It is a destination-scoped scan cursor, not a shared collect
cursor and not a stored complete-through cursor. Sharing collection or staging
never shares destination progress.

Destination batch outcomes are the operator-facing delivery ledger:
`pending`, `accepted`, `succeeded`, `failed`, and `skipped`. `accepted` and
`succeeded` are distinct resolved outcomes for runtime retry behavior:
`accepted` means the destination accepted the request without final success
evidence, while `succeeded` means the destination confirmed the work. Failed
retryability is metadata on the failed batch. `skipped` is terminal ledger
coverage for a batch or range intentionally not sent or retried. A Sync does
not select delivery policy; the selected Destination Surface declares its
successful `delivery_outcome`.

## State Removal And Resend

State collect is always-on ordered work production. It records current state and
emits State work rows for new, changed, and missing State identities under a
monotonic `collect_id`.

State Operations are selected on Sync, not State:

- send removes when missing source rows should remove destination state
- skip removes when the destination surface or user intent is upsert-only

Skipping removes suppresses State Operations for that Sync without deleting
ordered work evidence needed by other Syncs.

Explicit resend-all stages live current state as bounded upsert work in
deterministic canonical key order. It does not create a special collect path,
does not replay historical diffs, and uses a current-snapshot scan position
rather than ordered-work `collect_id` progress.

Normal State execution for a destination with no progress stages retained
ordered work. This is the intended initial-run default because the first
collect produces durable initial upserts in ordered work. RETL does not
automatically switch a later-added State destination to current-state
bootstrap.

When operators add a State destination after history already exists and need a
current baseline, they should run that Sync with explicit `resend_all=True`.
That path reads current state and sends upserts only; it does not read
ordered work or reconstruct historical removes and intermediate transitions.
It is not a baseline-and-join operation and does not establish the normal
ordered-work lower bound for later incremental runs.
Ordered-work cleanup can limit how much historical replay remains available for
later-added State destinations. Operators adding a later State destination must
choose between retained ordered-work replay, explicit current-state resend, and
any separate scoped skip, reset, or rebaseline operation needed to avoid later
historical replay.

## Run Many

`run_many` executes multiple Syncs while sharing upstream work where possible:

- same Source and declaration: collect ordered work once when valid
- each Sync: stage pending work from its own destination progress
- each Sync: reconcile and sync independently

This keeps shared production efficient while preserving independent destination
progress, delivery outcomes, reports, and recovery.

## Runtime Operations

Operators inspect and repair runtime-store state through `runner.operations`.
Inspection is intentionally high level and bounded: helpers summarize the
runtime store, declaration state, destination scopes, collect IDs, Target
Registry rows, and run evidence, and include SQL context for targeted
follow-up. It is not a public arbitrary-SQL execution API.

Mutation helpers are explicit about the authority they change. Skip operations
preserve ledger evidence by mapping unresolved destination batches or scoped
work ranges to `skipped`. State skip uses ordered-work collect/sequence
bounds; Event skip uses source keyset cursor and primary-key bounds. Reset and
rebaseline helpers mutate existing runtime authority tables for a scoped
runtime store, destination scope, collect sequence, ordered-work range, or State
declaration. Safe cleanup helpers are
separate: ordered-work cleanup is retention-watermark capped, cursor cleanup
removes stale pagination tokens, and diagnostic evidence cleanup removes only
selected run/report rows. Hard ordered-work deletion is destructive and
requires explicit force intent. Target Registry reset is separate from runtime
data reset, and diagnostic run/report cleanup is separate from progress,
ordered work, current state, destination batches, receipts, and Target Registry
rows. A `run_id` is diagnostic evidence, not a restore point.

The same surface is available to operators and background jobs as
`retl operations ...`. Commands use the forward operation names, for example
`inspect-runtime`, `inspect-destination-scope`, `dismiss-unresolved`,
`skip-ordered-work-range`, `skip-event-keyset-range`,
`reset-destination-scope`, `rebaseline-state`, `cleanup-ordered-work`,
`delete-ordered-work`, `cleanup-cursors`, `cleanup-evidence`,
`reset-target-registry`, `delete-run-evidence`, and
`delete-report-evidence`. Commands that need a destination scope take explicit
scope flags rather than importing a user script. Event keyset skip also
requires explicit scalar kinds for every cursor and primary-key bound; it does
not infer types from shell strings. All cursor bounds in one Event skip range
must use the same scalar kind, and all primary-key bounds in that range must
use the same scalar kind, because destination batch evidence stores one cursor
kind and one primary-key kind per Event source range. Output is compact JSON by
default and must not include resolved credentials or secret material.
