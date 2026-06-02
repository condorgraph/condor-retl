# Destinations

Destinations are the boundary where RETL turns generic **State** and **Event**
work into partner-specific API calls. Core RETL owns the primitives and runtime
phases; destination connectors own endpoint-specific choices through
**Destination Surfaces**.

## Destination Connector

A **Destination Connector** is a destination-specific adapter. It translates
generic State Operations and Event imports into the partner's API shape.

Connectors are where destination-specific opinions belong:

- which partner endpoint is used
- which fields are required
- which identifiers are accepted
- how a Target maps to a partner object
- whether partner objects can be created
- how submission evidence is interpreted
- which bounded partner diagnostic detail is useful after failed submissions
- whether submission returns definitive success or accepted delivery

Core RETL should not infer partner behavior from arbitrary Payload fields.
Users shape source rows and declarations so the fields required by the selected
Destination Surface are present.

Compatibility summaries use `supported_retl_versions` for the RETL package
range a destination package can load against. Replay safety uses
`destination_definition_fingerprint` so persisted work can prove it is being
replayed against the same destination definition that planned the original
submissions.

First-party connector packages live under `destination_connectors/` and expose
connectors through the `retl.destinations` entry point group. Core runtime
discovers those entry points through the generic destination registry; it must
not import concrete first-party package modules directly.

First-party publishable connector packages are licensed under Apache-2.0. Their
package license notices must identify `Dataration LLC (Condor)` as the copyright holder.

Core RETL ships built-in `retl/mock` and `retl/reference` connectors inside the
runtime package. Built-ins are local proof surfaces for runtime tests, examples,
and connector-author workflows; `retl/mock` is the runtime test double for
synthetic destination outcomes. They must not be treated as installable partner
destinations, and they must not stand in for production connector packaging.
The repo-local `retl/reference-http` connector under `destination_connectors/`
is the package-shaped proof path for destination package loading, HTTP request
planning, dry-run output, transport submission, receipts, and destination batch
ledger behavior.

The first-party `retl/file` connector is a local file handoff destination. It
writes new CSV export drops for State Operations and Event imports instead of
mutating a partner API. Each successful submission creates operation-specific
CSV files plus a manifest with RETL batch ids, counts, file sizes, and
checksums. File drops are at-least-once handoff artifacts: replay may create a
new export directory, and downstream consumers should use manifest evidence for
deduplication.

Active first-party partner packages may include opt-in `live_sandbox` tests
for disposable partner accounts or sandbox resources. These tests must stay
outside default checks, must be run through explicit `make test-sandbox-*`
targets, and must keep evidence bounded and redacted.

## Auth Modes

Destination auth is connector-owned configuration expressed as explicit
**Auth Modes**. First-party connectors must declare `auth_modes`; public,
mock, or local-only connectors declare `retl.auth.none()` rather than omitting
auth.

The implementation home for Auth Modes is `retl.auth`. Destination packages and
backend packages import shared auth helpers from that module; `retl.auth.*` is
also available after `import retl` for authoring ergonomics.

Core RETL supports these mode kinds:

- `none`
- `bearer_token`
- `api_key`
- `basic`
- `oauth2_client_credentials`
- `oauth_jwt`
- `native`
- `custom`

Auth Modes declare credential field contracts. They do not imply a transport by
themselves. HTTP connectors may apply resolved credentials as headers, cookies,
query parameters, or OAuth bearer tokens. SDK-backed and database-backed
connectors may use resolved credentials to construct backend-native clients,
sessions, or connection arguments at the sync submission boundary.

The `basic`, `bearer_token`, `api_key`, `oauth2_client_credentials`, and
`oauth_jwt` mode kinds should be used only when those semantics match the
destination's real auth mechanism. Backends such as Snowflake, BigQuery, and
Databricks must not encode SDK auth as HTTP Basic or API-key semantics unless
the backend actually uses those semantics.

`native` is the standard mode kind for non-HTTP credential sets. A connector
should give each native credential set a specific mode name, such as
`password`, `key_pair`, `service_account`, or `pat`, and declare the required
and optional credential fields for that set. Connector-owned submission code
then translates the resolved credential values and public config into
backend-native SDK or driver arguments. Core RETL owns field validation,
secret-shaped credential enforcement, secret resolution, and redacted auth
evidence; connector packages own backend-specific credential materialization.

When a connector declares exactly one mode, `retl.destinations.load(...)` may
omit `auth_mode`. When a connector declares multiple selectable modes, the
binding must pass `auth_mode=...`; unknown mode names fail with available mode
names.

Public connector config lives on the Destination Binding as explicit values.
Authors may source those values from `retl.config[...]`; the default environment
resolver maps `retl.config["destinations.meta.ad_account_id"]` to
`DESTINATIONS__META__AD_ACCOUNT_ID`: `.` separates hierarchy
with `__`, while `_` remains a normal word separator inside a segment.
Authors may also configure a TOML-backed public config resolver explicitly:
`retl.TomlConfigResolver("retl.local.toml")` reads nested tables such as:

```toml
[destinations.meta]
ad_account_id = "act_123"
```

Use `retl.ChainedConfigResolver(retl.TomlConfigResolver(...),
retl.EnvironmentConfigResolver())` when TOML values should have precedence and
environment variables should fill misses.

Credential inputs live on the Destination Binding as explicit secret-shaped
values: `SecretRef` or `SecretLiteral`. Bare strings are invalid in
`credentials={...}`; public non-secret strings belong in `config={...}`.
`SecretRef` values may appear in public authoring, but secret material is
resolved only at the runtime sync boundary. `SecretLiteral` is a process-local
escape hatch for already-loaded secret material and must not be serialized,
displayed, compared as durable identity, or persisted.

The default environment resolver maps
`retl.secrets["destinations.meta.access_token"]` to
`DESTINATIONS__META__ACCESS_TOKEN`: `.` separates hierarchy with `__`,
while `_` remains a normal word separator inside a segment. Prefixing is part
of the logical name, so `retl.secrets["retl.destinations.meta.access_token"]`
maps to `RETL__DESTINATIONS__META__ACCESS_TOKEN`. RETL does not try alias
environment variable names for the same logical secret. Production can replace
or precede the environment provider with configured read-only secret backends
without changing destination declarations.
`retl.TomlSecretResolver("retl.local.toml")` is one such explicit provider for
local development or controlled environments. Secret TOML values use the same
dotted logical names as `retl.secrets[...]`, must be non-empty strings, and are
not inferred from public config field names.

Resolved headers, cookies, query parameters, private keys, tokens, client
secrets, and auth-bearing URLs must not be persisted in bindings, reports, run
indexes, receipts, manifests, logs, traces, or diagnostics.

OAuth modes require connector-owned runtime hooks. Connectors that declare
`oauth2_client_credentials` must provide the token-exchange transport needed to
obtain an access token; connectors that declare `oauth_jwt` must provide both
the token-exchange transport and JWT signer. These hooks are used only at the
sync boundary and their returned tokens remain ephemeral.

Reports may expose redacted auth evidence: selected mode name, required-field
presence booleans, and whether resolution succeeded.

Destination bindings may use namespaces to avoid repeated per-field wiring:

```python
destination = retl.destinations.load(
    "retl/google-ads-data-manager",
    binding_name="google_ads_customer_match",
    auth_mode="service_account",
    credential_namespace="destinations.google_ads.service_account",
    config_namespace="destinations.google_ads",
    config={"customer_match_terms_accepted": True},
)
```

`credential_namespace` expands the selected Auth Mode's required credential
fields into `SecretRef` values such as
`retl.secrets["destinations.google_ads.service_account.private_key"]`. Binding
construction does not resolve secret material. Optional credential fields remain
explicit so a missing optional secret does not become required by namespace use.
This expansion is not secret discovery. RETL must not infer secrets from field
names, scan public config for keys such as `password`, `token`, or
`private_key`, or auto-promote config values into credentials.
This rule applies equally when public config and secrets are both sourced from
TOML-backed resolvers.

`config_namespace` reads only connector-declared `config_namespace_fields`.
Core RETL must not discover arbitrary config keys from a namespace and must not
infer Target mappings from namespaced config paths. Explicit `credentials={...}`
and `config={...}` entries override namespace-derived values field by field.

For example, a Snowflake connector can expose two native Auth Modes:

```python
auth_modes=(
    retl.auth.native(
        "password",
        required_fields=("user", "password"),
        optional_fields=("role",),
    ),
    retl.auth.native(
        "key_pair",
        required_fields=("user",),
        optional_fields=("private_key", "private_key_path", "private_key_passphrase", "role"),
    ),
)
```

A binding can select the key-pair mode and use namespaces for both credentials
and public config:

```python
snowflake = retl.destinations.load(
    "retl/snowflake",
    binding_name="warehouse",
    auth_mode="key_pair",
    credential_namespace="destinations.snowflake.key_pair",
    config_namespace="destinations.snowflake",
)
```

The selected Auth Mode expands required credential fields to environment-backed
secret references such as:

```text
DESTINATIONS__SNOWFLAKE__KEY_PAIR__USER
DESTINATIONS__SNOWFLAKE__KEY_PAIR__PRIVATE_KEY
DESTINATIONS__SNOWFLAKE__KEY_PAIR__PRIVATE_KEY_PATH
```

Snowflake key-pair connectors must accept exactly one private key material
source: `private_key` for inline key content or `private_key_path` for a local
file path that is read by the connector adapter at connection-open time.

The connector-declared `config_namespace_fields` resolve separately through the
public config resolver, for example:

```text
DESTINATIONS__SNOWFLAKE__ACCOUNT
DESTINATIONS__SNOWFLAKE__WAREHOUSE
DESTINATIONS__SNOWFLAKE__DATABASE
DESTINATIONS__SNOWFLAKE__SCHEMA
```

Failed destination submissions may also expose a bounded diagnostic detail in
`DestinationSubmissionEvidence`, and the runtime persists the latest redacted
value as `last_error_detail` on Sync Report and destination batch rows.
Connectors choose this diagnostic string from partner error objects, validation
paths, trace ids, or exception text when it helps operators fix schema and
configuration issues. Core runtime redacts obvious auth and secret material and
caps the value at 4096 characters before persistence. Connectors must not use
this field for raw request bodies by default.

## Destination Surface

A **Destination Surface** is a connector-owned named endpoint contract for a
specific shape of State or Event. Surface names are not standardized by core
RETL.

Examples of surface names a connector might expose:

- `user_profile`
- `profile_properties`
- `subscription_group_membership`
- `list_membership`
- `purchase_event`

A Sync must name exactly one surface:

```python
retl.sync(
    name="klaviyo_profile_properties",
    declaration=customer_state,
    destination=klaviyo,
    surface="profile_properties",
)
```

The selected surface defines what the declaration means for that destination.
The same State declaration can be valid for one surface and invalid for another.

## Surface Contract

A Destination Surface declares:

- whether it accepts State or Event
- required State Key or Event key shape
- accepted Identifier types
- Identifier presence requirements over that accepted set
- required Payload fields
- whether Target is required, optional, or unsupported
- supported State Operations: `upsert`, `remove`, or both
- whether managed Target creation is supported
- whether the surface is synchronous or asynchronous
- successful delivery outcome: `delivery_outcome="accepted"` or
  `delivery_outcome="succeeded"`

Each Destination Surface must bind Target to one coherent remote object subtype
and lifecycle. `target_mode` and managed Target support apply to every Target
used with that surface. A connector must not use one surface for mixed target
subtypes or mixed mutability rules, such as some Targets being managed
membership containers while others are lookup-only, read-only, immutable, or
rule-derived objects. Model those as separate surfaces with distinct names,
capabilities, docs, and tests.

Runtime validates the Sync against this contract before irreversible
destination writes. A Sync that can produce `remove` must fail before submission
when the surface only supports `upsert`.

Identifier compatibility is split into two surface-owned declarations:

- `accepted_identifier_types` names every Identifier type the surface accepts.
  A declaration that includes any other Identifier type is incompatible with
  that surface.
- `identifier_requirements` names the required presence policy. `any_of`
  requires at least one Identifier from the listed accepted types. `all_of`
  requires every listed accepted type. Requirement lists must refer only to
  accepted Identifier types. When a surface declares multiple requirements,
  every requirement must be satisfied.

Accepted Identifier types that are not needed to satisfy a requirement are
optional but supported for connector request rendering. Row-level checks for
empty values, hashing rules, and partner-specific usable combinations remain
connector or runtime data validation, because those facts can only be proven
after staging and reconciliation.

Surface compatibility reads authored Identifier types independent of whether a
declaration uses scalar `value` mappings or list-valued `values` mappings. A
list-valued mapping still produces normal canonical Identifier objects before
request planning, so connectors receive the same flat Identifier array shape
they receive from scalar mappings. Destination connectors must not rely on a
nested list inside an Identifier `value`.

Destination request item semantics are surface-owned. Core RETL preserves the
canonical work record and Identifier array; the connector decides how many
partner request items a work record renders. For example, a surface may render
one work record to one JSON row, while another may fan out repeated accepted
Identifiers into multiple partner rows.

Declarations must emit the exact Payload field names the selected surface
requires. RETL does not add a second payload-remapping layer between
declaration and surface.

`delivery_outcome` is the surface-owned declaration of successful delivery
evidence. Use `delivery_outcome="succeeded"` when successful submission returns
definitive partner evidence that the destination work completed. Use
`delivery_outcome="accepted"` when successful submission only proves the partner
accepted, queued, or started the work and RETL does not have final success
evidence. The field maps directly to durable destination batch ledger outcomes:
successful accepted-only evidence records `accepted`, while definitive success
records `succeeded`. Runtime treats `accepted`, `succeeded`, and `skipped` as
resolved ledger coverage for progress and retry decisions while preserving
`accepted` and `succeeded` as separate operator-visible outcomes.

## State Surfaces

State surfaces receive State Operations produced by reconciliation.

`upsert` means the destination should make the declared State identity true or
current for the selected surface.

`remove` means the destination should remove that declared State identity for
the selected surface.

The surface decides how those generic operations map to partner APIs. For
example, one connector surface might translate targeted State into list
membership calls, while another translates untargeted State into profile
property updates.

If the Sync removal policy sends `remove` work, the selected State surface must
support `remove`. If the surface only supports `upsert`, the Sync must skip
removes. In the current declaration model, reconcile uses the selected surface
capability as that policy source: `remove`-capable surfaces receive removes,
and upsert-only surfaces suppress them without mutating ordered work. Operators
can also use explicit resend-all behavior when that matches the desired intent.

## Event Surfaces

Event surfaces receive occurred facts from checkpointed sources. They do not
use State removal policy, resend-all staging, or core Target routing.

An Event surface defines the destination route from the event declaration and
binding. It may require specific Payload fields, Identifier types, or event
metadata, but it must not reinterpret arbitrary Payload fields as routing
instructions unless those fields are declared by the surface contract.

## Targets

Target is destination-facing routing for State. The selected Destination Surface
defines what Target means.

Depending on the surface, a Target may represent a partner object such as:

- audience
- list
- segment
- subscription group
- account

Within one surface, every Target must represent the same partner object subtype
and obey the same resolution and mutability contract. For example, a partner
that exposes customer-match lists, rule-based audiences, and immutable lookalike
audiences under one broad "audience" resource family must model those as
separate RETL surfaces when their lookup, creation, membership mutation, or
read-only behavior differs. A managed membership surface may support target
find-or-create and row add/remove operations; a rule-derived or immutable
audience reference surface must use its own surface with managed Target creation
disabled unless the partner API supports the same lifecycle.

Target Mappings may be configured at the destination binding level by default.
Surface-specific overrides are allowed only when one destination needs
different remote object IDs for the same logical Target on different surfaces.

The runner-owned runtime store is the normal durable Target Registry. Users do
not pass a separate registry for normal execution; the same DuckDB runtime store
that owns progress, reports, and the batch ledger persists resolved Target
records across runs. `DestinationBinding.target_registry` remains an advanced
injection point, but explicit Target Mappings take precedence over both injected
and runtime-store-backed records.

## Managed Targets

Managed Targets let source data introduce new logical Targets without changing
Python code. A State declaration can also use `retl.target("logical_name")`
when every row should route to the same logical Target; downstream target
resolution treats that static logical value the same way it treats a value read
from a column target such as `target="audience_key"`.

Runtime resolves Targets before mutation submission:

1. explicit Target Mappings
2. Target Registry
3. managed find-or-create when the selected surface supports it

Managed Target support is declared by the Destination Surface, not by the State
declaration. If the surface does not support managed targets and no mapping or
registry record exists, the Sync fails before row mutations are submitted.
Non-dry-run managed lookup or creation requires a writable Target Registry so
the remote target can be persisted before row mutation submission. Dry runs may
plan managed creation without writing registry rows or creating remote objects.
Operators reset persisted Target Registry rows separately from runtime data
with `runner.operations.reset_target_registry(...)`. Target Registry reset must
not mutate ordered work, destination progress, destination batch ledgers,
reports, or run rows.

For managed creation, the logical Target value is the default destination
display name. Optional target metadata may provide another display name later,
but metadata is not required for the initial model.

Meta Custom Audiences is a managed Target example. The `custom_audiences`
surface treats each logical State target as a Customer File Custom Audience
name, finds an existing audience with that exact name under the configured ad
account, or creates an empty `CUSTOM` audience before membership mutations are
planned. The resolved Meta audience id is persisted in the runtime Target
Registry.

## Delivery Outcomes

A Destination Surface declares whether submission can return definitive success
or only accepted delivery. `succeeded` means the destination synchronously or
definitively confirmed the batch worked. `accepted` means the destination
accepted the request but RETL does not have definitive success evidence. Runtime
treats both `accepted` and `succeeded` as resolved outcomes in the destination
batch ledger.

Destination progress is the runtime-owned destination scan cursor, not a
delivery-confirmation cursor. It advances after destination submission produces
durable batch ledger state for the scanned work. Destination batch outcomes are
tracked in the ledger as `pending`, `accepted`, `succeeded`, `failed`, or
`skipped`.
`pending` is durable pre-attempt evidence and supports attempt counting.
`failed` retryability is metadata, not a separate lifecycle status. `skipped`
is terminal ledger coverage for a batch or range intentionally not sent or
retried.

Remote tracking and accepted-batch finalization are outside this contract.
Connectors may return bounded receipt summaries and diagnostics, but durable
evidence must not include raw request bodies, raw partner responses, or
secret-bearing values.

## Connector Boundary

Heavy data engineering should happen upstream of the Destination Connector.
Source SQL and declarations should produce already-shaped records for the
selected surface.

The connector should translate records into partner calls. It should not:

- join, aggregate, dedupe, or reshape source data grain
- inspect arbitrary Payload fields to choose hidden behavior
- create Targets inline per row during mutation submission
- decide Source Mode compatibility
- share destination progress across Syncs

The connector may:

- expose opinionated surfaces for common partner endpoints
- validate surface-specific requirements
- resolve and create managed targets before submission
- translate State Operations into partner request bodies
- render partner-required hashed identifiers at the destination boundary
- classify destination receipts and failures
- return bounded delivery evidence for async work
- return bounded evidence that updates destination batch ledger outcomes

## Identifier Hashing

Core RETL owns shared destination primitives for SHA-256 identifier rendering in
`retl.destinations.identifiers`. Connectors that accept pre-hashed identifier
input should use those helpers to detect 64-character hexadecimal SHA-256
values, preserve them as lowercase, and hash other non-empty values only after
applying the connector's explicit partner normalization rule.

The helper boundary is intentionally narrow. It detects SHA-256 hex strings,
computes SHA-256 hex digests, and supports hash-or-preserve rendering with a
caller-provided normalizer. It does not validate email syntax, phone number
format, external-id shape, or whether a partner should accept a particular
identifier type. Those remain surface and connector contracts.

## HTTP Toolkit

Core RETL provides a small HTTP toolkit for connector packages that submit work
through partner HTTP APIs. The toolkit is shared mechanics only; it does not
make HTTP the only destination transport and does not add partner opinions to
core.

HTTP connector authors may use the toolkit to:

- render deterministic request batches from validated State Operations or Event
  imports
- split already reconciled work by connector-owned request payload limits
- apply relative request templates from public connector config and bounded
  batch context
- use connector-owned body hooks for partner-specific JSON envelopes
- execute through an injected transport boundary
- classify responses into succeeded, accepted, failed retryable, failed
  non-retryable, or pre-acceptance outcomes
- extract sanitized partner messages and retry-after hints
- model async submit flows without required accepted-batch finalization

First-party partner HTTP connectors own their production API origins in package
code. They must not expose arbitrary `base_url` values through
`config_namespace_fields` or read partner origins from public binding config.
Core binding construction rejects explicit `base_url` config for connectors
that do not declare `base_url` as a supported config field.
Tests and offline proofs use injected transports to capture submitted
`HttpRequest` values while preserving production URL construction.
Configurable HTTP origins belong to generic or private HTTP connectors such as
`retl/reference-http`, unless a future connector documents an explicit
exception in durable docs and mechanical checks.

The shared default HTTP response classification is:

- `2xx` responses are accepted or succeeded according to the selected surface
  contract and connector evidence
- `401`, `403`, and `407` are auth/access pre-acceptance failures
- `429`, `408`, `425`, `5xx`, and `599` are retryable failures with
  retry-after evidence when available
- every other `4xx` is a non-retryable failed submitted unit

The failure unit is the destination batch RETL can prove was submitted, not an
inferred bad row. A connector package may override the shared policy only for a
documented partner contract, and should not add partner-specific error-code
tables to core.

Connectors should make request batches stable and safe to repeat when they
return retryable evidence. RETL's in-run retry loop resubmits only the same
planned request batch and payload fingerprint; it does not ask the connector to
rebuild request planning or target resolution. Short `Retry-After` values may
be honored inside the run, while long retry windows or exhausted budgets should
remain durable retryable ledger evidence for the next-run retry sweep.
Runtime owns iteration across selected request batches and applies the Sync's
`on_failure` policy after each batch. Connector packages must not implement
their own multi-batch stopping semantics after a failed response.

Resolved Auth Modes are applied only at the sync execution boundary, after
request planning. Request plans and durable evidence must not contain auth
headers, cookies, tokens, API keys, private keys, client secrets, auth-bearing
query parameters, raw request bodies, or raw partner response bodies. Durable
HTTP evidence is limited to counts, deterministic batch IDs, payload
fingerprints, redacted request shape, sanitized bounded messages, retry hints,
and receipt summaries.

`RequestBatchingPolicy.max_rows` is a destination request item payload limit
over already reconciled State Operations or Event imports. It does not control
Source read windows or runner-level reconcile work batches. The default planner
counts one destination request item per RETL work record. Connectors that can
render one work record into multiple partner request items may provide a
columnar request item count hook that receives the Arrow work page before
`DestinationWorkRecord` materialization and returns one non-negative integer
count per work row. The request body renderer and count hook must share the
same fanout semantics, and connector tests must prove planned request item
counts align with rendered partner payload rows or objects. A single work
record over the request item limit fails planning clearly; RETL does not split
one work record across destination batches.

The request-batch planner preserves reconciled row order within a Sync,
partition boundaries such as Target or operation, byte limits, and bounded
batch context only. Runtime records each planned request batch as a destination
batch with stable identity derived from Sync, destination, surface, declaration
continuity identity, explicit source range coordinates, destination batch
index, payload fingerprint, and a redacted target/request fingerprint.
Incremental State batches include ordered-work coordinates; State
current-snapshot batches include canonical key ranges; Event batches include
source-native keyset ranges. Stable source ordering is a prerequisite for
stable destination batch identity. `RequestBatchingPolicy.max_bytes` is an
additional rendered body limit; it may split a request batch between whole work
records but must not split a single work record.

Destination-scope skip operations use the same ledger authority. Operators use
`runner.operations.dismiss_unresolved(sync)` for unresolved `pending` or
`failed` batches, or scoped skip helpers for known-bad ordered-work or Event
keyset ranges. Skip operations create or update `skipped` ledger evidence
without deleting destination batches and without mutating other destination
scopes, including State scopes that share collect output.

Destination request planning starts from bounded State Operation or Event Import
pages. It must not accept a full operation table, a Python current-state object
collection, or a prebuilt full request-body collection. Connector hooks may turn
one bounded page into partner-shaped Python or JSON payload batches at the sync
edge, and reports may persist batch metadata and fingerprints, not raw request
bodies.

Meta Custom Audiences defines a non-cartesian Identifier fanout policy for its
`payload.data` rows. Repeated values of the same accepted Identifier type render
as multiple rows. A scalar email plus scalar phone remains one rendered row
with both columns because the source has shaped one value for each type on the
record. Mixed repeated values across accepted types render one sparse row per
Identifier value, such as email-only rows and phone-only rows, rather than
synthetic pairs or a cartesian product. Meta enforces the partner 10,000
`payload.data` row limit from this rendered request item count.

Connectors that need executable submission behavior may expose a
connector-owned submission hook. The hook receives the selected surface,
binding, resolved ephemeral auth, reconciled work, dry-run flag, and
surface `delivery_outcome`, and returns bounded Destination Submission
Evidence for one selected request batch.
The hook remains connector-owned because transport execution, partner request
bodies, target APIs, async handle parsing, and failure classification are
destination-specific choices.

Submission hooks may provide sanitized partner error codes, subcodes, and
summary text for failed destination batch outcomes, including non-retryable
failure evidence. They must not persist raw request bodies, raw partner
responses, credentials, or auth-bearing values in failure evidence.

## Compatibility

Destination Surface compatibility is validated at runner run time.
Validation must catch incompatible declaration, Sync, and surface combinations
before destination mutation.

Examples:

- State declaration sent to an Event-only surface: invalid
- Event declaration sent to a State-only surface: invalid
- targeted State sent to a surface that does not accept Target: invalid
- State removal policy that sends removes to an upsert-only surface: invalid
- connector success evidence that does not match the surface
  `delivery_outcome`: invalid
- declared Identifier type not accepted by the selected surface: invalid
- missing an `any_of` or `all_of` Identifier requirement: invalid
- missing required Payload field: invalid

The goal is to let connectors be opinionated without making users model every
partner endpoint detail in core RETL.
