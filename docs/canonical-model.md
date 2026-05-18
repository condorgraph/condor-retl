# Canonical Model

The canonical model is the runtime's normalized representation of authored
State and Event declarations. It exists to keep reconciliation, receipts,
replay, and diagnostics deterministic without exposing partner-native payloads
as core RETL state.

## Canonical Invariants

Canonical output is organized around two declaration families:

- **State**: desired current facts that may produce `upsert` and, when allowed,
  `remove` operations.
- **Event**: occurred facts imported from checkpointed source windows.

Canonical records are tagged by declaration family, Sync identity, and the
selected Destination Surface. Destination-native JSON, CSV, upload layouts,
hashed partner fields, request bodies, and transport metadata are derived later
at the destination boundary.

Canonical Identifiers carry authored source values. Partner-required SHA-256
identifier fields are rendered by destination connectors at request planning or
submission time, using connector-selected normalization rules. Hashing is not
canonical validation: email syntax, phone format, and external-id format checks
remain separate connector or runtime validation concerns when a surface already
requires them.

Authored Identifier mappings have scalar and list-valued forms. A mapping such
as `{"type": "email", "value": "email"}` reads one source value and produces
one canonical Identifier object. A mapping such as
`{"type": "email", "values": "emails"}` reads one source array/list value and
produces one canonical Identifier object per item. In both cases canonical
output remains a flat ordered array of objects shaped as
`{"type": <identifier_type>, "value": <identifier_value>}`. Canonical
Identifier `value` fields do not contain nested list values.

For list-valued mappings, SQL collect sorts emitted items by canonical scalar
value before fingerprinting or ordered-work persistence. Null source lists and
empty source lists emit no Identifier objects. Non-list source values for a
`values` mapping fail during collect. Duplicate list items remain duplicate
canonical Identifier objects in this slice; dedupe is still upstream source
modeling unless a later canonicalization decision changes it. Blank string
source values are canonical scalar values unless later connector or runtime
validation rejects them. `value` does not accept a source list as a shortcut
for list-valued mapping.

Fingerprints are derived from normalized canonical payload state, not volatile
runtime metadata or partner-native rendering.

Each authored State/Event declaration also has runtime declaration metadata:

- Declaration name is the continuity identity for destination progress.
- `declaration_version_id` is a fingerprint of sanitized canonical declaration
  JSON. It is audit metadata for the current declaration shape, not a progress
  authority.

Canonical declaration JSON includes declaration kind, name, source name, source
mode, source query hash, typed Source and Runtime relation-space placement
metadata known at runtime, checkpoint mapping when present, field mappings,
identifiers, payload mapping, State target, and Event Occurred At. It excludes
raw source query text from the version payload except by hash, and it excludes
raw secrets and credential values.

## State Identity

State identity is built from:

- Sync identity
- State declaration name
- State Key values
- Target value when present
- selected Destination Surface when the surface affects operation semantics

The State Key is the logical identity for the fact. Target is destination-facing
routing and is part of State identity by default. A row missing for one target
does not imply removal for another target.

State records carry Identifiers and Payload. Identifiers let destinations find a
subject. Payload carries user-defined fields required by the selected surface.
Core RETL does not assign hidden partner-specific meaning to arbitrary Payload
fields.

## Event Identity

Event identity is built from:

- Event declaration name
- Event Key values
- Occurred At
- selected Destination Surface

Events represent occurred facts. They do not infer removal from source absence,
do not use Target routing, and do not participate in State removal policy or
resend-all staging.

Event collection reads checkpointed Source Windows using source-native cursor
and primary-key ordering. Event destination progress is scoped per destination
and records the source-native keyset position scanned for that destination.
`collect_id` may remain collect provenance, but it is not Event
destination progress. `run_many` does not share durable Event work across
destinations; each Event Sync plans source keyset ranges from its own
destination-scoped progress.

## Operations

State reconciliation produces State Operations:

- `upsert`: make the declared State identity true or current for the selected
  surface.
- `remove`: remove the declared State identity for the selected surface.

Event reconciliation produces Event imports from bounded Event work. Event work
is import-only in the core model; duplicate-effect prevention depends on event
identity, destination idempotency, destination batch ledger evidence, and Sync
Report diagnostics.

`noop` is a planner result and summary count, not a persisted destination
operation.

## Fingerprints

State fingerprints cover operation-relevant canonical state, including State
Key, Target, Identifiers, Payload, selected surface, and operation. They exclude
package id, generated timestamp, retry counters, transport metadata, and
partner-native rendering.

Event fingerprints are stable event-identity tokens for receipts and resume,
not full event payload diff hashes.

Fingerprint input must recursively sort object keys, preserve array order,
distinguish missing fields from explicit `null`, and treat nested-value changes
as meaningful canonical changes.

Replay and resume must be deterministic for the same canonical inputs.

Auth evidence is not canonical payload. Runtime summaries may include the
selected Auth Mode name and required credential presence booleans, but they
must exclude resolved credentials, access tokens, Authorization headers,
cookies, private keys, client secrets, and auth-bearing URLs. Replay and
recovery must re-resolve auth instead of reading serialized auth material.

## Progress Boundary

Destination progress is scoped to one Sync, destination, surface, declaration
family, and declaration name. It is a typed scan cursor recording how far source
work has been durably converted into destination batch ledger rows for that
scope.

State progress has two shapes. Incremental State uses `ordered_work` positions
compared by `(collect_id, sequence_order)`. Resend-all and
new-destination bootstrap use `current_snapshot` positions compared by
deterministic canonical key order over live current state. Current-snapshot
scans are not historical diff replay and do not require synthetic
`sequence_order` values.

Event progress uses source-native keyset positions, usually `(cursor_value,
primary_key_value)`. Source Checkpoint is not part of the active Event runtime
progress model.

Progress is authoritative only after `sync` has durable destination batch
ledger rows for the scanned work. Destination outcomes do not gate scan cursor
advancement. `pending`, `accepted`, `succeeded`, `failed`, and `skipped` are
ledger outcomes; complete-through answers are derived from the ledger instead
of stored as a separate cursor. `skipped` is terminal ledger coverage for a
batch or range intentionally not sent or retried.

## Sensitive Data Boundary

Canonical payloads remain sensitive runtime data. They may contain identifiers,
payload fields, and event properties when runtime correctness needs them, but
they must not become full source-row mirrors.

Logs, traces, human-facing contracts, and inspection surfaces must not expose
raw identifiers, raw Payload values, or raw canonical event payloads. They
should expose summaries, counts, ids, package references, and redacted
diagnostics instead.

## Ownership

- Public authoring names and operator workflows belong to
  [Product](./product.md).
- Runtime phase ordering, artifact authority, dry run, replay, recovery, and
  idempotency behavior belong to [Runtime](./runtime.md).
- Destination Surface contracts, target resolution, and receipt interpretation
  belong to [Destinations](./destinations.md).
- Exact type declarations, Arrow schemas, manifest schemas, SQL table catalogs,
  and JSON schemas are generated or code-owned material.
