# Data Plane Types

This page defines the durable data-type flow for RETL runtime work. It is a
boundary contract, not an implementation essay.

## Rule

RETL keeps runtime data columnar from Stage through Reconcile and into Sync.
Partner-shaped JSON appears only at the destination request planning/submission
edge inside Sync.

```text
collect   -> SQL relations, current-state tables, ordered work tables
stage     -> bounded Arrow-compatible work pages
reconcile -> bounded Arrow-compatible operation/import pages
sync      -> bounded Arrow-compatible operation/import pages
request   -> bounded partner-shaped Python/JSON payload chunks
```

## Phase Types

`collect` owns SQL backend placement and source-state production. It may use
SQL relations, temporary relations, current-state tables, ordered-work tables,
checkpoint tables, and bounded Arrow evidence. It must not hand full source
rows to downstream phases as Python objects.

Normal runner collect does not pull full Source rows into Python or Arrow
files. SQL-capable collect runs in the backend and writes RETL-owned runtime
relations, returning only bounded evidence such as counts, positions, and
provenance.

The database-to-Arrow optimization boundary for runtime execution is the
runtime-store read that feeds `stage`. Those reads must use bounded
`pyarrow.RecordBatch` pages and should prefer native streaming or result-batch
APIs. They must not call whole-result APIs such as `fetch_arrow_all()` when a
backend exposes batch iteration for the same result. Native warehouse
connectors, ADBC drivers, Flight SQL, BigQuery Storage Read API, and local
engines are implementation choices behind that bounded RecordBatch page
contract.

Generated SQL used by collect and runtime-store reads is represented as SQLGlot
expression trees behind RETL-owned SQL helper contracts where SQLGlot applies.
The rendered SQL string and driver parameter sequence remain an explicit RETL
handoff boundary. Runtime values must stay in the parameter sequence rather
than being inlined into SQL text, and runtime-owned relation or column names
must be validated before becoming SQL expression nodes.

`stage` reads pending State ordered work, current-state resend-all work, or
bounded Event Source SQL ranges and returns Arrow-compatible pages plus
metadata. Store-to-stage handoff must not expand work into Python row
dataclasses.

Event source replay pages carry explicit `event_occurred_at`,
`event_cursor_value`, `event_primary_key_value`, and range lower-bound columns.
Event checkpoint scalar kinds come from the Event's checkpointed Source
declaration, not from row-level diagnostic JSON or backend runtime type
inspection.

`reconcile` consumes staged columnar pages and emits destination-neutral
columnar State Operation or Event Import pages. Reconcile uses SQL or Arrow
expressions for filtering, projection, remove suppression, and counts.
It must not convert pages into Python row objects to package work.

`sync` receives reconciled columnar pages. It validates the selected Destination
Surface, resolves auth and targets, applies destination request batching, and
submits work.

Destination request planning is the first boundary where partner-shaped Python
or JSON payloads may be created. That conversion must happen in bounded request
chunks, not as a full operation table or full request-body collection.
At this boundary, backend-returned Identifier arrays are normalized to plain
Python JSON values when a backend represents array elements as JSON object or
array strings. Ordinary payload strings remain strings.

This is the sanctioned Arrow-to-Python/JSON expansion point. Runtime code may
materialize the bounded reconciled page or selected destination request records
here because the destination adapter is leaving RETL's canonical columnar data
plane and entering partner-specific request construction. This exception does
not apply earlier in collect, stage, or reconcile.

## Allowed Row Materialization

Python row materialization is allowed only for:

- focused tests and fixtures that are not runtime APIs
- destination request chunks after Sync has received bounded reconciled pages

Python row materialization is not allowed for:

- store-to-stage pending-work pages
- store-to-stage resend-all pages
- Stage work pages
- Reconcile operation/import pages
- remove filtering, operation counts, import counts, or destination-neutral
  packaging

## JSON Boundary

Canonical `key`, `target`, `identifiers`, `payload`, and `evidence` values may
be stored in SQL/columnar form during collect, stage, and reconcile. They should
remain columnar until a destination request renderer needs partner-shaped
Python/JSON values.

Sync may render JSON only after:

- the work is already reconciled into bounded operation/import pages
- the selected Destination Surface has been validated
- destination request batching has chosen a bounded request chunk

Today, implementations may perform this expansion at the bounded reconciled
page size before splitting into destination request chunks. That is allowed
only at this request-planning boundary. Prefer destination-request-chunk
granularity when practical, but do not move partner-shaped Python/JSON
materialization earlier to collect, stage, or reconcile.

Durable reports and evidence may store counts, fingerprints, ids, redacted
bounded reason strings, receipt summaries, and handles. Sync Reports are compact
runtime-store indexes and must not store row samples, raw full request bodies,
full partner response bodies, or rich page payloads.
