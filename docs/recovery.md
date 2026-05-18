# Recovery

This page defines how RETL records destination delivery state and how operators
recover when a Sync cannot fully resolve destination work. It replaces the older
failure-outcome taxonomy with the smaller contract the runtime actually needs:
durable ledger state, retry eligibility, and explicit operator repair.

## Recovery Model

Destination recovery is ledger-first. RETL does not infer remote state from a
later Source read, compact report index row, or in-memory attempt result. A
fresh run starts from committed destination progress and the destination batch
ledger:

1. Retry old `pending` batches and retryable `failed` batches before scanning
   new work.
2. Treat `accepted`, `succeeded`, and `skipped` batches as resolved for
   automatic retry.
3. Advance destination scan progress only after durable destination batch
   ledger rows exist for the scanned work.

Destination progress belongs to one Sync, destination, surface, declaration
family, and declaration name. It is not shared across Syncs. State Syncs may
share collect output; Event Syncs plan destination-scoped source keyset ranges.

## Ledger Statuses

The durable destination batch ledger uses five statuses:

- `pending`: submitted or prepared work without final durable outcome evidence.
- `accepted`: the destination accepted or queued the work, but final application
  is not proven by the selected surface.
- `succeeded`: the selected surface proved final successful application.
- `failed`: the runtime has durable failure evidence for the submitted batch.
- `skipped`: an operator or scoped skip operation intentionally resolved the
  batch or range without retrying it.

Retryability is metadata on `failed`; it is not a separate ledger status.
Failure category, HTTP status, partner error code, retry-after evidence,
submission handles, and bounded diagnostics are evidence attached to ledger,
attempt, report, or receipt rows. They do not replace the ledger status.

## Submission Evidence

Destination connectors report bounded submission evidence to Sync. Evidence
names such as `confirmed`, `accepted`, `retryable_failure`,
`terminal_record_failure`, `pre_acceptance_failure`, and `planned` describe the
attempt result RETL observed. They are not durable ledger statuses.

Within a current run, runtime applies the Sync's `on_failure` mode to request
batch and staged-page continuation. Under `continue_on_any`, a failed request
batch does not prevent later selected request batches or later progress-allowed
staged pages from being attempted. Failed batches still remain `failed` ledger
evidence with retry metadata. Later runs start from that ledger: resolved
batches are skipped, retryable unresolved failures are retried within budget,
and non-retryable unresolved failures remain blocked until repair or explicit
skip.

Pre-acceptance failures cover work RETL can prove was not accepted by the
destination, such as auth, transport, schema, rate-limit, or submission setup
failures. Submitted batch failures cover work that reached a destination
boundary and returned durable failed evidence. Connectors may include bounded,
redacted partner diagnostics, but must not persist raw request bodies, raw
partner response bodies, credentials, auth-bearing URLs, or full payload
fragments.

## Retry Rules

Current-run submission may make a bounded same-batch retry for clearly
retryable evidence: retryable submission evidence, `429`, `408`, `425`, `5xx`,
`599`, and transport-style retryable failures. Long `Retry-After` values,
exhausted attempt budgets, or exhausted sleep budgets leave the batch durable
for a later ledger-first retry when policy allows.

Auth/access failures, schema failures, validation failures, target-resolution
failures, compatibility failures, malformed payloads, and other
non-retryable `4xx` outcomes are not retried automatically in the same run.
They remain blocked until the user fixes data, mapping, configuration, target
state, connector code, or explicitly resolves the affected ledger scope.

## Operator Recovery

The public repair surface is `runner.operations` and the matching
`retl operations ...` CLI family. Recovery helpers are explicit about the
authority they change:

- `dismiss_unresolved(sync)` maps actionable unresolved `pending` and `failed`
  batches in one Sync destination scope to `skipped`.
- Scoped skip helpers create `skipped` ledger evidence for known-bad ordered
  work or Event keyset ranges without deleting destination ledger rows.
  The CLI forms are `skip-ordered-work-range` for State collect/sequence
  ordered-work bounds and `skip-event-keyset-range` for Event cursor plus
  primary-key keyset bounds. Event CLI bounds require explicit scalar kinds and
  values, with one cursor kind and one primary-key kind across the whole Event
  source range.
- Event retry re-reads Source SQL for the keyset range stored on the
  destination batch row. Operators must retain or be able to reconstruct source
  rows for unresolved Event ranges; otherwise retry reports a source replay or
  retention diagnostic.
- Reset and rebaseline helpers mutate runtime authority tables for a scoped
  runtime store, destination scope, collect ID, ordered-work range, State
  baseline, or Target Registry.
- Safe cleanup helpers may compact ordered work only through retention
  watermarks and unresolved-ledger blockers. Cursor cleanup may delete stale
  pagination tokens. Diagnostic cleanup may delete run or report evidence
  without pretending to repair destination progress.
- Hard ordered-work delete is a destructive operator action, separate from
  cleanup, and requires explicit force intent.

Recovery commands must require explicit scope or filters and must emit compact,
redacted diagnostics. They must not create operation-history tables, repair
ledgers, legacy command aliases, or hidden compatibility state.

## Outcome Guide

| Situation | Durable state | Normal recovery |
|---|---|---|
| Destination accepted queued work | `accepted` | Do not auto-retry; inspect destination or connector-specific receipt evidence if final application is uncertain. |
| Destination proved final success | `succeeded` | No recovery needed. |
| Retryable transport, rate-limit, or server failure | `failed` with retry metadata, or `pending` when final evidence is ambiguous | Retry automatically within budget, then retry from ledger on a later run. |
| Auth, schema, validation, target, compatibility, or malformed request failure | `failed` with non-retryable evidence | Fix the cause, then rerun or explicitly skip/dismiss the scoped unresolved work. |
| Operator decides known-bad work should not be sent | `skipped` | Preserve ledger coverage; future runs do not retry that scope automatically. |
| Runtime store, progress, checkpoint, or ledger invariant failure | no progress advancement unless already durable before the failure | Inspect with operations, then use scoped reset/rebaseline only when the authority boundary is understood. |
| Possible submit-before-record ambiguity | usually `pending` plus bounded receipt or handle evidence when available | Prefer idempotent retry or destination reconciliation; use scoped dismissal only when duplicate risk is understood. |

## Ownership

[Runtime](./runtime.md) owns phase contracts, progress, reports, recovery flow,
and ledger-first execution. [Destinations](./destinations.md) owns surface
delivery outcomes, connector evidence, receipts, HTTP classification, and
partner-specific diagnostics. This page is the compact recovery bridge between
those contracts.
