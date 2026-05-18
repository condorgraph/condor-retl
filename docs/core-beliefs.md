# Core Beliefs

This page names the product and engineering ideals that should shape RETL
design choices before implementation details are selected. These beliefs are
directional rules: runtime, product, destination, and control-plane contracts
still live in their dedicated compact root docs.

## Runtime Shape

RETL is a library runtime, not a hosted orchestrator. It owns durable package
boundaries, phase execution, replay semantics, destination progress, and receipt
authority. It does not own scheduling, queueing, worker pickup, lease
management, recurring orchestration, or operator calendar policy. External
orchestrators may call RETL, but they must honor RETL's phase and artifact
contracts.

The runner is source agnostic. SQL backend implementations read upstream truth
and commit source-side state or payloads through documented contracts; the
downstream runner should not fork into backend-specific execution models.
Backend-specific code should stay behind backend boundaries, share common
runtime machinery where reasonable, and avoid duplicating phase behavior across
source families.

## Destination Shape

Destinations are thin adapters at the edge. They declare capabilities, translate
canonical mutations into partner-native payloads during `sync`, execute
partner-specific transport, and return durable receipts. They should not own
runner semantics, source interpretation, reconciliation policy, scheduler
behavior, or broad custom runtimes.

Partner-specific payloads stay as late as possible. Collect, stage, and
reconcile remain canonical and destination-neutral; destination request bodies,
file layouts, upload jobs, and polling details belong inside destination sync
execution.

The destination request planning edge is the place where bounded Arrow pages may
be expanded into partner-shaped Python/JSON records. That expansion is a
destination adapter responsibility, not a license for earlier runtime phases to
materialize canonical work rows.

## Data Plane Shape

RETL's data plane is columnar and bounded by default. Source collection,
current-state maintenance, ordered work production, staging, reconciliation,
and destination handoff should move through SQL relations, Arrow batches,
manifests, durable handles, or other pageable columnar artifacts. They must
not depend on whole-run Python row
objects, whole-run dictionaries, full-table `fetchall()` calls, or in-memory
tables as the normal execution model.

Diffing belongs in the data system that owns the rows. For SQL-capable sources
and stores, current state, ordered work, inserts, changes, and removals should
be computed with SQL or columnar execution and exposed as bounded work and
operation pages. Python may coordinate phases and inspect bounded samples, but
it must not become the diff engine for production-shaped State runs.

Batching is a contract boundary, not a cleanup step after materialization.
Source pages, reconciliation operation pages, and destination payload batches
are separate limits with separate responsibilities. A destination payload limit
does not make it acceptable to first load the full source, full current state,
full ordered-work set, full operation set, or full request plan into Python
memory.

At the destination request boundary, it is acceptable to expand one bounded
reconciled page into Python/JSON for request rendering. Prefer expanding only
the selected destination request chunk when practical, but the durable rule is
that this expansion remains bounded and does not move upstream of Sync request
planning.

Small test fixtures may use in-memory tables to express examples, but active
runtime code must follow the same bounded artifact contracts for small and large
runs. Convenience implementations that work only because examples are small are
not acceptable stepping stones unless they are isolated behind a non-runtime
test fixture boundary and blocked from production execution by mechanical
checks.

## Performance and Resources

Optimize for speed without weakening correctness. Prefer clear phase boundaries,
columnar payloads, shared staged bases, capability-driven batching, and
mechanical checks over convenience paths that create slow or ambiguous behavior.

RETL must work under limited disk and memory budgets. Large runs should move
through bounded batches and durable handles instead of whole-package in-memory
row materialization. Small runs follow the same contracts as large runs so the
runtime does not grow a second execution model.

## Development Model

Development is AI-first and test-governed. Agents and humans should start from
repo-owned context, make changes that preserve repository legibility, and prove
meaningful behavior with tests or structural checks. Chat history, unstated
intent, and manual inspection are not durable substitutes for encoded docs,
code, tests, and checks.
