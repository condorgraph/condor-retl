# Docs Index

This tree is the durable knowledge base for the reverse ETL repository. It owns
AI, contributor, repository-control, and implementation-contract documentation.
The active product model is **Source**, **State**, **Event**, **Sync**, and
**Destination Surface**.

Public user documentation for installing, declaring, running, and operating
RETL lives in the Docusaurus source tree at [`../docs-site/`](../docs-site/).
Do not mirror this compact control-plane tree into the site.

The compact root pages are the primary reading surface:

- [Control Plane](./control-plane.md) — repository shape, docs policy,
  toolchain, lifecycle, validation, and safety standards
- [Core Beliefs](./core-beliefs.md) — directional product and engineering
  ideals that shape implementation choices
- [Product](./product.md) — public State/Event API, Sync execution, and
  operator-facing behavior
- [Runtime](./runtime.md) — phase contracts, destination scan progress,
  reports, dry run, recovery, and failure semantics
- [Recovery](./recovery.md) — ledger-first recovery, retry behavior, operator
  repair paths, and failure-state ownership
- [Data Plane Types](./data-plane-types.md) — exact SQL/Arrow/JSON
  handoff boundaries across collect, stage, reconcile, sync, and request
  planning
- [Canonical Model](./canonical-model.md) — canonical State/Event identity,
  operations, fingerprints, and sensitive-data boundaries
- [Destinations](./destinations.md) — Destination Surface contracts, targets,
  delivery outcomes, connector boundaries, and compatibility
- [Examples](./examples.md) — compact State/Event authoring and execution
  examples
- [Appendices](./appendices.md) — support context, taxonomy summary, and
  generated-doc disposition
- [Reference Mapping](./reference-mapping.md) — non-core mapping from older
  resource vocabulary to State/Event patterns

Supporting material remains available where needed:

- [Plans](./plans/index.md) — execution-planning rules and deferred-work ledger


## Reading Paths

### Implementing a Feature or Runtime Change

1. [Control Plane](./control-plane.md)
2. [Core Beliefs](./core-beliefs.md)
3. [Product](./product.md)
4. [Runtime](./runtime.md)
5. [Recovery](./recovery.md), if the change touches recovery, retry behavior,
   destination ledger state, or operator remediation
6. [Data Plane Types](./data-plane-types.md), if the change touches collect,
   stage, reconcile, sync, destination request planning, or runtime payloads
7. [Canonical Model](./canonical-model.md), if the change touches staged or
   reconciled intent
8. The relevant active plan after the durable docs above

### Building or Reviewing a Destination Connector

1. [Core Beliefs](./core-beliefs.md)
2. [Destinations](./destinations.md)
3. [Product](./product.md)
4. [Runtime](./runtime.md)
5. [Data Plane Types](./data-plane-types.md)
6. [Canonical Model](./canonical-model.md)
7. [Examples](./examples.md), for authoring shape

### Operator Recovery or Replay

1. [Product](./product.md)
2. [Runtime](./runtime.md)
3. [Recovery](./recovery.md)
4. [Destinations](./destinations.md), if recovery involves connector behavior,
   receipts, target resolution, or replay fingerprinting

### Repository Policy or Agent Workflow

1. [Control Plane](./control-plane.md)
2. The relevant temporary active plan, when one exists
3. Supporting references only when the root docs or active plan point there

## Ownership Rules

Durable behavior belongs in these compact root pages. Active plans are
execution records, not independent policy surfaces. Historical design, product,
and reference files may provide provenance, examples, or deeper detail, but
they do not override the compact root pages.

`docs-site/` owns public task-oriented user docs. When both audiences need the
same topic, keep the durable rule here and write a narrower user-facing page in
`docs-site/` that does not duplicate the full control-plane contract.

Meaningful changes update code, tests, docs, and checks together. Work is not
complete until applicable repository-local proof passes.
