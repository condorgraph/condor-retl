# Plans

Plans are temporary execution artifacts, not architecture essays. Durable rules
belong in the compact root docs before implementation is considered complete.
Closed execution records are not retained in this tree; use git history for
historical provenance. Unresolved follow-up belongs in
[Deferred Work](./deferred-work.md).

## Reading order

1. [Docs Index](../index.md)
2. [Control Plane](../control-plane.md)
3. [Product](../product.md)
4. [Runtime](../runtime.md)
5. [Recovery](../recovery.md), if the work touches retry or repair behavior
6. [Data Plane Types](../data-plane-types.md), if the work touches runtime
   payload handoffs
7. [Roadmap](./roadmap.md)
8. [Deferred Work](./deferred-work.md) for intentional deferrals

## Rules

- Durable policy belongs in the compact root docs.
- `docs/plans/deferred-work.md` records intentional deferrals.
- Active execution plans are optional temporary work artifacts and should not be
  committed as retained history.
- Every active plan must include these sections: `Outcome`, `Plan Contract`, `Depends on`, `Locked Inputs`, `Scope`, `Non-goals`, `Implementation Sequence`, `Proof Obligations`, `Required Tests and Fixtures`, `Docs That Must Be Updated When Complete`, `Decision Log`, and `Acceptance`.
- The `Plan Contract` section must at minimum name `status`, `owner`, `linked subsystem docs`, `decision log`, and `next steps`.
- Every active plan must name the repo-local validation surfaces that prove the slice: fixtures, dry runs, simulators or sandboxes, logs, metrics, traces, and docs or architecture checks as applicable.
- Every non-trivial change must have a plan before implementation starts.
- If a plan changes durable behavior or repository structure, the durable docs must be updated before the plan is closed.
- `subplans/` is optional, bounded execution detail. It must not become a second durable policy surface or a nested planning tree.
- A plan decision log is a provenance ledger. Record the source file, rule promoted or clarified, and the stable target instead of restating the full rule body there.
- Closed plans must promote durable rules into compact docs and preserve
  unresolved follow-up in `docs/plans/deferred-work.md`.
- Human-only manual QA is not sufficient acceptance for runtime or connector slices when a repo-local proof path can exist.
