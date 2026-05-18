# AGENTS

This repository is optimized for agent-first development.

Humans steer. Agents execute. The repository itself is the system of record:
if a rule, contract, or decision is not encoded in this repo, it should not be
treated as durable truth.

This file is a navigation shim, not the primary policy surface.

## Project Map

- Start with [README.md](./README.md) for repository intent and current scope.
- Use [docs/index.md](./docs/index.md) as the durable table of contents.
- Repository control-plane rules live in [docs/control-plane.md](./docs/control-plane.md).
- Product and engineering ideals live in [docs/core-beliefs.md](./docs/core-beliefs.md).
- User-facing and operator-facing behavior lives in [docs/product.md](./docs/product.md).
- Runtime contracts live in [docs/runtime.md](./docs/runtime.md).
- Recovery behavior lives in [docs/recovery.md](./docs/recovery.md).
- Runtime data handoff boundaries live in [docs/data-plane-types.md](./docs/data-plane-types.md).
- Canonical model rules live in [docs/canonical-model.md](./docs/canonical-model.md).
- Destination package rules live in [docs/destinations.md](./docs/destinations.md).
- Examples live in [docs/examples.md](./docs/examples.md).
- Appendices live in [docs/appendices.md](./docs/appendices.md).
- Execution planning rules live under [docs/plans/](./docs/plans/index.md).
- Core runtime and public API code live under [src/retl/](./src/retl).
- First-party publishable destination packages live under [destination_connectors/](./destination_connectors).
- Mechanical enforcement lives under [tools/checks/](./tools/checks/README.md).
- Tests and fixtures live under [tests/](./tests).

## Read Order

1. [docs/index.md](./docs/index.md)
2. [docs/control-plane.md](./docs/control-plane.md)
3. [docs/core-beliefs.md](./docs/core-beliefs.md)
4. [docs/product.md](./docs/product.md)
5. [docs/runtime.md](./docs/runtime.md)
6. [docs/recovery.md](./docs/recovery.md), when retry or repair behavior applies
7. [docs/data-plane-types.md](./docs/data-plane-types.md), when runtime payload handoffs apply
8. [docs/canonical-model.md](./docs/canonical-model.md)
9. [docs/destinations.md](./docs/destinations.md)
10. [docs/examples.md](./docs/examples.md)
11. [docs/appendices.md](./docs/appendices.md)
12. [docs/plans/index.md](./docs/plans/index.md)
13. the relevant temporary active plan, when one exists

## Core Beliefs

- Agent legibility is a repository goal, not an afterthought.
- Stable rules live in the compact root docs, not only in plans or support pages.
- Non-trivial work requires an active plan before implementation starts.
- Meaningful changes update code, tests, docs, and checks together.
- Mechanical proof is the blocking path; human-only QA is not enough where repo-local proof can exist.
- Strict boundaries and predictable structure are how the repo scales agent throughput without decay.
- `main` is the trunk branch. Routine agent work should use short-lived
  branches targeting `main`. Recommended branch prefixes are `feat/`, `fix/`,
  `chore/`, and `docs/`. Incomplete user-facing behavior should be hidden by a
  documented feature toggle, compatibility bridge, or non-exposure boundary.
- AI-assisted changes should start from repo-owned context and, where possible,
  a failing or characterizing proof before implementation.

## Package Boundaries

- `src/retl/` owns the core runtime, public API, registry, shared contracts, and CLI.
- `destination_connectors/` owns first-party publishable destination packages.
- `tests/` owns repository-local proof surfaces.
- `tools/checks/` owns structural and architecture enforcement.
- `docs/` owns durable rules, execution plans, generated-doc policy, and support references.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the top-level boundary map, then use
the compact root docs for the actual durable rules.

## Required Checks

Use the repo-local `uv` environment for repository commands.

Preferred control-plane entrypoints:

```bash
uv run python tools/checks/validate_repo_skeleton.py
uv run python tools/checks/validate_architecture.py
```

Default implementation baseline for code changes:

```bash
make check
```

This runs:

- `make format-check`
- `make lint`
- `make typecheck`
- `make test`
- `uv run python tools/checks/validate_repo_skeleton.py`
- `uv run python tools/checks/validate_architecture.py`

Also run these when they apply:

- `make lint-lock` when dependency, packaging, or lockfile surfaces change
- any narrower `Makefile` or `pytest` targets needed for the changed slice

For trunk, contributor, workflow, or release-policy changes, `make check` must
prove the branch-flow policy and CI trigger expectations through the
architecture validator.

Do not consider work complete until the applicable repository-local checks pass.
