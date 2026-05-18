# ARCHITECTURE

This file is a top-level architecture map for agents and new contributors.

It is not the primary policy surface. Durable architectural rules live in:

- [docs/index.md](./docs/index.md)
- [docs/control-plane.md](./docs/control-plane.md)
- [docs/runtime.md](./docs/runtime.md)
- [docs/canonical-model.md](./docs/canonical-model.md)
- [docs/destinations.md](./docs/destinations.md)
- [docs/appendices.md](./docs/appendices.md)

## Goal

Optimize the repository for agent legibility: clear boundaries, predictable
structure, stable read paths, and mechanical proof.

## Top-Level Ownership

- `docs/`: system of record for durable rules, execution plans, and support references
- `docs-site/`: Docusaurus source for public user-facing documentation
- `src/retl/`: core runtime, public API, shared contracts, registry, and CLI
- `destination_connectors/`: first-party publishable destination package root
  for connector packages, package-local docs, and connector-local tests
- `tests/`: repository-local proof surfaces including fixtures
- `tools/checks/`: structural and architecture enforcement

## Boundary Map

- Core runtime code under `src/retl/` must not depend on destination connector packages.
- Shared Source contracts live under `src/retl/sources/`; concrete SQL backend
  implementation lives under `src/retl/backends/<backend>/`.
- Runtime orchestration must not import DuckDB or other concrete warehouse
  clients.
- First-party destination growth happens through `destination_connectors/`, not by widening core runtime ownership.
- Plans and root shims must not become the only home for durable architecture rules.
- User docs in `docs-site/` must not duplicate or replace repository-control
  rules from `docs/`.
- Generated artifacts must never become the only durable source of truth.
- Validation must be repository-local and mechanically checkable.

## Validation Model

The repository expects proof through the applicable local surfaces:

- tests and deterministic fixtures
- dry runs, simulators, or documented equivalent sandbox paths
- structured logs, metrics, and traces when they are part of the proof path
- docs and architecture checks for control-plane and repository-shape changes

## Read Next

- [docs/index.md](./docs/index.md)
- [docs/control-plane.md](./docs/control-plane.md)
- [docs/product.md](./docs/product.md)
- [docs/runtime.md](./docs/runtime.md)
- [docs/canonical-model.md](./docs/canonical-model.md)
- [docs/destinations.md](./docs/destinations.md)
- [docs/examples.md](./docs/examples.md)
- [docs/appendices.md](./docs/appendices.md)
- [docs/plans/index.md](./docs/plans/index.md)
