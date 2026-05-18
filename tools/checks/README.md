# Repository Checks

This directory contains the repo-local enforcement surfaces for the control-plane slice.

The canonical maintained docs tree lives at root `docs/`.

The checks are stdlib-only and can run in this checkout without extra dependencies.

## Available checks

- `uv run python tools/checks/validate_repo_skeleton.py`
- `uv run python tools/checks/validate_architecture.py`

## What they cover

- compact docs root navigation reachability
- required compact docs page coverage
- repository-layout and package-layering contract checks
- repo-owned skill entrypoint, frontmatter, canonical Codex directory, and Claude symlink checks
- trunk-based branch-flow policy checks for `main` and CI trigger expectations
- cleanup proof checks for bare deferred-obligation markers and undocumented
  feature-toggle or compatibility-bridge metadata
- declarative destination surface guardrails for forbidden old request-batch
  authoring patterns, including aggregate reference HTTP refs, `ReferenceHttp*Client`
  or `ReferenceHttp*Factory` symbols, copied lifecycle loops, and package-local
  request-batch framework helpers
- destination-name hygiene checks for Meta destination connector references, backed by
  `destination_name_hygiene_allowlist.json` and bounded allowlist validation
- destination connector Apache license notice checksum checks for canonical
  Apache-2.0 package license content
- SQL runtime architecture guardrails for SQLGlot import boundaries, raw
  transferable SQL reintroduction under shared SQL runtime modules, per-backend
  driver import boundaries, Snowflake driver imports outside the live sandbox
  test path, the DuckDB facade/shared-runtime boundary, no DuckDB `ATTACH` path
  for executable collect, and Runtime-qualified writes
- validation-path surface checks for implementation tests and fixtures
- destination proof-surface checks that treat `destination_connectors/reference_http` as the repo-local package-shaped dry-run and HTTP connector proof path
- unavailable future repo-contract surfaces such as `pyproject.toml`, `uv.lock`, `Makefile`, `.github/workflows/`, `.agents/skills`, `.claude/skills`, `src/retl/`, and `destination_connectors/`
- unavailable future validation-path surfaces such as `tests/common/` and `tests/fixtures/`

In this docs-first checkout, missing future repo-contract surfaces are reported as `UNAVAILABLE` instead of being treated as validated.
