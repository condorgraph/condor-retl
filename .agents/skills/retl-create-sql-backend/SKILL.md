---
name: retl-create-sql-backend
description: Create and update RETL SQL source backends, including backend packages like DuckDB, Snowflake, Databricks, BigQuery, or any future SQL engine. Use when adding backend-owned source adapters, SQL dialect capabilities, runtime-store wiring, config/auth construction, optional dependencies, CLI operation support, or backend conformance tests.
---

# RETL SQL Backend Work

Use this skill when adding or changing a RETL SQL backend. In RETL, a backend is
the executable SQL source boundary plus the runtime-store implementation needed
to collect source rows into RETL-owned runtime relations.

Backends are engine-specific at the driver, auth, relation-space, and SQL
capability boundary, but must stay repository-native everywhere else. Reuse
RETL's shared Source, SQL, runtime-store, config, auth, CLI, and proof contracts
instead of copying runtime behavior into a backend package.

## Start From Repo Contracts

Before writing code, read the durable contracts that apply to the change:

- `docs/runtime.md` for SQL Backend Relation Spaces, executable collect, source
  modes, Progress, reports, recovery, and CLI operation boundaries.
- `docs/control-plane.md` for package boundaries, active-plan expectations,
  proof obligations, and architecture checks.
- `docs/data-plane-types.md` when source rows, canonical keys, or runtime payload
  handoffs are affected.
- `docs/canonical-model.md` when identifier, Payload, State, Event, or canonical
  key behavior is affected.
- Existing backend packages under `src/retl/backends/duckdb/` and
  `src/retl/backends/snowflake/`.
- Shared contracts in `src/retl/sources/`, `src/retl/sql/`,
  `src/retl/stores/contracts.py`, and `src/retl/stores/sql_runtime/`.
- Backend tests under `tests/backends/`, `tests/sources/`, `tests/sql/`,
  `tests/runtime/`, and `tests/stores/`.
- The active plan under `docs/plans/active/` for slice-specific decisions.

If these sources conflict, durable docs and existing shared contracts win over
plan notes or backend-local precedent.

## Research The SQL Engine

For a new production backend, search the latest official engine documentation
before choosing driver APIs, parameter styles, transaction behavior, JSON
functions, temp table semantics, auth modes, or SQL dialect names. Prefer
official docs for the Python driver, SQL reference, auth, transactions, DDL,
temporary tables, JSON/array operations, hashing, and identifier quoting.

Record non-obvious decisions in the active plan, backend tests, or short code
comments. Do not invent behavior for engines such as Databricks or BigQuery
when official docs are unavailable; ask for docs or approval to proceed with a
clearly marked stub.

## Backend Package Shape

First-party SQL backend packages live under `src/retl/backends/<name>/` and
should match the existing packages unless the engine contract proves a different
shape is needed:

- `backend.py`: immutable `NameSqlBackend` construction, relation spaces,
  `placement`, `source_backend()`, `source_adapter()`, and `runtime_store()`.
- `connection.py`: thin driver adapter implementing RETL's SQL connection
  protocol; import optional drivers lazily and translate missing-driver errors.
- `dialect.py`: backend-owned `SimpleSqlDialect` subclass and explicit SQL
  capability helpers that SQLGlot does not abstract safely.
- `source.py`: backend-owned `SourceBackend` and `SourceAdapter` wiring.
- `schema.py`: runtime schema initialization and backend-specific DDL helpers.
- `store.py`: `SqlRuntimeStore` subclass that creates `SqlRuntimeContext`.
- `auth.py`: only when backend auth is more than simple public config.
- `__init__.py`: public exports and lazy import boundaries.

Keep backend names lowercase and stable. Use classes like
`DatabricksSqlBackend` or `BigQuerySqlBackend`, dialect constants like
`DATABRICKS_DIALECT`, and package extras that match the public backend name.

Do not put destination behavior, declaration semantics, reconciliation logic,
batch ledgers, or generic SQL helpers inside a backend package. Shared runtime
behavior belongs under `src/retl/stores/sql_runtime/`; shared SQL builders
belong under `src/retl/sql/`; source contracts belong under `src/retl/sources/`.

## Relation Space Rules

Every executable SQL backend must expose one coherent backend execution context
with two relation spaces:

- Source relation space: read-only from RETL's perspective.
- Runtime relation space: read/write and RETL-owned.

`SqlCollectPlacement` requires both relation spaces to use the same
`backend_name`. Future backends may separate Source and Runtime by database,
schema, catalog, project, or dataset only when the engine can access both inside
one native execution context. Do not synthesize cross-backend collect with
independently configured databases or compatibility joins.

Validate placement early:

- Backend fields must be non-empty and normalized in `__post_init__`.
- Identifiers must be simple SQL identifiers unless a durable contract explicitly
  allows backend-native quoted names.
- Source and Runtime spaces must be distinct when the backend contract requires
  separation.
- Runtime-owned SQL must qualify runtime relations through the dialect helper.
- Source reads must use the source relation space and must not mutate source
  objects.

## SQL Dialect Capabilities

Generated SQL must use RETL-owned SQL contracts and SQLGlot expression trees
where they apply. Backend-specific differences belong in the backend dialect,
not scattered string patches.

Implement and test the engine-specific behavior needed by collect and runtime
stores:

- SQLGlot dialect name and parameter style.
- `source_relation`, `runtime_relation`, and render helpers.
- Source schema or catalog context switching, with restoration on exit.
- JSON object, array, parse, serialize, concat, and scalar extraction.
- Identifier list expansion for `values` mappings.
- Text casts, concatenation, SHA-256 hashing, and canonical key scalar rendering.
- Temporary relation creation/drop behavior.
- Limit rendering, transactions, rollback behavior, and sequence/upsert support.
- DDL and merge/upsert behavior needed by `SqlRuntimeStore`.

Runtime values must remain driver parameters when the backend supports them.
Never inline secrets or user/runtime values into generated SQL. Use SQL literals
only for validated compiler-owned values such as enum-like object names or
backend-controlled JSON keys.

## Source And Runtime Wiring

Source adapters should use the shared `retl.sources` contracts:

- Build snapshot collection through backend SQL source compilation.
- Build checkpointed Event collection with source-native keyset ordering.
- Validate Arrow schemas and duplicate columns.
- Keep source identity stable and bounded.
- Avoid importing optional drivers at module import time.

Runtime stores should subclass `SqlRuntimeStore`, create a `SqlRuntimeContext`
with the backend dialect, runtime relation space, and optional collect
placement, then initialize the backend schema. For executable collect, reject a
runtime store that was not constructed from the matching `NameSqlBackend`.

`collect` is the only phase that may read Source relations and write Runtime
relations in one execution. After collect, stage, reconcile, sync, recovery,
reports, operations, receipts, and Target Registry behavior must operate from
Runtime relations only.

## Config, Auth, And Dependencies

Public backend settings belong in explicit constructor fields or in a documented
config namespace such as `backends.<name>`. Credentials must use RETL auth or
secret-resolution primitives and resolve only when opening a connection.

For production backends:

- Add a `from_config(...)` constructor when operators need namespace-based
  construction.
- Use shared auth primitives where possible; add backend `auth.py` only for
  engine-native auth shape.
- Keep account, warehouse, host, HTTP path, project, dataset, catalog, schema,
  runtime location, and similar public settings out of credential namespaces.
- Never persist resolved passwords, tokens, private keys, OAuth material,
  cookies, or auth-bearing URLs in stores, reports, traces, or diagnostics.
- Add optional dependencies to `pyproject.toml` extras and keep driver imports
  lazy so importing `retl` works without every backend installed.

## CLI And Operator Surface

When runtime operations should support the backend, update the CLI operation
construction path in `src/retl/cli/main.py` and docs examples. CLI flags must
reconstruct a repeatable runtime store from backend inputs; they must not accept
live connection objects or print resolved credentials.

Add backend-specific operation examples only after the runtime store exists and
inspection can prove it. Keep Source relation-space settings distinct from
Runtime relation-space settings in flags and documentation.

## Proof Requirements

Start with failing or characterizing tests for the backend slice. Keep tests
mocked by default and reserve live sandbox tests for explicitly opt-in paths.

Cover at least:

- Backend construction validation, relation-space placement, and distinctness.
- Optional dependency import boundaries and missing-driver errors.
- Connection adapter parameterized execution and row/Arrow fetch behavior.
- Dialect rendering for relation paths, JSON, hashing, canonical keys, temp
  tables, transactions, DDL, upsert/merge, and limits.
- Source adapter snapshot and checkpointed query execution.
- Runtime schema initialization and `SqlRuntimeContext` wiring.
- Executable collect conformance through shared runtime tests.
- CLI operation construction when the backend is exposed there.
- Config/auth success, missing values, ambiguous auth, redaction, and lazy
  secret resolution when config/auth is part of the slice.

Use targeted tests while iterating, then run the applicable repo checks:

```bash
uv run pytest tests/backends tests/sources tests/sql tests/runtime tests/stores
uv run python tools/checks/validate_repo_skeleton.py
uv run python tools/checks/validate_architecture.py
make check
```

Also run `make lint-lock` when dependency, packaging, or lockfile surfaces
change.
