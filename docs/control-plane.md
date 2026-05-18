# Control Plane

## Overview

The control plane makes this repository self-describing to agents and contributors. It is not optional documentation — it is part of the implementation runtime. When an agent needs to answer "what should this repository look like?", "where does this code belong?", or "what checks must pass before I can call this done?", the answer must come from the compact root docs in `docs/`, not from chat history, plan prose, or root redirect files.

The control plane owns:

- the docs taxonomy and navigation contract
- the repository layout and top-level directory roles
- package boundaries and dependency direction rules
- the Python toolchain, build, and lockfile contract
- the branch, versioning, and release policy
- the validation and quality-gate policy
- the agent orientation and AI-assisted development harness

Root files such as `AGENTS.md` and `ARCHITECTURE.md` are short convenience redirects into `docs/`. They do not carry independent policy and must not compete with the compact docs for structural truth.

---

## Docs Taxonomy

The repository docs tree is a knowledge base with explicit role boundaries. The compact root pages are the primary source-of-truth surface:

**`docs/control-plane.md`** — repository structure, docs taxonomy, toolchain, lifecycle, validation, safety, reliability, quality gates, and agent workflow.

**`docs/core-beliefs.md`** — directional product and engineering ideals that shape implementation choices without replacing the dedicated runtime, product, destination, or control-plane contracts.

**`docs/product.md`** — public State/Event behavior, authoring model, Sync execution, destination binding from the operator view, run modes, and recovery workflows.

**`docs/runtime.md`** — runtime phase contracts, destination scan progress, destination batch ledgers, reports, dry run, recovery, errors, and idempotency.

**`docs/recovery.md`** — ledger-first recovery, retry behavior, durable destination batch state, operator repair paths, and recovery ownership between runtime and destinations.

**`docs/data-plane-types.md`** — exact SQL, Arrow, Python, and JSON handoff boundaries across collect, stage, reconcile, sync, and destination request planning.

**`docs/canonical-model.md`** — canonical State/Event identity, operation semantics, fingerprints, Progress boundaries, and sensitive-data boundaries.

**`docs/destinations.md`** — destination connector contract, Destination Surface compatibility, Target lifecycle, delivery outcomes, receipts, packaging, and conformance proof.

**`docs/examples.md`** — curated examples only. Examples illustrate the compact docs but do not override them.

**`docs/appendices.md`** — support context, taxonomy summaries, and schema-catalog disposition.

**`docs/plans/`** — execution-planning policy and deferred-work ledger only. Active plans are temporary work artifacts and closed plan records are not retained in the repository. Intentionally deferred work registers in `docs/plans/deferred-work.md`. Plans are support artifacts, not independent policy surfaces. Durable decisions produced by a plan must be promoted into the compact root docs before the plan can close.

**`docs-site/`** — Docusaurus source for public user-facing documentation:
install, quickstart, concepts, task guides, examples, and reference tables for
using RETL. It is not a mirror of `docs/` and must not become the home for
repository-control, contributor, architecture, runtime-contract, or active-plan
policy.

Generated documentation must be produced by repo-owned generation commands and
must declare its source and generation command. Do not hand-edit generated
artifacts.

### Navigation contract

`docs/index.md` is the durable docs entrypoint. The maintained compact
entrypoint links to the root pages and support subtrees. The tree must be
readable without first opening a parent-directory file.

---

## Repository Layout

The repository layout is part of the control plane. If the structure changes, the layout doc changes in the same unit of work.

### Top-level directories

| Path | Role |
|---|---|
| `src/retl/` | Core runtime library — the only home for core runtime code |
| `destination_connectors/` | First-party publishable destination packages |
| `tests/` | All tests — does not contain repo checks |
| `docs/` | Durable knowledge base |
| `docs-site/` | Docusaurus public user-docs source |
| `tools/checks/` | Repository-local structural and architecture checks |
| `.agents/skills/` | Canonical repo-owned shared agent skills |
| `.claude/skills` | Symlink to `../.agents/skills` for Claude discovery |
| `src/retl/skills/user/` | Packaged end-user AI skills shipped in the main wheel |
| `.github/workflows/` | Executable CI and release workflow definitions |

### Required root artifacts

The repo contract is incomplete if any of these are missing: `.gitignore`, `CONTRIBUTING.md`, `LICENSE.txt`, `README.md`, `pyproject.toml`, `uv.lock`, `Makefile`, `.github/workflows/main.yml`, `.github/workflows/lint.yml`, `.github/workflows/test_common.yml`, `.agents/skills/retl-create-destination/SKILL.md`, and `.claude/skills` (symlink).

Root-level rules that govern placement:

- Core runtime code belongs only under `src/retl/`.
- First-party publishable destination-package code belongs only under `destination_connectors/`.
- Repo-owned shared agent skills belong only under `.agents/skills/`. Claude reaches the same content through `.claude/skills`; no duplicated skill content is permitted.
- Packaged end-user skills belong under `src/retl/skills/user/` and are product assets shipped with `condor-retl`. They are installed into user projects by `retl install-skills`; they are not the contributor skill source used to develop this repository.
- Tests do not live under `retl/`. Repo checks do not live under `tests/`.
- Durable documentation does not live under `README.md`, `AGENTS.md`, or `ARCHITECTURE.md` beyond short summaries and navigation links.
- Public user documentation lives under `docs-site/`. Keep repository policy,
  implementation contracts, and agent workflow rules in `docs/`; keep
  task-oriented user pages in `docs-site/`.

### `destination_connectors/` package template

Each first-party destination connector package follows the pattern:

```text
destination_connectors/<connector>/
  pyproject.toml
  retl_<connector>/
    definitions.py
    hooks.py       # optional — only when bounded custom code is required
  tests/
```

`destination_connectors/*/pyproject.toml` declares first-party destination package metadata, a `condor-retl-<connector>` distribution name, Apache-2.0 license metadata, a bounded `condor-retl` compatibility range, and the `retl.destinations` entry point. Destination connector packages participate in the root-managed `uv` environment and the shared root `uv.lock`.

Connectors that support namespace-based public config loading declare bounded
`config_namespace_fields` metadata in `definitions.py`. Core binding code may
read only those declared paths from a `config_namespace`; target mappings and
test-only injected objects remain explicit. Runtime-store-backed Target
Registry records are operational state owned by the runner's selected runtime
store, not connector package config. First-party partner connector definitions
must not expose arbitrary `base_url` through `config_namespace_fields`; partner
API origins are connector-owned constants, with `retl/reference-http` retained
as the generic configurable HTTP endpoint exception.

### Repository checks

All repository-local structural and architecture checks live under `tools/checks/`, named with the `validate_<subject>.py` pattern. The current required checks are `validate_repo_skeleton.py` and `validate_architecture.py`. Checks must be callable from the `Makefile` and test suite. Compiled cache or bytecode artifacts are never source enforcement surfaces.

When the current checkout lacks a documented root or package surface, checks must report that gap explicitly rather than treating the surface as validated.

> **Note:** Exact file inventory details for `src/retl/`, `destination_connectors/`, and `tests/` are candidates for future generated or code-owned treatment.

---

## Runtime and Package Shape

The `retl` package is a library, not a platform. It owns stage contracts, durable correctness state, replay, restore, and built-in convenience runners. Teams that need wider parallelism may add external orchestration above these stage boundaries without changing `retl`'s correctness rules.

### Package ownership

| Package | Role |
|---|---|
| `src/retl/declarations/` | Public declaration constructors, policies, destination bindings, and secret references |
| `src/retl/runtime/` | Runner execution, runner orchestration, phase evidence, results, reports, and recovery |
| `src/retl/artifacts/` | Columnar artifact, Arrow IPC, batch, and manifest helpers |
| `src/retl/state_runtime/` | State identity, current-state storage, ordered work production, staging, operations, and reconciliation |
| `src/retl/events/` | Event source-window reads, source-native keyset ordering, and reconciliation |
| `src/retl/sources/` | Source contracts, fixtures, and backend-neutral SQL source helpers |
| `src/retl/destinations/` | Bindings, surfaces, compatibility, target, operation, delivery outcome, and failure contracts |
| `src/retl/sync_runtime/` | Sync submission orchestration and sync-phase evidence |
| `src/retl/sql/` | Shared generated-SQL contracts, SQLGlot AST adapters, dialect capability contracts, and connection protocols |
| `src/retl/stores/` | Runtime store contracts and backend-neutral SQL runtime semantics |
| `src/retl/backends/` | Concrete SQL backend packages, one package per backend |
| `src/retl/cli/` | Operator CLI layer — kept outside the public authoring API |
| `destination_connectors/` | First-party publishable destination packages |

### Dependency direction rules

Core runtime must not import concrete source or destination packages. Concrete
SQL backend packages are also outside runtime orchestration dependencies.
Backend-specific SQL implementation stays in backend packages.
Partner-specific logic stays at destination edges. Public API assembly belongs
at `src/retl/__init__.py`.

The runtime phases `collect`, `stage`, `reconcile`, and `sync` are not separate products. External orchestration may invoke those stages through committed artifacts and the bounded `retl.runtime` callable surface, but it must not redefine package semantics, replay rules, receipt semantics, or destination-progress semantics.

Core runtime must not branch on `duckdb`, `snowflake`, or any other concrete
backend name in orchestration, package lifecycle, canonical diff logic, or
replay gating. No warehouse-specific logic belongs in runner, stage, reconcile,
sources, stores, shared SQL, or destinations packages. No destination-specific
logic belongs in runner, stage, reconcile, sources, stores, shared SQL, or
backend packages.

Generated SQL belongs in shared SQL modules under `src/retl/sql/` or in
runtime-store modules that consume those shared SQL contracts. Runtime modules
should build generated query SQL through SQLGlot-backed helpers and explicit
backend capabilities, not by scattering backend-specific SQL strings through
phase orchestration or shared runtime modules. SQLGlot does not own RETL
semantics: relation-space validation, runtime table ownership, parameter
binding, transactions, progress, and destination ledger rules remain RETL
contracts.

Concrete SQL backend implementation belongs under
`src/retl/backends/<backend>/`. Backend packages own concrete Source relation
placement wiring, runtime-store wiring, dialect capability implementations,
connection objects, driver imports, and backend-specific SQL behavior for that
backend.
Shared runtime semantics remain backend-neutral under packages such as
`src/retl/stores/sql_runtime/`, `src/retl/sql/`, `src/retl/sources/`, and
`src/retl/stores/`.

Concrete driver imports are mechanically bounded. DuckDB imports, Snowflake
imports, BigQuery Google client imports, and future SQL driver imports are
allowed only inside the corresponding backend package or explicitly documented
test fixtures. Runtime orchestration calls generic Source, SQL backend, and
runtime-store contracts and must not import concrete drivers or concrete
backend packages directly.

### Key module responsibilities

- `src/retl/__init__.py` — the only root public re-export surface.
- `src/retl/declarations/` — owns source, state, event, sync, destination-binding, policy, and secret declaration types.
- `src/retl/runtime/` — owns runner execution, public runtime results, and runtime exception surfaces.
- `src/retl/artifacts/`, `src/retl/state_runtime/`, `src/retl/events/`, `src/retl/sources/`, `src/retl/sync_runtime/`, `src/retl/stores/`, and `src/retl/backends/` — own their corresponding runtime implementation boundaries.
- `src/retl/sql/` — owns SQLGlot-backed generated-SQL helpers, parameter
  allocation, validated SQL names, dialect capability contracts, and connection
  protocols shared by SQL runtime stores, source SQL helpers, and concrete SQL
  backend packages.
- `src/retl/backends/<backend>/` — owns the concrete SQL backend package for
  one backend, including Source relation placement, runtime-store adapter,
  dialect capability implementation, connection implementation, driver imports,
  and backend-specific SQL behavior.
- `src/retl/destinations/` — owned by the core repo because the repo owns shared destination contracts and loading surfaces.
- `src/retl/auth.py` — owns shared Auth Mode declarations, credential
  validation, secret resolution helpers, and redacted auth evidence for
  destinations and backend-native connectors.
- `src/retl/declarations/secrets.py` — owns the runtime-owned secret-reference surface.

Pre-release root compatibility modules for direct imports such as `retl.stage`,
`retl.results`, `retl.specs`, `retl.collect`, `retl.reconcile`,
`retl.progress`, and `retl.runner` are not part of the active package model.
Use `import retl` for the public root API and canonical ownership packages for
implementation imports.

Shared auth implementation belongs in `src/retl/auth.py`. `import retl.auth`
is the canonical submodule import for Auth Modes and runtime credential
resolution helpers. Destination packages and backend packages must import
shared auth helpers from `retl.auth`, not from destination-owned modules.
Snowflake backend auth is owned by `src/retl/backends/snowflake/` and must use
shared native Auth Modes plus explicit `backends.snowflake` config and
credential namespaces. BigQuery backend auth is owned by
`src/retl/backends/bigquery/` and must use application default credentials or
service-account credentials resolved from an explicit `backends.bigquery`
credential namespace. The old destination-auth module path and raw Snowflake
connection-JSON credential path are not compatibility surfaces.

### Growth model

Built-in SQL backend growth adds one backend package under
`src/retl/backends/<backend>/`. Shared source contracts, shared store
contracts, generated-SQL contracts, and backend-neutral SQL runtime semantics
remain in their existing shared packages. DuckDB and Snowflake are built-in SQL
backend packages; each owns its concrete Source relation placement,
runtime-store wiring, dialect behavior, connection wrapper, optional driver
import, and backend-specific SQL behavior under its own package. Official
destination growth happens through publishable packages under
`destination_connectors/`.
BigQuery is a built-in SQL backend package using the official
`google-cloud-bigquery` and `google-cloud-bigquery-storage` clients behind
`src/retl/backends/bigquery/`. PostgreSQL is a built-in SQL backend package
using the official Psycopg 3 driver behind `src/retl/backends/postgresql/`.
BYO destination growth is supported through external packages implementing the
same `DestinationDefinition` contract.

> **Note:** Exact types, interfaces, and callable signatures across runtime modules are candidates for future generated or code-owned treatment.

---

## Toolchain and Packaging

### Locked defaults

- **Package manager:** `uv` — used for local development, dependency locking, build, and publish.
- **Root `pyproject.toml`:** the monorepo root and source of truth for the `condor-retl` distribution metadata, supported Python range, repo-level dependencies, console entry points, and build backend configuration.
- **`uv.lock`:** the single committed dependency-resolution artifact for the root package and all first-party destination packages. It is a repository contract artifact, not a disposable local file.
- **`Makefile`:** the canonical command surface for contributors, agents, and CI jobs.
- **Build backend:** `hatchling` is the default for the main `retl` distribution unless a durable exception is documented in the design docs.

### Python version policy

The repo declares `requires-python = ">=3.12,<3.15"`. CI must cover Python 3.12, 3.13, and 3.14 through an explicit matrix. Python 3.12 is the local lint and type-check baseline. The authoritative version contract lives in `pyproject.toml`.

### Dependency model

Core library dependencies live under `project.dependencies`. `pyarrow` and
SQLGlot are core library dependencies — they must not be hidden behind optional
extras. SQLGlot is RETL's generated-SQL AST and rendering layer.
Source-driver dependencies for built-in warehouse adapters are exposed through
strict root extras on `retl`. Development and maintenance dependencies live in
`dependency-groups`, not in the runtime dependency surface. Each destination
connector package declares its own bounded `retl` compatibility range in
`destination_connectors/*/pyproject.toml`.

Dependency additions or removals in any `pyproject.toml` must update `uv.lock` and any affected contributor or CI command surfaces in the same unit of work.

### Canonical command surface

The `Makefile` must expose these command families, backed by `uv`:

- `make install-uv`, `make dev` — environment bootstrap (`make dev` runs `uv sync --all-extras --group dev` and prepares the full shared monorepo environment)
- `make format` — Ruff formatter (`uv run ruff format .`)
- `make format-check` — non-mutating formatting check (`uv run ruff format --check .`)
- `make lint` — Ruff lint (`uv run ruff check .`)
- `make typecheck` — MyPy (`uv run mypy src tests` plus active first-party destination connector package paths)
- `make lint-lock` — lockfile consistency check (`uv lock --check`)
- `make test` — main test suite (`uv run pytest tests destination_connectors/reference_http/tests destination_connectors/meta/tests -q -n auto -m "not live_sandbox"`)
- `make test-sandbox-meta` — opt-in live Meta sandbox validation, excluded from
  default checks by the `live_sandbox` marker
- `make check` — default verification baseline: runs `format-check`, `lint`, `typecheck`, `test`, and the `validate_repo_skeleton.py` and `validate_architecture.py` checks through `uv run python`
- `make build-library`, `make publish-library` — core distribution build and publish
- `make build-destination-connector PACKAGE=<connector-directory>` — active
  destination connector package build, for example `PACKAGE=meta`
- `make publish-destination-connector PACKAGE=<connector-directory>` —
  publish artifacts from the connector package `dist/` directory. The
  repo-local `reference_http` connector is not a PyPI publishing target.

CI workflows run for pushes to `main` and pull requests targeting `main`, and use the same `Makefile` and `uv` entrypoints wherever practical.

There is no repo-level `build-source` or `publish-source` command family for first-party built-in sources — those adapters ship inside the main `retl` distribution.

### Runtime Operations Contract

The public operator repair surface is `runner.operations`. Operations are
explicitly scoped helpers over the configured runtime store:

- bounded inspection: runtime-store, declaration, destination-scope,
  collect-id, Target Registry, and run evidence summaries
- skip helpers: unresolved destination-batch dismissal and scoped skipped
  ledger coverage for ordered-work or Event keyset ranges
- reset and rebaseline helpers: runtime-store reset, destination-scope reset,
  collect-id deletion, ordered-work range deletion, and State rebaseline
- isolated cleanup: Target Registry reset and diagnostic run/report evidence
  deletion

Operation helpers mutate existing runtime-store authority tables directly.
They must not add operation ledgers, repair-history tables, compatibility
aliases, or translated legacy command names. `run_id` is diagnostic evidence,
not a restore boundary.

The operator CLI exposes the same forward vocabulary under
`retl operations ...`. CLI commands are thin wrappers over
`runner.operations`; they must not issue direct operation SQL or import user
declaration scripts to recover a Sync. Destination-scoped commands accept
explicit `--sync-name`, `--destination-name`, `--surface`, `--family`, and
`--declaration-name` flags. CLI output is compact JSON by default and follows
the same redaction boundary as reports and inspection artifacts.

### User Skill CLI

The user-facing AI setup surface is `retl install-skills`. It installs the
packaged end-user skill set into the
project-local `.agents/skills/` and `.claude/skills` directories by default so
Codex-style and Claude-style project skill discovery see the same packaged
content. A caller may choose a different single project-local destination with
`--destination`. Existing unchanged skill files are left alone, changed skill
files are overwritten from the packaged copy, and unrelated project files are
preserved.

Project initialization itself is skill-driven. The packaged
`retl-start-project` skill guides an AI agent to inspect the user's repository,
source backend, destination, and operating model before creating files.

---

## Lifecycle and Release Policy

### Branch policy

The canonical trunk branch is `main`. `main` is the source of truth for normal
development, release candidates, hotfixes, tags, and rollback decisions.
Routine work uses small, short-lived branches that target `main`. Recommended branch prefixes are `feat/`, `fix/`, `chore/`, and `docs/`. Direct commits to `main` are allowed only when repository branch protection and required checks permit them. Legacy `devel` and `master` branch names are not normal integration, release, or hotfix branches for this repository.

Branches should stay narrow enough to merge after one review cycle. Incomplete user-facing behavior may land on `main` only when it is hidden from unsupported users, preserves public contracts, and has tests or checks proving the disabled or bridged behavior.

### Versioning

The main `retl` distribution and each first-party destination package use semantic versioning independently. Breaking changes require a major version bump. Minor versions may add features and compatibility-preserving migrations. Patch versions are reserved for bug fixes and low-risk corrections. The initial public `retl` release line is `0.1.x`. The initial public destination-connector compatibility baseline is `>=0.1,<0.2`.

### Deprecation and compatibility

Public API changes that would break existing user code require either a major version bump or an explicit compatibility bridge with deprecation guidance. Backward compatibility is an implementation contract, not a review preference, and must be covered by automated tests where the repo claims continuity. Changes to persisted artifact, state, or schema shapes must carry explicit version fields and documented migration or rejection behavior.

Core major versions may break destination connector compatibility when documented explicitly. Core minor versions may add runtime or toolkit capabilities but must preserve existing connector contracts or ship an explicit bridge.

### Release flow

Release candidates are selected from a proven `main` commit after applicable local and CI checks pass. Tags correspond to the version declared in `pyproject.toml` for the relevant package. Hotfixes start from `main` whenever possible. Rollbacks select a previous known-good `main` commit or release tag, with follow-up work captured by a test, fixture, check, or documented operational proof.

An undocumented difference between actual branch flow and documented branch flow is a repository bug.

---

## Validation and Feedback Loops

### Core rule

The repo must provide repository-local, reproducible, machine-checkable validation paths for meaningful runtime and connector work. Human-only QA, partner UI inspection, or manual dashboard checking are not valid primary feedback loops.

Built-in `retl/mock` is the core runtime test double for synthetic destination
outcomes. `destination_connectors/reference_http/tests` is the canonical
package-shaped dry-run proof surface for destination execution, request
planning, transport submission, receipts, and destination batch ledgers. That
surface is expected to exercise the evidence boundary:
`resolved_targets.json`, `submissions.jsonl`, `receipts/summary.json`,
`receipts/succeeded.jsonl`, `receipts/accepted.jsonl`,
`receipts/failed.jsonl`, and `receipts/pending.jsonl`.

### Blocking check categories

Every meaningful change must pass all applicable categories before completion:

1. **Static quality checks** — formatting verification, linting, type checking, import and dependency hygiene. Blocking for all code changes. Default baseline: `make check`. Add `make lint-lock` when dependency, packaging, or lockfile surfaces change.
2. **Structural architecture checks** — allowed dependency directions, forbidden cross-layer imports, top-level file and directory expectations, naming and placement rules.
3. **Documentation consistency checks** — required control documents exist for shipped behavior; docs change when behavior or architecture changes; generated artifacts include provenance.
4. **Automated tests** — scope depends on change type: core runtime changes require unit and integration tests; connector changes require unit tests plus connector contract tests; control-plane changes require structural tests plus packaging or command surface validation.
5. **Dry-run or connector validation** — where relevant, dry-run runner execution through `destination_connectors/reference_http/tests` or a connector-owned equivalent. Connector work is not complete if it only passes unit tests.

### Validation output requirements

Validation output must distinguish between violated rules and unavailable proof surfaces. Unavailable proof surfaces are acceptable only when the current checkout does not yet expose the documented source files or executable artifacts. When a surface is unavailable, validation must name the missing paths explicitly. Failure messages must say what failed, why it matters, what rule was violated, and where to inspect next.

### Stage validation expectations

Each runtime stage must have a reproducible validation path:

- `collect` — source query fixtures, pagination behavior, SQL-backed current-state commits, ordered work creation, collect ID assignment for State provenance, and Event source-native keyset position projection
- `stage` — pending ordered-work page fixtures, destination scan lower bounds, current-snapshot upsert pages, and inspectable staged-package outputs
- `reconcile` — canonical mutation fixtures, Sync removal-policy suppression behavior, Event Import packaging, and destination-mapping validation
- `sync` — connector contract behavior, destination-definition loading, partial-failure handling, receipts, delivery outcomes, and destination batch ledger boundaries

For `sync`, destination work is incomplete unless the changed path passes
`destination_connectors/reference_http/tests` or an explicitly documented
connector-owned equivalent dry-run path that emits `resolved_targets.json`,
`submissions.jsonl`, `receipts/summary.json`, `receipts/succeeded.jsonl`,
`receipts/accepted.jsonl`, `receipts/failed.jsonl`, and
`receipts/pending.jsonl`.
Runtime report proof for failed destination batches must expose failed batch
ledger counts, retry metadata, and bounded samples without raw payloads,
credentials, auth-bearing values, or unbounded partner responses.

Destination auth must be mechanically visible as auth modes. First-party
connectors must not rely on flat `credential_fields` as an auth authoring
surface, and toolkit helpers must not synthesize default modes from flat fields.
Architecture checks must flag toolkit fallback synthesis so new connectors do
not recover pre-auth-mode ambiguity.

Live partner sandbox tests are optional smoke validation, not a default quality
gate. They must be excluded from `make check`, require explicit opt-in, scope
credentials to sandbox or disposable partner resources, use synthetic data, emit
redacted structured evidence, and skip cleanly when required environment values
are absent.

### Dry-run policy

Dry runs are a first-class validation mode. A dry run must execute real runner logic without causing irreversible partner mutations, emit inspectable artifacts, produce structured diagnostic output, and support deterministic comparison where golden outputs exist. Partner-shaped payload dumps or transport-only success markers are not sufficient proof.

### Observability

Structured logs are required when they are the primary way to diagnose stage behavior or prove recovery semantics. Metrics and traces are required where they materially improve failure diagnosis. Observability artifacts must follow the same redaction boundary as state and inspection artifacts.

RETL logging uses the standard Python `logging` package under the package
logger namespace `retl`. Module loggers must be normal child loggers such as
`retl.runtime.executor`, and embedding applications remain free to configure
handlers, levels, propagation, and filters through ordinary Python logging
mechanisms. Importing `retl` must not configure root logging, add root handlers,
change root levels, or emit logs by default. RETL may expose an opt-in helper
for operator convenience, but that helper must configure only the `retl`
logging surface unless the caller explicitly chooses otherwise.

Operator-facing RETL log formatting must support both readable text output and
parseable JSON output. JSON logs should use stable field names for runtime
context where available, but logs remain live diagnostics. They are not
durable authority for replay, recovery, checkpointing, retry decisions, audit,
or destination delivery outcomes.

Operator console progress is a separate optional human-facing surface, not a
logging format and not durable authority. Library callers must opt in with the
`console=...` runner construction argument, using renderers exposed by
`retl.console` such as `retl.console.text(...)` or `retl.console.null()`.
Runner execution emits bounded console callbacks for operator summaries.
Console renderers consume bounded runtime events and counters rather than
parsing formatted log lines, and they follow the same redaction and data
minimization boundary as logs, reports, traces, and
inspection artifacts. Logs, reports, ledgers, rendered Run Indexes, progress
records, receipts, and runtime-store tables remain the diagnostic surfaces for
replay, recovery, retry, audit, progress advancement, and delivery evidence;
durable runtime authority remains in runtime-store rows such as `runs`, Sync
Reports, destination batch ledgers, destination progress, receipts, and Target
Registry records.

### Cleanup

Feature toggles and compatibility bridges must name the public behavior they hide or preserve, the validation surface that proves the hidden path, and the condition that removes the bridge. Deferred TODOs must either be cleared in the implementing change or recorded in `docs/plans/deferred-work.md`. Doc and check drift is a validation failure when a repo-local check can detect it.

---

## Safety, Reliability, and Completion Standards

### Security boundary

The runtime security boundary is strict secret exclusion, field minimization, redaction, operator-owned storage, and explicit retention or purge policy. The library does not manage encryption keys or wrap customer-data artifacts in opaque encrypted blobs by default.

Secret material is forbidden in persisted runtime surfaces, including bindings, manifests, state mirrors, replay artifacts, traces, logs, generated docs, and advisory summaries. Secrets resolve from environment variables or configured read-only secret backends; explicit Python auth overrides are process-local only. Fresh-worker replay, collection, restore, and CLI recovery must re-resolve auth instead of reading serialized credentials.

Customer-derived runtime data is sensitive even when it is not secret material. Source snapshots, canonical artifacts, receipts, destination scan progress, destination batch ledgers, partner-returned operational metadata, logs, traces, and inspection surfaces must persist only what correctness, replay, resume, diffing, or bounded inspection requires. Logs, traces, contracts, and broad inspection metadata must expose summaries, counts, ids, package references, fingerprints, and redacted diagnostics rather than raw identifiers, traits, payload fragments, partner URLs, account ids, auth-bearing locators, or validation blobs.

External and generated destinations are executable Python dependencies. Entry-point registration and hook imports run with host-process privileges. Bounded hook kinds are API-boundary controls, not sandboxing, package signing, attestation, or allowlist enforcement. Operators choose which connector code to install and should pin versions, isolate execution where needed, and scope credentials to minimum required access.

### Reliability boundary

Resume order, durable authority, partner-specific behavior, and failure
handling must remain explicit enough that implementation can proceed from
repo-owned context. Destination scan progress and the destination batch ledger
are the durable authority for recovery. The scan cursor records scanned source
work that has durable batch ledger rows; it is not receipt-gated completion
state. Retry recovery selects bounded `pending` rows and `failed` rows whose
retry metadata allows automatic retry, once per overall run before new scan
work for that destination scope. Remote tracking and accepted-batch
finalization are not active recovery paths in this contract. Durable backends,
artifact locators, destination scan progress, destination batch rows, attempt
evidence, and redacted receipts are authority; local mirrors and inspection
files are not alternate recovery truth in durable mode.

Reliability proof must be repository-local. Runtime and connector work needs tests, dry runs, fixtures, simulators, or equivalent proof paths. Logs and inspection artifacts must be sufficient to diagnose failures and prove recovery behavior without relying on manual partner UI inspection.

### Operational safety

Framework-owned safety includes target validation before irreversible sync, explicit run modes, dry-run and inspection paths for pre-execution review, bounded runtime operations, destination-declared execution budgets, bounded retry budgets, and destination batch ledger outcomes. Upload completion, request success, or job submission alone is not definitive success; connectors must classify delivery as `accepted`, `succeeded`, or `failed`.

Operators own environment isolation, credential scoping, rollout and approval policy, destination-specific change management, and organization-specific blast-radius controls. `retl` validates target identity, execution boundaries, and destination-declared throttling constraints; it does not enforce every downstream change-management policy.

### Retention and purge

Retention is a runner-runtime concern, not a `SyncSpec` concern. Completed payload artifacts are purgeable after successful completion and retention expiry. Collect-scoped ordered work remains until relevant destination scan cursors and unresolved destination batch ledger evidence allow compaction. Destination scan progress and destination batch ledger rows are retained until explicit cleanup or drop.

Current defaults: local mode has no automatic TTL; durable mode retains completed payload artifacts for 7 days, logs and traces for 30 days, and sync receipts for 30 days after durable sync completion. Once required artifacts are purged, replay is no longer guaranteed for those packages. Purge must not delete pending ordered work, current destination scan progress, unresolved destination batch ledger rows, or current state needed for current-snapshot staging as part of artifact-retention cleanup.

### Completion standard

Work is complete only when applicable compact docs, code, tests, generated or structural checks, and proof artifacts agree. The current quality baseline is: compact control-plane and product/runtime docs are navigable from `docs/index.md`; plan folder contracts are explicit and mechanically checkable; repo-local architecture checks exist; runtime replay, inspection, operator-boundary, dry-run, built-in mock outcome, and `reference_http` package proof paths are covered by targeted tests; and future public recovery or inspection surfaces extend checks when added.

---

## Agent Workflow

### Orientation order

Before making any structural change, an agent reads in this order:

1. `docs/index.md` and the relevant compact root docs
2. The relevant temporary active plan, only after the compact docs above
3. Plan `subplans/` only when that plan uses them and a compact doc or plan points there explicitly
4. Supporting references only when a compact doc, active plan, or repo-owned skill points there explicitly

The docs tree is the system of record. Prompts, chat history, and generated summaries are not durable policy unless promoted into the repository.

### Planning requirement

Non-trivial work requires an active plan before implementation starts. A plan is required for new connectors, stage contract changes, cross-package refactors, architecture or quality-gate changes, and any work expected to take multiple implementation steps. Small isolated fixes may proceed without a standalone plan if they do not change structure or behavior broadly.

Each active plan must include sections covering `Outcome`, `Plan Contract`, `Depends on`, `Locked Inputs`, `Scope`, `Non-goals`, `Implementation Sequence`, `Proof Obligations`, `Required Tests and Fixtures`, `Docs That Must Be Updated When Complete`, and `Acceptance`. The `Plan Contract` section must name `status`, `owner`, `linked subsystem docs`, `decision log`, and `next steps`.

Plan decision logs are provenance ledgers: record the source file, promoted or clarified rule, and stable target. Do not restate the full durable rule body in a plan.

### Behavior-change contract

Meaningful changes update code, tests, docs, and checks together. A change that alters durable behavior must update the durable docs in the same unit of work. Repository-local checks block completion. Human review is not a substitute for repository updates.

When a behavior can be captured mechanically, start from a failing test, characterization test, fixture, architecture check, or other repo-local proof surface. Review AI-assisted changes adversarially against stable docs, public contracts, and failure modes — not just because generated code compiles.

### AI-assisted development harness

Generated code must be reviewed against stable contracts, edge cases, compatibility obligations, and tests before merge. Post-merge cleanup is part of the workflow: stale TODOs, expired feature toggles, stale compatibility bridges, and doc/check drift must be removed or recorded in `docs/plans/deferred-work.md`.

### Quality gates summary

- `make check` is the default completion baseline for ordinary code changes.
- `make lint-lock` is blocking when dependency, packaging, or lockfile surfaces change.
- Lockfile consistency, packaging metadata, and release-policy consistency are blocking whenever the repo-contract surface changes.
- Trunk-policy consistency is blocking whenever lifecycle, contributor, workflow, or control-plane surfaces change.
- Control-plane changes must preserve root navigation, subtree-index coverage,
  and generated-vs-authored ownership.

---

## What This Page Does Not Own

The following topics belong to other compact docs areas:

- **Runtime phase contracts** (`collect`, `stage`, `reconcile`, `sync`), durable boundary rules, storage tiers, artifact authority, and replay compatibility → `runtime.md` page
- **Public Python API** (`retl.runner(...)`, `@retl.source`, etc.), authoring model, execution narrative, and operator workflows → `product.md` page
- **Destination connector model**, capabilities, toolkit contract, target lifecycle, packaging and registration, and proof-surface conformance → `destinations.md` page
- **Canonical identity and mutation model**, operation and diff semantics → `canonical-model.md`
- **Concrete usage examples** (source authoring, runner declaration, operator commands) → `examples.md` page

---
