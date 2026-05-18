---
name: retl-cli-operations
description: Troubleshoot RETL runtime-store state with the `retl operations` CLI, including bounded inspection, run-shape discovery, scoped reset, rebaseline, skip, and diagnostic evidence cleanup.
---

# RETL CLI Operations Troubleshooting

Use this skill when a user wants an AI agent to understand the shape of a RETL
run, inspect runtime-store state, diagnose stuck or failed Sync progress, or
choose a scoped repair/reset operation through the CLI.

The CLI surface is `retl operations ...`. It is a thin wrapper over
`runner.operations`; do not bypass it with direct SQL for normal
troubleshooting. Start read-only, preserve redaction boundaries, and treat every
reset/delete/rebaseline command as a mutation of runtime authority.

## Start From Repo Contracts

Before changing behavior or recommending destructive repair, read the contracts
that apply to the question:

- `docs/control-plane.md` for the Runtime Operations Contract and CLI boundary.
- `docs/runtime.md` for progress, reports, inspection, reset, rebaseline, and
  `run_id` semantics.
- `docs/recovery.md` for failure recovery and ledger-first repair behavior.
- `docs/examples.md` for current CLI usage examples.
- `src/retl/cli/main.py` for the command flags currently exposed by the CLI.
- `docs/plans/active/003-cli-runtime-operations/README.md` when active
  implementation details or acceptance notes matter.

If these sources conflict, durable docs and code win over plan notes.

## Operating Rules

- Use the repo-local environment: prefix commands with `uv run`.
- Prefer `--pretty` while reading results yourself; omit it only when a caller
  needs compact JSON.
- Resolve the actual runtime operation context before inspecting: backend,
  runtime-store location or namespace, schema, and credential namespace when
  applicable. Do not assume the CLI fallback flags match the runner that
  produced the evidence.
- Inspect before mutating. Capture the exact destination scope, declaration,
  collect sequence, ordered-work range, target, run id, or report filter before
  choosing a repair command.
- Ask for explicit user confirmation before destructive CLI mutations unless
  the user already requested that exact mutation.
- Never treat `run_id` as a restore point. It identifies diagnostic run/report
  evidence only.
- Do not import or execute user declaration scripts merely to inspect or repair
  runtime-store state. Destination scopes are passed with explicit flags.
- Do not print secrets or raw customer payloads. CLI output should stay within
  the same redaction boundary as reports and inspection artifacts.

## Resolve The Operation Context First

Troubleshooting must use the runtime store that produced the run, progress, or
report evidence. If the user does not provide the backend flags, locate them
from the code path they ran before issuing `retl operations` commands.

Source count is not runtime-store count. A run may read many Sources while
writing one runtime authority store, or a project may have separate runners with
separate runtime stores. For CLI operations, find the runtime store for the
specific runner/run/sync being investigated, not every source database.

Look for:

- runner construction: `retl.runner(...)`, `Runner(...)`, or local helper
  functions that create a runner
- runtime store construction: `runtime_store=`, `DuckDBRuntimeStore`,
  backend `.runtime_store()`, or a backend-specific runtime-store class
- backend construction: `DuckDBSqlBackend`, `SnowflakeSqlBackend`, future
  backend classes, and config namespaces
- local constants and config: `DEFAULT_*PATH`, `database=`, `schema=`,
  `namespace=`, `runtime_database`, `runtime_schema`, environment files, and
  local README examples

Prefer values passed to the runner's runtime store over source-only settings.
For SQL backends, distinguish source relation-space settings from runtime
relation-space settings.

Build one reusable operation context and use it for every CLI command in the
same investigation:

```bash
OP_FLAGS=(--backend BACKEND BACKEND_SPECIFIC_RUNTIME_FLAGS --pretty)
```

Then sanity-check that the operation context matches known evidence before
trusting deeper inspection:

```bash
uv run retl operations inspect-run \
  --run-id RUN_ID \
  "${OP_FLAGS[@]}"
```

If a known `run_id` is not found, do not infer that the run is absent until you
have checked whether another runtime store, schema, namespace, or backend was
used.

Backend-specific flag examples:

DuckDB runtime store:

```bash
OP_FLAGS=(
  --backend duckdb \
  --database PATH_TO_RUNTIME.duckdb \
  --schema retl \
  --pretty
)
```

Snowflake runtime store:

```bash
OP_FLAGS=(
  --backend snowflake \
  --namespace backends.snowflake \
  --auth-mode password \
  --credential-namespace backends.snowflake.password \
  --pretty
)
```

Use the user's actual database, schema, namespace, auth mode, and credential
namespace when they provide them.

## Read-Only Triage

Start broad, then narrow:

1. Runtime inventory:

   ```bash
   uv run retl operations inspect-runtime "${OP_FLAGS[@]}"
   ```

2. Declaration shape:

   ```bash
   uv run retl operations inspect-declaration \
     --declaration-name customer_state \
     "${OP_FLAGS[@]}"
   ```

3. Destination scope:

   ```bash
   uv run retl operations inspect-destination-scope \
     --sync-name customer_sync \
     --destination-name crm \
     --surface user_profile \
     --family state \
     --declaration-name customer_state \
     "${OP_FLAGS[@]}"
   ```

4. Collect sequence:

   ```bash
   uv run retl operations inspect-collect-sequence \
     --declaration-name customer_state \
     --collect-sequence 1 \
     "${OP_FLAGS[@]}"
   ```

5. Target Registry:

   ```bash
   uv run retl operations inspect-target-registry \
     --destination-name crm \
     "${OP_FLAGS[@]}"
   ```

6. Diagnostic run evidence:

   ```bash
   uv run retl operations inspect-run \
     --run-id run_123 \
     "${OP_FLAGS[@]}"
   ```

## Mutation Choices

Choose the smallest operation that matches the evidence.

- `dismiss-unresolved`: mark pending or failed destination batches in one
  destination scope as skipped without deleting evidence.
- `skip-ordered-work-range`: record skipped coverage for an ordered-work range.
- `reset-destination-scope`: delete progress and batch authority for one Sync
  destination scope.
- `delete-collect-sequence`: delete a collect sequence only after dependent
  destination scopes are handled; use `--force` only with a clear reason.
- `delete-ordered-work-range`: delete a state/event ordered-work range only
  after dependent destination scopes are handled; use `--force` only with a
  clear reason.
- `rebaseline-state`: reset State baseline authority for one declaration/source
  pair; use when the user intends to accept current source state as the new
  baseline.
- `reset-target-registry`: clear target registry rows; this is isolated from
  ordered work, progress, batches, runs, and reports.
- `delete-run-evidence` and `delete-report-evidence`: remove diagnostic
  evidence only; these are not rollback or restore commands.
- `reset-runtime-store`: nuclear local authority reset. Use only when the user
  intends to discard the runtime store's RETL authority tables.

For destination-scoped mutation commands, pass all explicit scope flags:

```bash
--sync-name customer_sync \
--destination-name crm \
--surface user_profile \
--family state \
--declaration-name customer_state
```

For ordered-work range commands, pass all explicit range flags:

```bash
--first-collect-sequence 1 \
--first-sequence-order 0 \
--last-collect-sequence 1 \
--last-sequence-order 99
```

## Verify After Mutations

After any mutation, run the narrowest matching inspection command and one
broader inspection command. For example, after `reset-destination-scope`, run
`inspect-destination-scope` for that scope and `inspect-runtime`. Report:

- the command executed
- the scope or filter used
- the rows/counts/statuses that changed
- any refusal or retention-limit message
- the next safe command, if further repair is needed
