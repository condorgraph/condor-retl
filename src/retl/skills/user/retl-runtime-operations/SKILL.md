---
name: retl-runtime-operations
description: Inspect and repair RETL runtime-store state with retl operations commands in an end-user project.
---

# Runtime Operations

Use this skill when a user asks to inspect runtime-store state, repair a scoped
Sync, skip unresolved work, reset progress, rebaseline State, or clean bounded
diagnostic evidence.

## Workflow

1. Identify the runtime backend and store location.
   Use `retl-configure-backend` to map project config to the runtime store
   relation space.
2. Use `retl operations inspect-runtime` before mutating state.
3. For destination-scoped commands, collect `--sync-name`, `--destination-name`,
   `--surface`, `--family`, and `--declaration-name`.
4. For Event skip ranges, require explicit cursor and primary-key scalar kinds.
5. Use destructive commands only with explicit operator approval and the
   command's force flag when required.

## Safety Rules

- Runtime-store ledgers and progress are durable authority. Treat mutation
  helpers as operational repair, not normal Sync execution.
- A `run_id` is diagnostic evidence, not a restore boundary.
- Do not infer missing scope flags from user code when a CLI command requires
  explicit scope.

## Validation

Capture compact JSON command output and confirm the affected scope or evidence
row count matches the operator intent. Keep outputs redacted and bounded.
