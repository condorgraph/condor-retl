---
name: retl-create-sync
description: Create or update a RETL Source, State or Event declaration, and Sync in an end-user RETL project.
---

# Create A RETL Sync

Use this skill when a user wants a new RETL Sync or wants to extend an existing
project with another Source, State, Event, or destination binding.

## Workflow

1. Inspect the project first. Prefer the existing application layout. If there
   is no RETL structure yet, follow the `retl-start-project` skill's setup
   guidance.
2. Use `retl-configure-backend` before creating a new backend or runtime store.
   Reuse the existing configured backend when one is already present.
3. Identify whether the work is current State or occurred Event work.
4. Keep source SQL replayable and bounded. Do not embed credentials or partner
   account secrets in Python files, TOML examples, tests, or generated docs.
5. Build the declaration through `import retl` root APIs.
6. Bind one declaration to one destination surface per Sync.
7. Add a dry-run-first entrypoint or test before suggesting live destination
   mutation.

## Authoring Rules

- Use `retl.source(..., mode="snapshot")` for State.
- Use `retl.source(..., mode="checkpointed", checkpoint={...})` for Event.
- Use `retl.state(...)` for desired current facts.
- Use `retl.event(...)` for occurred facts.
- Use `retl.sync(...)` for one declaration, one destination, and one surface.
- Use `runner.run(..., dry_run=True)` as the first executable path.

## Validation

Run the narrowest project checks that import the changed declaration and execute
the dry-run path. If the project has no checks yet, add a minimal import test
and a dry-run command that does not write to a live destination.
