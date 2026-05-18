---
name: retl-create-local-destination
description: Create a project-local RETL destination connector for private end-user destination behavior.
---

# Create A Local Destination

Use this skill when a user needs destination behavior that is private to one
project and should not become a first-party `destination_connectors/` package.

## Workflow

1. Inspect the project layout. In organized RETL projects, prefer an existing
   destinations module or a package-local destination module.
2. Model the destination as a `DestinationConnector` with explicit surfaces.
3. For a partner API, bake the production origin into connector code; use
   configurable `base_url` only for generic/private HTTP endpoints.
4. Keep request planning, auth handling, and submission code bounded and
   testable.
5. Add fixture-driven tests for request planning and dry-run behavior.
6. Use injected transports in tests to capture `HttpRequest` values without
   changing partner URLs or making live HTTP calls.
7. Keep live credentials out of source files and examples.

## Boundaries

- Project-local destination code is application code, not a publishable
  first-party connector.
- First-party publishable connector packages belong outside user projects.
- Do not copy repository contributor skills from `.agents/skills/` into a user
  project. Use packaged user skills installed by `retl install-skills`.

## Validation

Prove request planning with deterministic fixtures. Prove non-mutating behavior
with dry-run execution before any live submission path is used.
