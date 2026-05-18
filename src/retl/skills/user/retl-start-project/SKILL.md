---
name: retl-start-project
description: Start a new RETL project with AI-selected structure, files, checks, and dry-run behavior based on the user's actual source and destination.
---

# Start A RETL Project

Use this skill when a user wants to set up RETL from scratch or add RETL to an
existing application. Inspect the repository first, then choose the smallest
structure that makes the work maintainable.

## Setup Awareness

Before creating files, inspect for existing RETL code:

- `sync.py`
- `retl.toml` or local TOML config files
- `sources/`, `states/`, `events/`, `destinations/`, or `syncs/`
- application packages that already own data or job orchestration
- existing tests, CI, and dependency files

If a RETL setup exists, preserve its structure unless the user asks to
reorganize it. If no setup exists, ask only for missing essentials that cannot
be inferred from the project: source backend, destination, State versus Event
intent, and whether this is a quick proof or a maintained project.

## Recommended Shapes

For one source, one declaration, and one destination, a single-file setup is
acceptable:

```text
sync.py
```

For multiple Syncs, multiple destinations, private destination code, team
review, or production operation, prefer an organized package:

```text
retl_project/
  sources.py
  states.py
  events.py
  destinations.py
  syncs.py
  run.py
tests/
  test_retl_imports.py
```

Adapt names to the user's existing package. Do not create a separate
`retl_project/` package if their application already has a natural module for
jobs or data activation.

## Workflow

1. Identify the source backend and runtime-store placement.
   Use `retl-configure-backend` for backend config namespaces, naming defaults,
   and Source/Runtime relation-space splits.
2. Identify destination package, binding name, surface, credentials namespace,
   public config namespace, and target behavior.
3. Before writing local credential or override files, create or update root
   `.gitignore` rules so secrets and developer-local values cannot be
   committed.
4. Create committed non-secret config and ignored local secret or override
   files.
5. Wire RETL config and secret resolvers in the generated entrypoint before
   constructing destination bindings.
6. Choose State for desired current facts or Event for occurred facts.
7. Write declarations through `import retl` root APIs.
8. Keep credentials and secret values out of source, tests, and docs.
9. Add a dry-run-first command or test before live destination mutation.
10. Add minimal tests that import the declarations and validate obvious naming
   or surface choices.
11. Leave a short README or existing project doc update only when it helps the
   user run or operate the setup.

## Config And Secrets

When the project needs config or credentials, use one committed non-secret TOML
file plus one ignored environment file:

```text
retl.toml
.env
.gitignore
```

Commit `retl.toml` and the root `.gitignore`. Update the root `.gitignore`
before writing secret environment placeholders:

```gitignore
.env
```

Use `retl.toml` for non-secret project config that benefits from structure, and
populate it with every selected backend and destination public config key the
generated code will read. Use `.env` for secret environment variable names by
default, and populate it with every required credential field the generated
bindings will read. Use placeholder values such as `REPLACE_ME` only when the
real value is not available; never invent working credentials. Do not create
extra TOML override files unless the user explicitly asks for that workflow.

Placeholder generation is part of the setup, not a follow-up. When the user
names a backend, destination, surface, or credential model, write the matching
placeholder keys before finishing:

- Backend public config uses `[backends.<backend>]` in `retl.toml`.
- Destination shared public config uses `[destinations.<destination_name>]` in
  `retl.toml`.
- Destination surface public config uses
  `[destinations.<destination_name>.<surface>]` in `retl.toml` only when the
  generated binding or declaration reads those surface fields.
- Destination shared credentials use
  `export DESTINATIONS__<DESTINATION_NAME>__<FIELD>=REPLACE_ME` in `.env`.
- Destination surface credentials use
  `export DESTINATIONS__<DESTINATION_NAME>__<SURFACE>__<FIELD>=REPLACE_ME` in
  `.env` only when the selected connector exposes surface-scoped
  credentials.

Use uppercase environment names with `__` separators for each dotted namespace
segment. Do not leave `retl.toml` or `.env` as generic comments when the
selected backend or destination has known required fields.

RETL environment resolvers read process environment variables; they do not
automatically parse `.env` files. If the project already uses a dotenv loader or
shell wrapper, wire `.env` into that entrypoint. Otherwise, write `export`
statements in `.env` and document that operators must source `.env` before
running RETL.

Wire generated run entrypoints before declaring destinations:

```python
import retl

retl.configure(
    config_resolver=retl.ChainedConfigResolver(
        retl.TomlConfigResolver("retl.toml"),
        retl.EnvironmentConfigResolver(),
    ),
)
```

Then bind destinations with namespaces instead of embedding values:

```python
destination = retl.destinations.load(
    "<connector-ref>",
    binding_name="<destination_name>",
    credential_namespace="destinations.<destination_name>",
    config_namespace="destinations.<destination_name>",
)
```

Only include public config keys that the selected connector declares as
namespace-loadable. Do not add partner API fields, URLs, or commented
placeholders unless the selected connector documentation explicitly exposes
those fields.

## Boundaries

- Do not copy repository contributor skills from `.agents/skills/` into a user
  project.
- Use packaged user skills installed by `retl install-skills`.
- Do not invent partner API behavior. Use official destination docs or the
  installed connector's public documentation.

## Validation

Run the narrowest project command that proves the setup imports and can execute
without irreversible destination writes. Prefer dry-run runner execution or a
mocked destination test before live credentials are used.
