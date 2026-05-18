---
name: retl-use-destinations
description: Configure RETL destination bindings, surfaces, credentials, targets, and dry-run checks in an end-user project.
---

# Use RETL Destinations

Use this skill when a user needs to load a destination connector, select a
surface, configure auth and public config, or route State targets.

## Workflow

1. Inspect installed destination packages and existing destination bindings.
2. Use `retl-configure-backend` to identify the configured runtime store and
   durable target registry before creating destination-bound Syncs.
3. If the requested connector is unavailable, identify the matching connector
   distribution from project docs or the connector README, add it with the
   project's package manager, and retry destination discovery before authoring
   the binding.
4. Before writing local credential or override files, create or update root
   `.gitignore` rules so secrets and developer-local values cannot be
   committed.
5. Create committed non-secret config and ignored local files for required
   secrets or overrides.
6. Wire RETL config and secret resolvers in the run entrypoint before
   constructing destination bindings.
7. Load destinations with `retl.destinations.load(...)`.
8. Keep public config and secret references separate.
9. Prefer `credential_namespace=...` for required credentials.
10. Use explicit `config_namespace=...` only for public connector config fields.
11. For State targets, confirm whether the selected surface is targetless,
   static-target, source-column-target, or connector-managed target work.
12. Do not add partner API fields, URLs, or commented placeholders unless the
   selected connector explicitly declares them as namespace-loadable config.

## Config And Secrets

Prefer one committed non-secret TOML file plus one ignored environment file when
destination setup needs values:

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

Use `retl.toml` for every public non-secret config path the generated binding,
target routing, or declaration code will read, such as:

```toml
[destinations.<destination_name>]
required_public_field = "REPLACE_ME"
optional_public_field = "REPLACE_ME"

[destinations.<destination_name>.<surface>]
required_surface_public_field = "REPLACE_ME"
```

Only include public config keys that the selected connector declares as
namespace-loadable, plus explicit surface routing fields only when the generated
code reads them from config. Do not add `base_url` or any partner API endpoint
field unless the selected connector documentation explicitly exposes it.

Use `.env` for every required secret environment variable name. Follow RETL's
namespace mapping exactly: each dotted namespace segment becomes uppercase and
is separated with `__`. Write exported values so sourcing the file makes them
available to RETL commands.

```sh
export DESTINATIONS__<DESTINATION_NAME>__<FIELD>=REPLACE_ME
export DESTINATIONS__<DESTINATION_NAME>__<SURFACE>__<FIELD>=REPLACE_ME
```

Placeholder generation is required when destination setup needs local values.
Do not leave `retl.toml` or `.env` with only generic comments when the selected
connector has known required public config or credential fields. Discover those
fields from the installed connector metadata, connector README, or project-owned
connector docs before authoring the binding.

RETL environment resolvers read process environment variables; they do not
automatically parse `.env` files. If the project already uses a dotenv loader or
shell wrapper, wire `.env` into that entrypoint. Otherwise, write `export`
statements in `.env` and document that operators must source `.env` before
running RETL.

Wire the generated run entrypoint to read committed TOML public config first and
environment public config second:

```python
import retl

retl.configure(
    config_resolver=retl.ChainedConfigResolver(
        retl.TomlConfigResolver("retl.toml"),
        retl.EnvironmentConfigResolver(),
    ),
)
```

Environment secrets work without explicit configuration.

Destination bindings should then use namespaces:

```python
destination = retl.destinations.load(
    "<connector-ref>",
    binding_name="<destination_name>",
    credential_namespace="destinations.<destination_name>",
    config_namespace="destinations.<destination_name>",
)
```

## Safety Rules

- Do not put API keys, tokens, private keys, passwords, cookies, or OAuth
  material into source files, generated files, tests, reports, or examples.
- Do not write actual local credential, secret, or developer-override files
  until the matching root `.gitignore` rule already exists.
- Do not assume a destination write succeeded from request submission alone.
  Use RETL destination outcomes and receipts.
- Do not change target routing without a dry-run or explicit operator approval.

## Validation

Use dry-run runner execution first. Inspect destination binding errors, surface
compatibility errors, target resolution evidence, and Sync reports before live
runs.
