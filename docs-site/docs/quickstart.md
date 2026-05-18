---
title: Install Skills Quickstart
sidebar_label: Install skills
description: Install RETL's user-facing AI skills into a project and use them to start a sync.
---

The fastest path is to install RETL's AI skills into the repository where the
sync will live, then ask your agent to use those skills. The skills are product
assets shipped with `condor-retl`; they are not copied from this repository's
contributor skill tree.

## Install RETL and skills

```sh
pip install "condor-retl[duckdb]"
retl install-skills . --pretty
```

By default, skills are installed into `.agents/skills/` and `.claude/skills/`
in the target project. Unchanged files are left alone, changed skill files are
refreshed from the packaged copy, and unrelated project files are preserved.

## What gets installed

The installed skills cover the normal user lifecycle:

| Skill | Use it for |
| --- | --- |
| `retl-start-project` | Starting RETL in a new or existing project |
| `retl-configure-backend` | Configuring SQL backend and runtime-store layout |
| `retl-create-sync` | Creating Source, State or Event declarations, and Syncs |
| `retl-use-destinations` | Loading destination connectors, surfaces, credentials, and targets |
| `retl-create-local-destination` | Creating private project-local destination behavior |
| `retl-debug-sync` | Debugging failed, partial, or surprising sync runs |
| `retl-runtime-operations` | Inspecting and repairing runtime-store state |
| `retl-organize-project` | Moving a growing RETL setup into a maintainable layout |

## Ask your agent to start

After installing the skills, prompt your agent with the real project context:

```sh
Use the retl-start-project skill in this repository.

Source: DuckDB at warehouse.duckdb, source schema main.
Destination: Meta Custom Audiences.
Goal: sync newsletter customers with email identifiers.
Start in dry-run mode and add the smallest checks that prove the setup works.
```

The agent should inspect the repository first, choose the smallest maintainable
layout, configure the backend and destination, then create dry-run-first code
for your actual source and destination.

## Review the generated sync

The generated code should still be understandable. For a DuckDB-to-destination
State sync, expect the same pieces you would write by hand:

```python
import retl
from retl.backends.duckdb import DuckDBSqlBackend


db = DuckDBSqlBackend(
    database="warehouse.duckdb",
    source_schema="main",
    runtime_schema="retl",
)

audience = retl.state(
    name="newsletter_audience",
    source=retl.source(
        name="newsletter_customers",
        mode="snapshot",
        backend=db.source_backend(),
        query="""
        select customer_id, email
        from customers
        where email is not null
        """,
    ),
    key={"customer_id": "customer_id"},
    target=retl.target("newsletter_customers"),
    identifiers=[{"type": "email", "value": "email"}],
)

meta = retl.destinations.load(
    "retl/meta",
    binding_name="meta_primary",
    credential_namespace="destinations.meta",
    config_namespace="destinations.meta",
)

result = retl.runner(
    name="send_newsletter_audience",
    runtime_store=db.runtime_store(),
).run(
    retl.sync(
        name="newsletter_audience_to_meta",
        declaration=audience,
        destination=meta,
        surface="custom_audiences",
    ),
    dry_run=True,
)

print(result.to_text())
```

## Next steps

- Review the full [first sync guide](./guides/first-sync.md).
- Install destination packages from [connector package refs](./reference/connector-packages.md).
- Learn the [core concepts](./concepts/source-state-event-sync-destination.md).
- Inspect [connector package refs](./reference/connector-packages.md).
