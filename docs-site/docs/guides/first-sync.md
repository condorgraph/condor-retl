---
title: Create a First Sync
description: Compose a source, declaration, destination binding, Sync, and runner.
---

The recommended authoring path is to install the RETL skills and ask your agent
to use `retl-start-project` or `retl-create-sync` in your project:

```sh
retl install-skills .
```

Those skills should produce code with the same shape shown below. Use this page
to understand and review the generated sync.

The normal sync shape is:

1. Create a source backend.
2. Declare a Source query.
3. Map rows into State or Event intent.
4. Load a destination connector.
5. Bind the declaration and destination into a Sync.
6. Run with `dry_run=True` before writing to the destination.

## Source backend

```python
from retl.backends.duckdb import DuckDBSqlBackend

db = DuckDBSqlBackend(
    database="warehouse.duckdb",
    source_schema="main",
    runtime_schema="retl",
)
```

## Source and State

```python
import retl

customers = retl.source(
    name="active_customers",
    mode="snapshot",
    backend=db.source_backend(),
    query="""
    select customer_id, email, plan
    from customers
    where email is not null
    """,
)

customer_state = retl.state(
    name="customer_profiles",
    source=customers,
    key={"customer_id": "customer_id"},
    identifiers=[{"type": "email", "value": "email"}],
    payload={"plan": "plan"},
)
```

## Destination and Sync

```python
destination = retl.destinations.load(
    "retl/klaviyo",
    binding_name="klaviyo_primary",
    credential_namespace="destinations.klaviyo",
    config_namespace="destinations.klaviyo",
)

sync = retl.sync(
    name="customer_profiles_to_klaviyo",
    declaration=customer_state,
    destination=destination,
    surface="profiles",
)
```

## Runner

```python
result = retl.runner(
    name="customer_profile_run",
    runtime_store=db.runtime_store(),
).run(sync, dry_run=True)

print(result.to_text())
```

When the dry-run output matches your intent and destination configuration is
ready, run the same Sync with `dry_run=False`.
