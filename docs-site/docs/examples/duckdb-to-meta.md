---
title: DuckDB to Meta Audience
description: A compact State sync example for Meta Custom Audiences.
---

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

sync = retl.sync(
    name="newsletter_audience_to_meta",
    declaration=audience,
    destination=meta,
    surface="custom_audiences",
)

result = retl.runner(
    name="send_newsletter_audience",
    runtime_store=db.runtime_store(),
).run(sync, dry_run=True)

print(result.to_text())
```
