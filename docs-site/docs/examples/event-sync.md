---
title: Event Sync
description: Declare occurred facts from a checkpointed source.
---

Use Event declarations for facts that happened at a point in time.

```python
import retl

purchases = retl.source(
    name="purchase_events",
    mode="checkpointed",
    backend=db.source_backend(),
    query="""
    select purchase_id, email, purchased_at, order_total, currency
    from purchases
    where purchased_at >= :window_start
      and purchased_at < :window_end
    """,
)

purchase_events = retl.event(
    name="purchase",
    source=purchases,
    key={"purchase_id": "purchase_id"},
    occurred_at="purchased_at",
    identifiers=[{"type": "email", "value": "email"}],
    payload={
        "order_total": "order_total",
        "currency": "currency",
    },
)
```

Bind the Event declaration to a destination surface the same way you bind
State.
