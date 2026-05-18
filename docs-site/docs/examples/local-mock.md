---
title: Local Mock Destination
description: Use local destination surfaces for proof and examples.
---

Use local destination surfaces when you need deterministic examples, tests, or
authoring proof without a partner API.

```python
mock = retl.destinations.load(
    "retl/mock",
    binding_name="mock_primary",
)

sync = retl.sync(
    name="customers_to_mock",
    declaration=customer_state,
    destination=mock,
    surface="profiles",
)

result = runner.run(sync, dry_run=True)
print(result.to_text())
```

Local surfaces are proof tools. Install partner connector packages when an
operator needs to write to a real destination.
