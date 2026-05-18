---
title: Use Destinations
description: Install connector packages and load destination refs.
---

Destination connectors expose refs through the `retl.destinations` entry point
group.

```python
meta = retl.destinations.load(
    "retl/meta",
    binding_name="meta_primary",
    credential_namespace="destinations.meta",
    config_namespace="destinations.meta",
)
```

A destination ref identifies the connector. A surface identifies the API
contract inside that connector.

```python
sync = retl.sync(
    name="newsletter_to_meta",
    declaration=audience,
    destination=meta,
    surface="custom_audiences",
)
```

For local proof and examples, RETL includes `retl/mock` and `retl/reference`.
Partner connectors are installed separately.

See [connector packages](../reference/connector-packages.md) for package names,
refs, and surfaces.
