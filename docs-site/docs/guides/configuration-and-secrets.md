---
title: Configuration and Secrets
description: Use namespaces and environment variables for destination configuration.
---

Destination bindings can read config and credential values from named
namespaces.

```python
destination = retl.destinations.load(
    "retl/meta",
    binding_name="meta_primary",
    credential_namespace="destinations.meta",
    config_namespace="destinations.meta",
)
```

The default environment resolver maps namespace segments to environment
variables with double underscores.

```sh
export DESTINATIONS__META__ACCESS_TOKEN="..."
export DESTINATIONS__META__AD_ACCOUNT_ID="act_..."
```

Prefer namespaced configuration over scattering credentials through sync code.
It keeps declarations portable across local runs, CI checks, and scheduled
operators.
