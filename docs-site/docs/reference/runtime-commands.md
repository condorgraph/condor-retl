---
title: Runtime Commands
description: Common runtime entrypoints for running and operating Syncs.
---

| Command | Use |
| --- | --- |
| `runner.run(sync)` | Run one Sync. |
| `runner.run(sync, dry_run=True)` | Plan one Sync without irreversible destination writes or destination progress advancement. |
| `runner.run_many([sync_a, sync_b])` | Run an explicit Sync set that can share collect and stage work. |
| `runner.dismiss_unresolved(sync)` | Dismiss unresolved destination batches in one Sync destination scope. |

The Python API is the normal authoring surface. The CLI is operator-facing and
intended for inspection, recovery, and local operations where supported by the
installed package.
