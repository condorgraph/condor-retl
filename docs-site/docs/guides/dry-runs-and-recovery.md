---
title: Dry Runs and Recovery
description: Inspect work before writing and understand retry behavior.
---

Run a Sync with `dry_run=True` before sending work to a real destination.

```python
result = runner.run(sync, dry_run=True)
print(result.to_text())
```

Dry runs are useful for checking source row counts, target resolution, request
planning, and destination payload shape. They do not perform irreversible
destination writes or advance destination progress.

## Retry model

Runtime recovery is ledger-first. Later runs retry old `pending` batches and
retryable `failed` batches before scanning new work. Terminal outcomes such as
`succeeded`, `accepted`, and `skipped` are not retried automatically.

## Dismiss unresolved work

Use dismissal only when an operator intentionally accepts that unresolved
destination work should no longer block the Sync.

```python
runner.dismiss_unresolved(sync)
```

Keep dismissal scoped to the affected Sync destination scope.
