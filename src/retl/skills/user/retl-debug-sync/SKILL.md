---
name: retl-debug-sync
description: Debug failed or partial RETL Sync runs using reports, logs, destination batch ledgers, and dry-run reproduction.
---

# Debug A RETL Sync

Use this skill when a RETL Sync failed, completed partially, produced surprising
counts, or needs a dry-run reproduction.

## Workflow

1. Identify the runner name, Sync name, destination name, surface, declaration
   family, declaration name, run id, and report reference when available.
2. Inspect bounded reports, logs, and destination batch ledger evidence.
3. Reproduce with `dry_run=True` when destination mutation is not required.
4. Isolate the phase: collect, stage, reconcile, sync, progress, or report
   persistence.
5. Fix declaration, config, destination binding, or data-shape issues with a
   deterministic test or fixture.

## Safety Rules

- Do not dump raw source rows, full payloads, request bodies, credentials, or
  unbounded partner responses into chat, logs, tests, or docs.
- Do not reset, delete, rebaseline, or skip runtime-store state without
  explicit operator intent.
- Prefer inspection before repair.

## Validation

After a fix, run an import test and a dry-run or focused project test that
proves the failed path is now understood.
