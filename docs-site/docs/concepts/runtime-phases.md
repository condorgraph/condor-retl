---
title: Runtime Phases
description: How collect, stage, reconcile, and sync fit together.
---

RETL runs declarations through four durable phases.

| Phase | Purpose |
| --- | --- |
| `collect` | Read source rows through the selected backend. |
| `stage` | Convert rows into canonical State or Event records. |
| `reconcile` | Compare staged intent with durable runtime state and produce work. |
| `sync` | Submit destination-facing work and record delivery outcomes. |

The phases are evidence boundaries. Failures should leave enough runtime state
to retry or inspect the run without guessing what happened.

Dry runs execute planning paths without irreversible destination writes or
destination progress advancement.
