---
title: Glossary
description: User-facing RETL terms.
---

| Term | Meaning |
| --- | --- |
| Source | SQL-backed set of rows read by a backend. |
| State | Desired current truth, such as profile attributes or audience membership. |
| Event | Occurred truth, such as a purchase or signup. |
| Sync | One State or Event declaration bound to one destination surface. |
| Destination Surface | Connector-owned API contract used by a Sync. |
| Runtime store | Durable state used to track phase progress, reconciliation, and destination outcomes. |
| Dry run | Planning mode that avoids irreversible destination writes and destination progress advancement. |
