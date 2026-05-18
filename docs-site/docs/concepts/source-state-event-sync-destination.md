---
title: Sources, State, Events, Syncs, and Destination Surfaces
description: The core RETL authoring model.
---

RETL separates source shaping from destination delivery. Your SQL owns row
shape, filtering, joins, aggregation, and dedupe. RETL declarations map those
already-shaped rows into destination-ready intent.

## Source

A Source is a SQL-backed set of rows. Snapshot sources describe current data.
Checkpointed sources describe new data windows for events.

## State

State is desired current truth, such as profile attributes, list membership, or
audience membership. A State declaration defines keys, identifiers, payload
fields, and optional target mapping.

## Event

Event is occurred truth, such as purchases, signups, or lifecycle events. An
Event declaration includes `occurred_at` so RETL can keep event delivery tied to
source-window ordering.

## Sync

A Sync binds one State or Event declaration to one destination surface. A runner
executes Syncs and produces phase results, diagnostics, and destination receipt
evidence.

## Destination Surface

A Destination Surface is a connector-owned API contract. Examples include Meta
Custom Audiences, Klaviyo profiles, and customer-match list membership in ad
platforms.
