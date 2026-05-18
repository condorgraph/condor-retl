# Appendices

This page contains support context that is useful for implementation, but is
not the primary policy surface. Current behavior belongs in the compact root
docs:

- [Product](./product.md)
- [Runtime](./runtime.md)
- [Canonical Model](./canonical-model.md)
- [Destinations](./destinations.md)

## Taxonomy Summary

Active RETL vocabulary is organized around:

- **Source**: a reusable declaration for reading upstream rows.
- **Source Mode**: `snapshot` for State, `checkpointed` for Event.
- **State**: desired current facts keyed by logical fields.
- **Event**: occurred facts imported from checkpointed windows.
- **Sync**: one declaration bound to one destination and one Destination
  Surface.
- **Destination Surface**: connector-owned endpoint contract for State or Event
  work.
- **Target**: optional destination-facing routing key for State.
- **Progress**: destination-scoped scan cursor for one Sync, destination,
  surface, declaration family, and declaration name.
- **Destination Batch Ledger**: durable delivery, retry, and outcome records
  used to derive completion and retry summaries.

## Generated Material

Exact JSON field shapes, Arrow schemas, manifest schemas, package records,
public DTO definitions, SQL table catalogs, and generated API references are
code-owned or generation-owned material. When generated docs exist, they must
declare their source and generation command.

Narrative docs should state behavioral contracts and ownership boundaries, not
duplicate generated schema catalogs.
