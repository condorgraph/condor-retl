# RETL File Destination

`condor-retl-file` is a first-party destination connector that writes RETL
State Operations and Event imports to local CSV export drops. It exposes the
`retl/file` connector through the `retl.destinations` entry point group.

## Surfaces

- `csv_state_operations` accepts State Operations, supports `upsert` and
  `remove`, and writes `upserts.csv` and `removes.csv` when those operation
  kinds are present.
- `csv_event_imports` accepts Event imports and writes `imports.csv`.

Both surfaces accept RETL's common identifier types without requiring a specific
identifier, because the connector writes identifier values through to the CSV
rather than translating them for a partner API.

Each non-dry-run submission creates a new timestamped export directory under
`output_dir` and writes a `manifest.json` with file names, row counts, byte
sizes, SHA-256 checksums, RETL batch ids, and operation counts. Successful file
writes produce definitive succeeded delivery evidence.

## Binding Config

The file connector does not use secrets or credentials.

- `output_dir` is required and points to the parent directory for export drops.
- `file_batch_max_rows` controls RETL file batch planning. It defaults to
  `10000`.
- `create_parent_dirs` controls whether missing output parents are created
  during non-dry-run submission. It defaults to `true`.

`output_dir`, `file_batch_max_rows`, and `create_parent_dirs` are
namespace-loadable public config:

```python
destination = retl.destinations.load(
    "retl/file",
    binding_name="file_exports",
    config_namespace="destinations.file",
)
```

## CSV Shape

CSV files use stable structured columns:

- `operation`
- `record_identity`
- `identifiers_json`
- `key_json`
- `payload_json`
- `target`
- `occurred_at`
- `collect_id`
- `sequence_order`
- `payload_fingerprint`

V1 writes new export drops for every submission. It does not mutate existing
CSV files and does not claim exactly-once file delivery; downstream consumers
should use manifest batch ids and checksums for deduplication.
