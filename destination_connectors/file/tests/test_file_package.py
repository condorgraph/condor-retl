from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pytest
from retl_file.common import file_config
from retl_file.definitions import (
    EVENT_SURFACE,
    FILE_ACCEPTED_IDENTIFIER_TYPES,
    FILE_CONNECTOR_REF,
    STATE_SURFACE,
    file_connector,
)
from retl_file.hooks import plan_file_requests, submit_file_destination

import retl
from retl.declarations import DestinationBinding
from retl.errors import DeclarationValidationError
from retl.events.reconcile import EventReconcileEvidence
from retl.state_runtime.reconcile import StateReconcileEvidence


def test_file_connector_declares_state_event_surfaces_and_auth() -> None:
    connector = file_connector()

    assert connector.connector_ref == FILE_CONNECTOR_REF
    assert connector.auth_modes[0].name == "none"
    assert set(connector.surface_names) == {STATE_SURFACE, EVENT_SURFACE}
    assert connector.config_namespace_fields == (
        "output_dir",
        "file_batch_max_rows",
        "create_parent_dirs",
    )
    state_surface = connector.surface(STATE_SURFACE)
    event_surface = connector.surface(EVENT_SURFACE)
    assert state_surface.declaration_family == "state"
    assert state_surface.supported_operations == ("upsert", "remove")
    assert state_surface.accepted_identifier_types == FILE_ACCEPTED_IDENTIFIER_TYPES
    assert state_surface.delivery_outcome == "succeeded"
    assert event_surface.declaration_family == "event"
    assert event_surface.supported_operations == ("import",)
    assert event_surface.accepted_identifier_types == FILE_ACCEPTED_IDENTIFIER_TYPES
    assert event_surface.delivery_outcome == "succeeded"
    assert callable(connector.submission_hook)


def test_file_loads_namespace_config(tmp_path: Path) -> None:
    retl.configure(
        config_resolver=retl.MappingConfigResolver(
            {
                "destinations.file.output_dir": str(tmp_path),
                "destinations.file.file_batch_max_rows": "2",
                "destinations.file.create_parent_dirs": "true",
            }
        )
    )
    try:
        connector = file_connector()
        registry = retl.destinations.DestinationRegistry()
        registry.register(connector)

        destination = retl.destinations.load(
            FILE_CONNECTOR_REF,
            binding_name="file_exports",
            config_namespace="destinations.file",
            registry=registry,
        )

        config = file_config(destination)
        assert config.output_dir == tmp_path
        assert config.file_batch_max_rows == 2
        assert config.create_parent_dirs is True
    finally:
        retl.configure(config_resolver=None)


def test_file_config_requires_output_dir() -> None:
    connector = file_connector()
    binding = DestinationBinding(
        binding_name="file_exports",
        destination_ref=connector.connector_ref,
        connector=connector,
    )

    with pytest.raises(DeclarationValidationError, match="output_dir"):
        file_config(binding)


def test_file_dry_run_plans_batches_without_writing(tmp_path: Path) -> None:
    connector = file_connector()
    binding = _binding(tmp_path, file_batch_max_rows=1)
    reconciled = _state_reconciled(_state_page())

    plan = plan_file_requests(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        reconciled=reconciled,
    )
    evidence = submit_file_destination(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        delivery_outcome="succeeded",
        attempted_count=2,
        dry_run=True,
        resolved_auth=object(),
        reconciled=reconciled,
        selected_request_plans=plan.plans,
    )

    assert plan.request_count == 2
    assert [request.row_count for request in plan.plans] == [1, 1]
    assert evidence.status == "planned"
    assert evidence.request_batch_count == 2
    assert list(tmp_path.iterdir()) == []


def test_file_state_submission_writes_operation_csvs_and_manifest(tmp_path: Path) -> None:
    connector = file_connector()
    binding = _binding(tmp_path)
    reconciled = _state_reconciled(_state_page())

    evidence = submit_file_destination(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        delivery_outcome="succeeded",
        attempted_count=2,
        dry_run=False,
        resolved_auth=object(),
        reconciled=reconciled,
    )

    export_dir = _single_export_dir(tmp_path)
    upserts = _csv_rows(export_dir / "upserts.csv")
    removes = _csv_rows(export_dir / "removes.csv")
    manifest = _manifest(export_dir)

    assert evidence.status == "confirmed"
    assert evidence.confirmed_count == 2
    assert evidence.remote_handles[0].kind == "file_export"
    assert Path(evidence.remote_handles[0].value) == export_dir
    assert upserts[0]["operation"] == "upsert"
    assert upserts[0]["record_identity"] == "customer-1"
    assert json.loads(upserts[0]["payload_json"]) == {"status": "active"}
    assert removes[0]["operation"] == "remove"
    assert manifest["connector_ref"] == FILE_CONNECTOR_REF
    assert manifest["surface"] == STATE_SURFACE
    assert manifest["sync_name"] == "file_state_sync"
    assert manifest["operation_counts"] == {"remove": 1, "upsert": 1}
    files = cast(list[dict[str, object]], manifest["files"])
    assert {file["name"] for file in files} == {"upserts.csv", "removes.csv"}
    _assert_manifest_checksums(export_dir, manifest)


def test_file_event_submission_writes_imports_csv_and_manifest(tmp_path: Path) -> None:
    connector = file_connector()
    binding = _binding(tmp_path)
    reconciled = _event_reconciled(_event_page())

    evidence = submit_file_destination(
        binding=binding,
        surface=connector.surface(EVENT_SURFACE),
        delivery_outcome="succeeded",
        attempted_count=1,
        dry_run=False,
        resolved_auth=object(),
        reconciled=reconciled,
    )

    export_dir = _single_export_dir(tmp_path)
    imports = _csv_rows(export_dir / "imports.csv")
    manifest = _manifest(export_dir)

    assert evidence.status == "confirmed"
    assert imports[0]["operation"] == "import"
    assert imports[0]["occurred_at"] == "2026-06-02T12:34:56Z"
    assert json.loads(imports[0]["key_json"]) == {"event_id": "evt-1"}
    assert manifest["operation_counts"] == {"import": 1}
    files = cast(list[dict[str, object]], manifest["files"])
    assert files[0]["name"] == "imports.csv"
    _assert_manifest_checksums(export_dir, manifest)


def test_file_submission_fails_when_output_dir_parent_creation_disabled(tmp_path: Path) -> None:
    connector = file_connector()
    missing_dir = tmp_path / "missing"
    binding = _binding(missing_dir, create_parent_dirs=False)

    evidence = submit_file_destination(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        delivery_outcome="succeeded",
        attempted_count=2,
        dry_run=False,
        resolved_auth=object(),
        reconciled=_state_reconciled(_state_page()),
    )

    assert evidence.status == "pre_acceptance_failure"
    assert evidence.pre_acceptance_failure_category == "transport"
    assert evidence.confirmed_count == 0
    assert not missing_dir.exists()


def test_file_submission_fails_when_output_dir_is_file(tmp_path: Path) -> None:
    connector = file_connector()
    output_file = tmp_path / "exports"
    output_file.write_text("not a directory", encoding="utf-8")
    binding = _binding(output_file)

    evidence = submit_file_destination(
        binding=binding,
        surface=connector.surface(STATE_SURFACE),
        delivery_outcome="succeeded",
        attempted_count=2,
        dry_run=False,
        resolved_auth=object(),
        reconciled=_state_reconciled(_state_page()),
    )

    assert evidence.status == "pre_acceptance_failure"
    assert evidence.pre_acceptance_failure_category == "transport"
    assert evidence.confirmed_count == 0


def _binding(
    output_dir: Path,
    *,
    file_batch_max_rows: int = 1000,
    create_parent_dirs: bool = True,
) -> DestinationBinding:
    connector = file_connector()
    return DestinationBinding(
        binding_name="file_exports",
        destination_ref=connector.connector_ref,
        connector=connector,
        config={
            "output_dir": str(output_dir),
            "file_batch_max_rows": file_batch_max_rows,
            "create_parent_dirs": create_parent_dirs,
        },
    )


def _state_page() -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "upsert",
                "record_identity": "customer-1",
                "collect_id": "00000000-000b-7000-8000-000000000000",
                "sequence_order": 0,
                "state_key": {"customer_id": "1"},
                "identifiers": [{"type": "email", "value": "one@example.test"}],
                "payload": {"status": "active"},
                "payload_fingerprint": "fp-upsert",
            },
            {
                "operation": "remove",
                "record_identity": "customer-2",
                "collect_id": "00000000-000b-7000-8000-000000000000",
                "sequence_order": 1,
                "state_key": {"customer_id": "2"},
                "identifiers": [{"type": "email", "value": "two@example.test"}],
                "payload": {"status": "inactive"},
                "payload_fingerprint": "fp-remove",
            },
        ]
    ).to_batches()[0]


def _event_page() -> pa.RecordBatch:
    return pa.Table.from_pylist(
        [
            {
                "operation": "import",
                "record_identity": "event-1",
                "collect_id": "00000000-000c-7000-8000-000000000000",
                "sequence_order": 0,
                "event_key": {"event_id": "evt-1"},
                "identifiers": [{"type": "email", "value": "one@example.test"}],
                "payload": {"event_name": "purchase"},
                "occurred_at": "2026-06-02T12:34:56Z",
                "payload_fingerprint": "fp-event",
            },
        ]
    ).to_batches()[0]


def _state_reconciled(page: pa.RecordBatch) -> StateReconcileEvidence:
    return cast(
        StateReconcileEvidence,
        SimpleNamespace(sync_name="file_state_sync", operation_pages=(page,)),
    )


def _event_reconciled(page: pa.RecordBatch) -> EventReconcileEvidence:
    return cast(
        EventReconcileEvidence,
        SimpleNamespace(
            sync_name="file_event_sync",
            import_pages=(page,),
            event_cursor_kind="occurred_at",
            event_primary_key_kind="event_id",
        ),
    )


def _single_export_dir(output_dir: Path) -> Path:
    exports = list(output_dir.iterdir())
    assert len(exports) == 1
    assert exports[0].is_dir()
    return exports[0]


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _manifest(export_dir: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads((export_dir / "manifest.json").read_text()))


def _assert_manifest_checksums(export_dir: Path, manifest: dict[str, object]) -> None:
    files = manifest["files"]
    assert isinstance(files, list)
    for file in files:
        assert isinstance(file, dict)
        path = export_dir / str(file["name"])
        payload = path.read_bytes()
        assert file["byte_size"] == len(payload)
        assert file["sha256"] == hashlib.sha256(payload).hexdigest()
