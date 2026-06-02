from __future__ import annotations

import csv
import hashlib
import json
import secrets
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.acknowledgements import (
    DestinationReceipt,
    DestinationSubmissionEvidence,
    RemoteHandle,
)
from retl.destinations.request_batch import (
    DestinationWorkRecord,
    DryRunSubmissionPlan,
    RequestBatchContext,
    RequestBatchingPolicy,
    RequestBatchPlan,
    plan_request_batches,
)
from retl.destinations.surfaces import DestinationSurface
from retl.errors import DeclarationValidationError
from retl.events.reconcile import EventReconcileEvidence
from retl.state_runtime.reconcile import StateReconcileEvidence
from retl_file.common import FileConfig, compact_json, file_config

CSV_COLUMNS = (
    "operation",
    "record_identity",
    "identifiers_json",
    "key_json",
    "payload_json",
    "target",
    "occurred_at",
    "collect_id",
    "sequence_order",
    "payload_fingerprint",
)

STATE_FILE_NAMES = {
    "upsert": "upserts.csv",
    "remove": "removes.csv",
}
EVENT_FILE_NAME = "imports.csv"


@dataclass(frozen=True)
class WrittenFile:
    name: str
    operation: str
    row_count: int
    byte_size: int
    sha256: str


def plan_file_requests(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
) -> DryRunSubmissionPlan:
    config = file_config(binding)
    work = getattr(reconciled, "operation_pages", None) or getattr(reconciled, "import_pages", None)
    if work is None:
        return DryRunSubmissionPlan(
            dry_run=True,
            plans=(),
            record_count=0,
            request_count=0,
            notes=("File destination work is deferred until reconcile produces pages.",),
        )
    return plan_request_batches(
        sync_name=reconciled.sync_name,
        surface_name=surface.name,
        work=work,
        request_template=surface.request_template,
        batching_policy=RequestBatchingPolicy(max_rows=config.file_batch_max_rows),
        dry_run=True,
        body_hook=_file_batch_body,
        family="state_operations" if surface.declaration_family == "state" else "event_imports",
    )


def submit_file_destination(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    delivery_outcome: str,
    attempted_count: int,
    dry_run: bool,
    resolved_auth: object,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    selected_request_plans: tuple[RequestBatchPlan, ...] | None = None,
) -> DestinationSubmissionEvidence:
    _ = (delivery_outcome, resolved_auth)
    try:
        config = file_config(binding)
        request_plans = _selected_request_plans(
            binding=binding,
            surface=surface,
            reconciled=reconciled,
            selected_request_plans=selected_request_plans,
        )
    except Exception as exc:
        return _pre_acceptance_failure(
            attempted_count=attempted_count,
            request_batch_count=0,
            message=f"File destination planning failed: {exc}",
        )

    if dry_run:
        return DestinationSubmissionEvidence.planned(
            attempted_count=attempted_count,
            dry_run=True,
            request_batch_count=len(request_plans),
            summary=(
                "File destination dry run planned "
                f"{len(request_plans)} file batch(es) for {attempted_count} record(s)."
            ),
        )

    try:
        export = _write_export(
            config=config,
            binding=binding,
            surface=surface,
            reconciled=reconciled,
            request_plans=request_plans,
        )
    except Exception as exc:
        return _pre_acceptance_failure(
            attempted_count=attempted_count,
            request_batch_count=len(request_plans),
            message=f"File destination write failed: {exc}",
        )

    handle = RemoteHandle(kind="file_export", value=str(export.export_dir))
    return DestinationSubmissionEvidence(
        status="confirmed",
        attempted_count=attempted_count,
        confirmed_count=attempted_count,
        request_batch_count=len(request_plans),
        receipts=(
            DestinationReceipt(status="confirmed", count=attempted_count, remote_handle=handle),
        ),
        remote_handles=(handle,),
        summary=(f"File destination wrote {attempted_count} record(s) to {export.export_dir}."),
    )


@dataclass(frozen=True)
class ExportResult:
    export_id: str
    export_dir: Path
    generated_at: str
    files: tuple[WrittenFile, ...]


def _selected_request_plans(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    selected_request_plans: tuple[RequestBatchPlan, ...] | None,
) -> tuple[RequestBatchPlan, ...]:
    planned = plan_file_requests(binding=binding, surface=surface, reconciled=reconciled).plans
    if selected_request_plans is None:
        return planned
    selected_ids = {plan.batch_id for plan in selected_request_plans}
    return tuple(plan for plan in planned if plan.batch_id in selected_ids)


def _file_batch_body(context: RequestBatchContext) -> JSONValue:
    return {
        "sync": context.sync_name,
        "surface": context.surface_name,
        "family": context.family,
        "operation": context.operation,
        "records": tuple(_record_payload(record) for record in context.records),
    }


def _record_payload(record: DestinationWorkRecord) -> JSONValue:
    data: dict[str, JSONValue] = {
        "operation": record.operation,
        "record_identity": record.record_identity,
        "identifiers": record.identifiers,
        "payload": record.payload,
        "key": record.key,
    }
    if record.target is not None:
        data["target"] = record.target
    if record.occurred_at is not None:
        data["occurred_at"] = record.occurred_at
    if record.collect_id is not None:
        data["collect_id"] = record.collect_id
    if record.sequence_order is not None:
        data["sequence_order"] = record.sequence_order
    if record.payload_fingerprint is not None:
        data["payload_fingerprint"] = record.payload_fingerprint
    return data


def _write_export(
    *,
    config: FileConfig,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    request_plans: tuple[RequestBatchPlan, ...],
) -> ExportResult:
    generated_at = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    export_id = f"{_filesystem_timestamp(generated_at)}_{secrets.token_hex(4)}"
    export_dir = config.output_dir / export_id
    _prepare_output_dir(config, export_dir)

    records = tuple(_records_from_plans(request_plans))
    files = _write_csv_files(export_dir=export_dir, surface=surface, records=records)
    manifest = _manifest(
        binding=binding,
        surface=surface,
        reconciled=reconciled,
        export_id=export_id,
        generated_at=generated_at,
        request_plans=request_plans,
        files=files,
    )
    _write_json(export_dir / "manifest.json", manifest)
    return ExportResult(
        export_id=export_id,
        export_dir=export_dir,
        generated_at=generated_at,
        files=files,
    )


def _prepare_output_dir(config: FileConfig, export_dir: Path) -> None:
    parent = config.output_dir
    if parent.exists() and not parent.is_dir():
        raise DeclarationValidationError(
            "File destination `output_dir` exists but is not a directory."
        )
    if not parent.exists():
        if not config.create_parent_dirs:
            raise DeclarationValidationError(
                "File destination `output_dir` does not exist and `create_parent_dirs` is false."
            )
        parent.mkdir(parents=True, exist_ok=True)
    export_dir.mkdir()


def _records_from_plans(plans: Iterable[RequestBatchPlan]) -> Iterable[Mapping[str, object]]:
    for plan in plans:
        body = plan.request.json_body
        if not isinstance(body, Mapping):
            raise TypeError("File destination request plan body must be a mapping.")
        records = body.get("records")
        if not isinstance(records, tuple | list):
            raise TypeError("File destination request plan body requires records.")
        for record in records:
            if not isinstance(record, Mapping):
                raise TypeError("File destination request records must be mappings.")
            yield record


def _write_csv_files(
    *,
    export_dir: Path,
    surface: DestinationSurface,
    records: tuple[Mapping[str, object], ...],
) -> tuple[WrittenFile, ...]:
    if surface.declaration_family == "event":
        if not records:
            return ()
        return (_write_csv(export_dir / EVENT_FILE_NAME, "import", records),)
    written: list[WrittenFile] = []
    for operation, file_name in STATE_FILE_NAMES.items():
        operation_records = tuple(
            record for record in records if record.get("operation") == operation
        )
        if operation_records:
            written.append(_write_csv(export_dir / file_name, operation, operation_records))
    return tuple(written)


def _write_csv(
    path: Path, operation: str, records: tuple[Mapping[str, object], ...]
) -> WrittenFile:
    tmp_path = path.with_name(f".{path.name}.tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for record in records:
            writer.writerow(_csv_row(record))
    tmp_path.replace(path)
    return _written_file(path, operation=operation, row_count=len(records))


def _write_json(path: Path, data: Mapping[str, object]) -> None:
    tmp_path = path.with_name(f".{path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
    tmp_path.replace(path)


def _csv_row(record: Mapping[str, object]) -> dict[str, str]:
    return {
        "operation": _string(record.get("operation")),
        "record_identity": _string(record.get("record_identity")),
        "identifiers_json": compact_json(record.get("identifiers", ())),
        "key_json": compact_json(record.get("key", {})),
        "payload_json": compact_json(record.get("payload", {})),
        "target": _string(record.get("target")),
        "occurred_at": _string(record.get("occurred_at")),
        "collect_id": _string(record.get("collect_id")),
        "sequence_order": _string(record.get("sequence_order")),
        "payload_fingerprint": _string(record.get("payload_fingerprint")),
    }


def _manifest(
    *,
    binding: DestinationBinding,
    surface: DestinationSurface,
    reconciled: StateReconcileEvidence | EventReconcileEvidence,
    export_id: str,
    generated_at: str,
    request_plans: tuple[RequestBatchPlan, ...],
    files: tuple[WrittenFile, ...],
) -> dict[str, object]:
    operation_counts = _operation_counts(request_plans)
    return {
        "connector_ref": binding.destination_ref,
        "surface": surface.name,
        "sync_name": reconciled.sync_name,
        "export_id": export_id,
        "generated_at": generated_at,
        "batch_ids": [plan.batch_id for plan in request_plans],
        "request_batch_count": len(request_plans),
        "operation_counts": operation_counts,
        "files": [
            {
                "name": file.name,
                "operation": file.operation,
                "row_count": file.row_count,
                "byte_size": file.byte_size,
                "sha256": file.sha256,
            }
            for file in files
        ],
    }


def _operation_counts(plans: tuple[RequestBatchPlan, ...]) -> dict[str, int]:
    counts = {"upsert": 0, "remove": 0, "import": 0}
    for record in _records_from_plans(plans):
        operation = record.get("operation")
        if operation in counts:
            counts[str(operation)] += 1
    return {key: value for key, value in counts.items() if value}


def _written_file(path: Path, *, operation: str, row_count: int) -> WrittenFile:
    payload = path.read_bytes()
    return WrittenFile(
        name=path.name,
        operation=operation,
        row_count=row_count,
        byte_size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def _filesystem_timestamp(generated_at: str) -> str:
    return generated_at.replace(":", "").replace("-", "").replace(".", "").removesuffix("Z") + "Z"


def _pre_acceptance_failure(
    *,
    attempted_count: int,
    request_batch_count: int,
    message: str,
) -> DestinationSubmissionEvidence:
    return DestinationSubmissionEvidence(
        status="pre_acceptance_failure",
        attempted_count=attempted_count,
        dry_run=False,
        pre_acceptance_failure_count=attempted_count,
        pre_acceptance_failure_category="transport",
        request_batch_count=request_batch_count,
        summary=message,
    )


def _string(value: object) -> str:
    if value is None:
        return ""
    return str(value)


__all__ = [
    "CSV_COLUMNS",
    "EVENT_FILE_NAME",
    "STATE_FILE_NAMES",
    "plan_file_requests",
    "submit_file_destination",
]
