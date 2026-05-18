from __future__ import annotations

from retl.destinations.receipts import sanitize_partner_error_detail
from retl.errors import DeclarationValidationError
from retl.stores.sql_runtime import json as json_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.writes import execute_runtime_insert


def record_sync_report(
    context: SqlRuntimeContext,
    sync_reports: list[object],
    report: object,
) -> None:
    sync_reports.append(report)
    persist_sync_report(context, report)


def persist_sync_report(context: SqlRuntimeContext, report: object) -> None:
    run_id = _string_attr(report, "run_id")
    attempt_id = _string_attr(report, "attempt_id")
    sync_name = _string_attr(report, "sync_name")
    status = _string_attr(report, "status")
    report_id = _string_attr(report, "report_id")
    if (
        run_id is None
        or attempt_id is None
        or sync_name is None
        or status is None
        or report_id is None
    ):
        raise DeclarationValidationError(
            "persisted Sync reports require run_id, attempt_id, sync_name, status, and report_id."
        )
    report_ref = getattr(report, "ref", None)
    destination = getattr(report, "destination", None)
    commit = getattr(report, "commit", None)
    submitted_count = _int_attr(destination, "attempted_count")
    succeeded_count = _int_attr(destination, "confirmed_count")
    accepted_count = _int_attr(destination, "accepted_count")
    retryable_count = _int_attr(destination, "retryable_failure_count")
    terminal_count = _int_attr(destination, "terminal_failure_count")
    pre_acceptance_count = _int_attr(destination, "pre_acceptance_failure_count")
    failed_count = retryable_count + terminal_count + pre_acceptance_count
    data = json_helpers.report_json(report, "sync_report")
    execute_runtime_insert(
        context,
        "sync_reports",
        (
            (
                "report_id",
                report_id,
            ),
            ("report_ref", _string_attr(report_ref, "ref")),
            ("run_id", run_id),
            ("attempt_id", attempt_id),
            ("runner_name", _string_attr(report, "runner_name")),
            ("sync_name", sync_name),
            ("declaration_name", _string_attr(report, "declaration_name")),
            ("declaration_version_id", _string_attr(report, "declaration_version_id")),
            ("declaration_kind", _string_attr(report, "declaration_kind")),
            ("destination_name", _string_attr(report, "destination_binding_name")),
            ("surface", _string_attr(report, "surface")),
            ("status", status),
            ("dry_run", bool(getattr(report, "dry_run", False))),
            ("submitted_record_count", submitted_count),
            ("succeeded_record_count", succeeded_count),
            ("accepted_record_count", accepted_count),
            ("failed_record_count", failed_count),
            ("retryable_failure_count", retryable_count),
            ("terminal_failure_count", terminal_count),
            ("pre_acceptance_failure_count", pre_acceptance_count),
            ("progress_advanced", bool(getattr(commit, "progress_advanced", False))),
            ("failure_category", _string_attr(destination, "failure_category")),
            ("http_status", getattr(destination, "http_status", None)),
            ("last_error_summary", _string_attr(destination, "last_error_summary")),
            (
                "last_error_detail",
                sanitize_partner_error_detail(_string_attr(destination, "last_error_detail")),
            ),
            ("report_json", data),
        ),
    )


def _string_attr(value: object, name: str) -> str | None:
    attr = getattr(value, name, None)
    if attr is None:
        return None
    return str(attr)


def _int_attr(value: object, name: str) -> int:
    return int(getattr(value, name, 0) or 0)


__all__ = [
    "persist_sync_report",
    "record_sync_report",
]
