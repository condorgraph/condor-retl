from __future__ import annotations

from typing import Any, cast

from retl.runtime import executor
from retl.runtime.reports import (
    CommitSummary,
    DestinationSummary,
    FailureSummary,
    ProgressSummary,
    ReconcileSummary,
    ReportReference,
    SyncReport,
)


def test_sync_report_does_not_expose_legacy_rich_report_sections() -> None:
    report = SyncReport(
        ref=ReportReference(ref="sync-report:1", kind="sync_report"),
        report_id="run-1:attempt-1:sync",
        runner_name="runner",
        run_id="run-1",
        attempt_id="attempt-1",
        sync_name="sync",
        declaration_name="audience",
        declaration_version_id="decl:1",
        declaration_kind="state",
        destination_binding_name="destination",
        surface="custom_audiences",
        dry_run=True,
        status="succeeded",
        on_failure="continue_on_any",
        phases=(),
        reconcile=ReconcileSummary(operation_count=2, upsert_count=2),
        destination=DestinationSummary(
            binding_name="destination",
            surface="custom_audiences",
            submission_status="confirmed",
            request_batch_count=1,
            destination_batch_count=1,
            destination_batch_ids=("destination-batch:1",),
            attempted_count=2,
            confirmed_count=2,
        ),
        progress=ProgressSummary(
            scope={
                "sync_name": "sync",
                "destination_name": "destination",
                "surface": "custom_audiences",
                "family": "state",
                "declaration_name": "audience",
            },
            page_count=1,
            staged_row_count=2,
            advanced=False,
            decision_allowed=True,
        ),
        failures=FailureSummary(),
        commit=CommitSummary(progress_advanced=False, reason="dry run"),
    )

    data = cast(dict[str, Any], report.to_dict())

    assert "data_plane" not in data
    assert "stage_page_metadata" not in str(data)
    assert "reconcile_page_metadata" not in str(data)
    assert "samples" not in str(data)
    assert "payload_json" not in str(data)
    assert "request_body" not in str(data)


def test_executor_no_longer_exposes_rich_report_page_combiners() -> None:
    assert not hasattr(executor, "_PageExecution")
    assert not hasattr(executor, "_combine_staged_pages")
    assert not hasattr(executor, "_combine_reconciled_pages")
    assert not hasattr(executor, "_combine_synced_pages")
