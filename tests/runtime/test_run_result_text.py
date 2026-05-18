from __future__ import annotations

from retl.runtime.reports import (
    CommitSummary,
    DestinationSummary,
    FailureSummary,
    ProgressSummary,
    ReconcileSummary,
    ReportReference,
    RunIndex,
    SyncReport,
)
from retl.runtime.results import PhaseEvidence, PhaseStatus, RunResult, SyncResult


def _phase(name: str) -> PhaseStatus:
    return PhaseStatus(
        name=name,  # type: ignore[arg-type]
        status="succeeded",
        evidence=PhaseEvidence(kind="planned", message=f"{name} complete", dry_run=True),
    )


def test_run_result_text_combines_run_index_and_compact_sync_report() -> None:
    result = RunResult(
        runner_name="runner",
        status="succeeded",
        dry_run=True,
        source_groups=(),
        declaration_stages=(),
        syncs=(
            SyncResult(
                sync_name="sync",
                declaration_name="audience",
                declaration_kind="state",
                destination_binding_name="destination",
                surface="custom_audiences",
                dry_run=True,
                attempt_id=None,
                report_reference="sync-report:1",
                collect=_phase("collect"),
                stage=_phase("stage"),
                reconcile=_phase("reconcile"),
                sync=_phase("sync"),
                operation_count=3,
                upsert_count=2,
                remove_count=1,
                destination_batch_count=1,
                destination_submission_status="confirmed",
                progress_advanced=True,
            ),
        ),
        run_index=RunIndex(
            ref=ReportReference(ref="run-index:1", kind="run_index"),
            runner_name="runner",
            run_id="run-1",
            status="succeeded",
            dry_run=True,
            source_groups=(),
            declaration_stages=(),
            syncs=(),
        ),
        sync_reports=(_report(),),
    )

    summary = result.to_text()

    assert "Run Index: run-1" in summary
    assert "Sync Results:\nsync=sync status=confirmed" in summary
    assert "Sync Report: sync" in summary
    assert "Report ID: run-1:attempt-1:sync" in summary
    assert "Progress: scope=sync_name=sync,destination_name=destination" in summary
    assert "Destination Batch IDs: destination-batch:1" in summary
    assert "Submission Attempts" not in summary


def test_run_result_text_falls_back_to_compact_status_when_reports_are_absent() -> None:
    result = RunResult(
        runner_name="runner",
        status="planned",
        dry_run=True,
        source_groups=(),
        declaration_stages=(),
        syncs=(),
    )

    assert result.to_text() == "Run Result: runner\nStatus: planned\nDry Run: True"


def test_run_result_text_shows_redacted_compact_failure_detail() -> None:
    result = RunResult(
        runner_name="runner",
        status="failed",
        dry_run=False,
        source_groups=(),
        declaration_stages=(),
        syncs=(
            SyncResult(
                sync_name="sync",
                declaration_name="audience",
                declaration_kind="state",
                destination_binding_name="destination",
                surface="custom_audiences",
                dry_run=False,
                attempt_id=None,
                report_reference="sync-report:1",
                collect=_phase("collect"),
                stage=_phase("stage"),
                reconcile=_phase("reconcile"),
                sync=_phase("sync"),
                operation_count=1000,
                upsert_count=1000,
                destination_batch_count=1,
                destination_submission_status="pre_acceptance_failure",
                destination_pre_acceptance_failure_count=1000,
                destination_pre_acceptance_failure_category="auth",
            ),
        ),
        sync_reports=(
            _report(
                status="failed",
                destination=DestinationSummary(
                    binding_name="destination",
                    surface="custom_audiences",
                    submission_status="pre_acceptance_failure",
                    request_batch_count=1,
                    destination_batch_count=1,
                    destination_batch_ids=("destination-batch:1",),
                    attempted_count=1000,
                    pre_acceptance_failure_count=1000,
                    failure_category="auth",
                    http_status=400,
                    last_error_summary=(
                        "Meta rejected the request authorization=Bearer abc123 token=secret."
                    ),
                    last_error_detail=(
                        '{"error_data":{"blame_field_specs":[["custom_data","value"]]},'
                        '"client_secret":"json-secret"}'
                    ),
                ),
            ),
        ),
    )

    summary = result.to_text()

    assert "sync=sync status=failed" in summary
    assert "blocking_rows=1000" in summary
    assert "Partner Error Detail:" in summary
    assert "blame_field_specs" in summary
    assert "client_secret=[redacted]" in summary
    assert "Bearer abc123" not in summary
    assert "json-secret" not in summary


def _report(
    *,
    status: str = "succeeded",
    destination: DestinationSummary | None = None,
) -> SyncReport:
    return SyncReport(
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
        dry_run=False,
        status=status,
        on_failure="continue_on_any",
        phases=(),
        reconcile=ReconcileSummary(operation_count=3, upsert_count=2, remove_count=1),
        destination=destination
        or DestinationSummary(
            binding_name="destination",
            surface="custom_audiences",
            delivery_outcome="succeeded",
            submission_status="confirmed",
            request_batch_count=1,
            destination_batch_count=1,
            destination_batch_ids=("destination-batch:1",),
            confirmed_count=3,
        ),
        progress=ProgressSummary(
            scope={
                "sync_name": "sync",
                "destination_name": "destination",
                "surface": "custom_audiences",
                "family": "state",
                "declaration_name": "audience",
            },
            stage_mode="pending",
            page_count=1,
            staged_row_count=3,
            advanced=True,
            decision_allowed=True,
        ),
        failures=FailureSummary(),
        commit=CommitSummary(progress_advanced=True, reason="destination ledger covered."),
    )
