from __future__ import annotations

from typing import Any, cast

from retl.runtime.reports import (
    CommitSummary,
    DestinationSummary,
    FailureSummary,
    ProgressSummary,
    ReconcileSummary,
    ReportReference,
    RunIndex,
    SyncReport,
    build_run_index,
)


def test_compact_sync_report_serializes_runtime_store_index_fields_only() -> None:
    report = _report()
    data = cast(dict[str, Any], report.to_dict())

    assert data["run_id"] == "run-1"
    assert data["attempt_id"] == "attempt-1"
    assert data["report_id"] == "run-1:attempt-1:sync"
    assert data["status"] == "succeeded"
    assert data["progress"]["scope"] == {
        "sync_name": "sync",
        "destination_name": "destination",
        "surface": "custom_audiences",
        "family": "state",
        "declaration_name": "customer_state",
    }
    assert data["progress"]["page_count"] == 2
    assert data["reconcile"]["operation_count"] == 4
    assert data["destination"]["destination_batch_ids"] == (
        "destination-batch:1",
        "destination-batch:2",
    )
    assert "data_plane" not in data
    assert "stage_pages" not in str(data)
    assert "reconcile_pages" not in str(data)
    assert "payload_json" not in str(data)
    assert "request_body" not in str(data)


def test_report_serializes_and_renders_bounded_partner_error_detail() -> None:
    detail = (
        '{"message":"Invalid parameter","error_data":{"blame_field_specs":'
        '[["custom_data","value"]]},"access_token":"abc123"}'
    )
    report = _report(
        status="failed",
        destination=DestinationSummary(
            binding_name="destination",
            surface="custom_audiences",
            submission_status="pre_acceptance_failure",
            request_batch_count=1,
            destination_batch_count=1,
            destination_batch_ids=("destination-batch:1",),
            attempted_count=1,
            pre_acceptance_failure_count=1,
            failure_category="schema",
            http_status=400,
            last_error_summary="Partner rejected access_token=abc123.",
            last_error_detail=detail,
        ),
    )

    data = cast(dict[str, Any], report.to_dict())
    destination = cast(dict[str, Any], data["destination"])

    assert destination["last_error_detail"] is not None
    assert "blame_field_specs" in str(destination["last_error_detail"])
    assert "custom_data" in str(destination["last_error_detail"])
    assert "access_token=[redacted]" in str(destination["last_error_detail"])
    assert "abc123" not in str(destination["last_error_detail"])
    assert "Partner Error Detail:" in report.to_text()


def test_run_index_stays_thin_and_points_to_sync_reports() -> None:
    report = _report()

    run_index = build_run_index(
        runner_name="runner",
        run_id="run-1",
        status="succeeded",
        dry_run=False,
        source_groups=(),
        declaration_stages=(),
        reports=(report,),
    )
    index_data = run_index.to_dict()
    index_syncs = cast(tuple[dict[str, Any], ...], index_data["syncs"])

    assert isinstance(run_index, RunIndex)
    assert index_syncs[0]["report_id"] == report.report_id
    assert index_syncs[0]["report_ref"]["ref"] == report.ref.ref
    assert index_syncs[0]["operation_count"] == 4
    assert "destination_batch_ids" not in str(index_data)


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
        declaration_name="customer_state",
        declaration_version_id="decl:1",
        declaration_kind="state",
        destination_binding_name="destination",
        surface="custom_audiences",
        dry_run=False,
        status=status,
        on_failure="continue_on_any",
        phases=(),
        reconcile=ReconcileSummary(operation_count=4, upsert_count=3, remove_count=1),
        destination=destination
        or DestinationSummary(
            binding_name="destination",
            surface="custom_audiences",
            delivery_outcome="succeeded",
            submission_status="confirmed",
            request_batch_count=2,
            destination_batch_count=2,
            destination_batch_ids=("destination-batch:1", "destination-batch:2"),
            attempted_count=4,
            confirmed_count=4,
        ),
        progress=ProgressSummary(
            scope={
                "sync_name": "sync",
                "destination_name": "destination",
                "surface": "custom_audiences",
                "family": "state",
                "declaration_name": "customer_state",
            },
            stage_mode="pending",
            page_count=2,
            staged_row_count=4,
            safe_to_advance_collect_id=True,
            before=None,
            after={
                "collect_id": "00000000-0004-7000-8000-000000000000",
                "family": "state",
                "mode": "ordered_work",
                "sequence_order": 0,
            },
            advanced=True,
            decision_allowed=True,
            decision_reason="destination ledger covered.",
        ),
        failures=FailureSummary(on_failure="continue_on_any", decision="allowed"),
        commit=CommitSummary(progress_advanced=True, reason="destination ledger covered."),
    )
