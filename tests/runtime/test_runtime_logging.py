from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb
import pytest

import retl
from retl.backends.duckdb import DuckDBRuntimeStore, DuckDBSqlBackend
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.destinations.targets import RemoteTarget, TargetMapping
from retl.runtime import executor
from retl.sync_runtime import submission as submission_module


def test_runner_emits_bounded_runtime_boundary_logs(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    sync = _sync(_state(backend), on_failure="stop_on_any")

    caplog.set_level(logging.INFO, logger="retl")

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    records = [record for record in caplog.records if record.name == "retl.runtime.executor"]
    submission_records = [
        record for record in caplog.records if record.name == "retl.sync_runtime.submission"
    ]
    progress_records = [
        record for record in caplog.records if record.name == "retl.runtime.progress"
    ]
    events = [getattr(record, "event", None) for record in records]
    assert "run_started" in events
    assert "run_completed" in events
    assert "sync_started" in events
    assert "sync_completed" in events
    assert "destination_compatibility_checked" in events
    assert "sync_report_recorded" in events
    assert _phase_events(records, "collect") == {"phase_started", "phase_completed"}
    assert _phase_events(records, "stage") == {"phase_started", "phase_completed"}
    assert _phase_events(records, "reconcile") == {"phase_started", "phase_completed"}
    assert _phase_events(records, "sync") == {"phase_started", "phase_completed"}

    run_completed = _record(records, "run_completed")
    assert _field(run_completed, "run_id") == result.run_id
    assert _field(run_completed, "runner_name") == "crm_to_lifecycle"
    assert _field(run_completed, "status") == "succeeded"
    assert _field(run_completed, "dry_run") is False
    assert _field(run_completed, "report_references") == result.report_references
    assert _field(run_completed, "run_index_reference") == result.run_index_reference

    sync_completed = _record(records, "sync_completed")
    assert _field(sync_completed, "run_id") == result.run_id
    assert _field(sync_completed, "sync_name") == "customer_profiles"
    assert _field(sync_completed, "declaration_name") == "customer_state"
    assert _field(sync_completed, "declaration_kind") == "state"
    assert _field(sync_completed, "destination_binding_name") == "mock_profiles"
    assert _field(sync_completed, "surface") == "profile_properties"
    assert _field(sync_completed, "destination_batch_count") == 1
    assert _field(sync_completed, "confirmed_count") == 2
    assert _field(sync_completed, "report_reference") == result.syncs[0].report_reference

    destination_completed = _record(submission_records, "destination_submission_completed")
    assert _field(destination_completed, "run_id") == result.run_id
    assert _field(destination_completed, "attempt_id") == result.syncs[0].attempt_id
    assert _field(destination_completed, "sync_name") == "customer_profiles"
    assert _field(destination_completed, "surface") == "profile_properties"
    assert _field(destination_completed, "status") == "confirmed"
    assert _field(destination_completed, "attempted_count") == 2
    assert _field(destination_completed, "confirmed_count") == 2
    assert _field(destination_completed, "request_batch_count") == 1
    assert _field(destination_completed, "progress_decision_allowed") is True

    assert _field(_record(submission_records, "destination_batches_planned"), "page_index") == 1
    assert (
        _field(_record(submission_records, "destination_batches_recorded"), "upserted_batch_count")
        == 1
    )
    batch_attempt = _record(submission_records, "destination_batch_attempt_completed")
    assert _field(batch_attempt, "destination_batch_index") == 0
    assert _field(batch_attempt, "row_count") == 2
    assert _field(batch_attempt, "run_action") == "attempted"
    assert _field(batch_attempt, "progress_implication") == "resolved_for_progress"

    progress_decision = _record(progress_records, "progress_commit_decided")
    assert _field(progress_decision, "run_id") == result.run_id
    assert _field(progress_decision, "attempt_id") == result.syncs[0].attempt_id
    assert _field(progress_decision, "progress_decision_allowed") is True
    assert _field(progress_decision, "progress_advanced") is True

    log_payload = _log_payload(
        [record for record in caplog.records if record.name.startswith("retl.")]
    )
    assert "one@example.com" not in log_payload
    assert "two@example.com" not in log_payload
    assert "cust_1" not in log_payload
    assert "cust_2" not in log_payload
    assert "tier_alpha_raw" not in log_payload
    assert "tier_beta_raw" not in log_payload


def test_target_resolution_log_records_counts_without_raw_targets(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    sync = _sync(
        _targeted_state(backend),
        surface="list_membership",
        target_mappings=(
            TargetMapping(
                logical_target="sensitive_audience_raw",
                remote=RemoteTarget(remote_id="remote_audience_raw"),
            ),
        ),
    )

    caplog.set_level(logging.INFO, logger="retl")

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    submission_records = [
        record for record in caplog.records if record.name == "retl.sync_runtime.submission"
    ]
    target_resolution = _record(submission_records, "target_resolution_completed")

    assert result.syncs[0].target_resolution_status == "resolved"
    assert _field(target_resolution, "run_id") == result.run_id
    assert _field(target_resolution, "status") == "resolved"
    assert _field(target_resolution, "target_count") == 1
    assert _field(target_resolution, "mapped_count") == 1
    log_payload = _log_payload(submission_records)
    assert "sensitive_audience_raw" not in log_payload
    assert "remote_audience_raw" not in log_payload


def test_target_resolution_failure_logs_do_not_echo_raw_targets(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    sync = _sync(_targeted_state(backend), surface="list_membership")

    caplog.set_level(logging.INFO, logger="retl")

    with pytest.raises(Exception, match="Target resolution failed"):
        retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    retl_records = [record for record in caplog.records if record.name.startswith("retl.")]
    submission_records = [
        record for record in retl_records if record.name == "retl.sync_runtime.submission"
    ]
    executor_records = [record for record in retl_records if record.name == "retl.runtime.executor"]
    target_failed = _record(submission_records, "target_resolution_failed")
    phase_failed = _record(executor_records, "phase_failed")
    sync_failed = _record(executor_records, "sync_failed")
    run_failed = _record(executor_records, "run_failed")

    assert _field(target_failed, "target_missing_count") == 1
    assert _field(phase_failed, "exception_type") == "DestinationCompatibilityError"
    assert (
        _field(sync_failed, "exception_message") == "Destination compatibility validation failed."
    )
    assert _field(run_failed, "exception_message") == "Destination compatibility validation failed."

    log_payload = _log_payload(retl_records)
    assert "sensitive_audience_raw" not in log_payload
    assert "one@example.com" not in log_payload
    assert "cust_1" not in log_payload


def test_destination_failure_log_redacts_partner_diagnostics(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    sync = _sync(_state(backend), on_failure="stop_on_any")

    def fail_submission(**_: Any) -> DestinationSubmissionEvidence:
        return DestinationSubmissionEvidence(
            status="pre_acceptance_failure",
            attempted_count=2,
            pre_acceptance_failure_count=2,
            pre_acceptance_failure_category="auth",
            http_status=401,
            partner_error_code="auth_failed",
            partner_error_detail="Authorization: Bearer raw-secret-token",
            summary="Partner rejected Authorization: Bearer raw-secret-token",
        )

    monkeypatch.setattr(submission_module, "_submission_evidence", fail_submission)
    caplog.set_level(logging.INFO, logger="retl")

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    assert result.status == "failed"
    submission_records = [
        record for record in caplog.records if record.name == "retl.sync_runtime.submission"
    ]
    failure = _record(submission_records, "destination_submission_failed")
    assert _field(failure, "status") == "pre_acceptance_failure"
    assert _field(failure, "failure_category") == "auth"
    assert _field(failure, "http_status") == 401
    assert "raw-secret-token" not in str(_field(failure, "partner_error_detail"))
    assert "[redacted]" in str(_field(failure, "partner_error_detail"))
    batch_attempt = _record(submission_records, "destination_batch_attempt_completed")
    assert _field(batch_attempt, "status") == "failed"
    assert _field(batch_attempt, "http_status") == 401
    assert "raw-secret-token" not in str(_field(batch_attempt, "diagnostic_summary"))
    assert "[redacted]" in str(_field(batch_attempt, "diagnostic_summary"))
    assert "raw-secret-token" not in _log_payload(
        [record for record in caplog.records if record.name.startswith("retl.")]
    )


def test_runner_failure_logs_redacted_exception_context(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _backend(tmp_path)
    store = backend.runtime_store()
    sync = _sync(_state(backend))

    def fail_collect(**_: Any) -> object:
        raise RuntimeError("collect failed with token=top-secret")

    monkeypatch.setattr(executor, "_produce_collect", fail_collect)
    caplog.set_level(logging.ERROR, logger="retl.runtime.executor")

    with pytest.raises(RuntimeError, match="collect failed"):
        retl.runner(name="crm_to_lifecycle", runtime_store=store).run(sync)

    records = [record for record in caplog.records if record.name == "retl.runtime.executor"]
    run_failed = _record(records, "run_failed")
    phase_failed = _record(records, "phase_failed")

    assert _field(run_failed, "status") == "failed"
    assert _field(run_failed, "exception_type") == "RuntimeError"
    assert "top-secret" not in str(_field(run_failed, "exception_message"))
    assert "[redacted]" in str(_field(run_failed, "exception_message"))
    assert _field(phase_failed, "phase") == "collect"
    assert _field(phase_failed, "exception_type") == "RuntimeError"


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return _backend(tmp_path).runtime_store()


def _backend(tmp_path: Path) -> DuckDBSqlBackend:
    return DuckDBSqlBackend(
        database=_warehouse_database(tmp_path),
        source_schema="main",
        runtime_schema="retl",
    )


def _warehouse_database(tmp_path: Path) -> Path:
    return tmp_path / "warehouse.duckdb"


def _state(backend: DuckDBSqlBackend) -> retl.State:
    source_database = Path(backend.database)
    connection = duckdb.connect(str(source_database))
    connection.execute("create table customers (customer_id varchar, email varchar, plan varchar)")
    connection.executemany(
        "insert into customers values (?, ?, ?)",
        [
            ("cust_1", "one@example.com", "tier_alpha_raw"),
            ("cust_2", "two@example.com", "tier_beta_raw"),
        ],
    )
    connection.close()
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="select customer_id, email, plan from customers",
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _targeted_state(backend: DuckDBSqlBackend) -> retl.State:
    source_database = Path(backend.database)
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table customers (
            customer_id varchar,
            email varchar,
            audience varchar
        )
        """
    )
    connection.executemany(
        "insert into customers values (?, ?, ?)",
        [("cust_1", "one@example.com", "sensitive_audience_raw")],
    )
    connection.close()
    return retl.state(
        name="targeted_customer_state",
        source=retl.source(
            name="targeted_customers",
            query="select customer_id, email, audience from customers",
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience",
        identifiers=[{"type": "email", "value": "email"}],
    )


def _sync(
    declaration: retl.State,
    *,
    surface: str = "profile_properties",
    target_mappings: tuple[TargetMapping, ...] = (),
    on_failure: retl.FailureHandlingMode = "continue_on_any",
) -> retl.Sync:
    return retl.sync(
        name="customer_profiles",
        declaration=declaration,
        destination=retl.destinations.load(
            "retl/mock",
            binding_name="mock_profiles",
            target_mappings=target_mappings,
        ),
        surface=surface,
        on_failure=on_failure,
    )


def _record(records: list[logging.LogRecord], event: str) -> logging.LogRecord:
    matches = [record for record in records if getattr(record, "event", None) == event]
    assert matches
    return matches[-1]


def _field(record: logging.LogRecord, name: str) -> object:
    return getattr(record, name)


def _phase_events(records: list[logging.LogRecord], phase: str) -> set[str]:
    return {
        str(getattr(record, "event", ""))
        for record in records
        if getattr(record, "phase", None) == phase
    }


def _log_payload(records: list[logging.LogRecord]) -> str:
    ignored = {
        "args",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
    }
    payload: list[object] = []
    for record in records:
        payload.append({key: value for key, value in record.__dict__.items() if key not in ignored})
    return repr(payload)
