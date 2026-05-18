from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.stores.contracts import (
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    destination_batch_id,
)


def test_delete_run_evidence_does_not_delete_destination_batches(tmp_path: Path) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")
    batch = _batch()
    store.upsert_destination_batch(
        DestinationBatchRecord(
            batch_id=batch.batch_id,
            identity=batch.identity,
            run_id="run-1",
            attempt_id="attempt-1",
            record_count=1,
            status="failed",
            completion_state="unresolved",
            attempt_count=1,
        )
    )
    store._connection.execute(
        """
        insert into retl.runs (run_id, runner_name, status, dry_run, started_at)
        values ('run-1', 'ops', 'failed', false, current_timestamp)
        """
    )
    store._connection.execute(
        """
        insert into retl.sync_reports (
            report_id,
            run_id,
            attempt_id,
            runner_name,
            sync_name,
            declaration_name,
            declaration_kind,
            surface,
            status,
            dry_run,
            submitted_record_count,
            succeeded_record_count,
            accepted_record_count,
            failed_record_count,
            retryable_failure_count,
            terminal_failure_count,
            pre_acceptance_failure_count,
            progress_advanced,
            report_json
        )
        values (
            'report-1',
            'run-1',
            'attempt-1',
            'ops',
            'customer_sync',
            'customer_state',
            'state',
            'profile',
            'failed',
            false,
            1,
            0,
            0,
            1,
            1,
            0,
            0,
            false,
            '{}'
        )
        """
    )

    result = retl.runner(name="ops", runtime_store=store).operations.delete_run_evidence("run-1")

    assert result["deleted_rows"]["runs"] == 1
    assert result["deleted_rows"]["sync_reports"] == 1
    assert store.get_destination_batch(batch_id=batch.batch_id) is not None


def test_delete_run_evidence_filters_in_memory_evidence_mirrors(tmp_path: Path) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")
    store.sync_reports.extend(
        [
            SimpleNamespace(run_id="run-1", sync_name="customer_sync"),
            SimpleNamespace(run_id="run-2", sync_name="customer_sync"),
        ]
    )

    retl.runner(name="ops", runtime_store=store).operations.delete_run_evidence("run-1")

    assert [cast(SimpleNamespace, report).run_id for report in store.sync_reports] == ["run-2"]


def test_delete_report_evidence_requires_scope_and_only_deletes_reports(tmp_path: Path) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")
    store._connection.execute(
        """
        insert into retl.sync_reports (
            report_id,
            run_id,
            attempt_id,
            runner_name,
            sync_name,
            declaration_name,
            declaration_kind,
            surface,
            status,
            dry_run,
            submitted_record_count,
            succeeded_record_count,
            accepted_record_count,
            failed_record_count,
            retryable_failure_count,
            terminal_failure_count,
            pre_acceptance_failure_count,
            progress_advanced,
            report_json
        )
        values (
            'report-1',
            'run-1',
            'attempt-1',
            'ops',
            'customer_sync',
            'customer_state',
            'state',
            'profile',
            'failed',
            false,
            1,
            0,
            0,
            1,
            1,
            0,
            0,
            false,
            '{}'
        )
        """
    )

    result = retl.runner(name="ops", runtime_store=store).operations.delete_report_evidence(
        sync_name="customer_sync"
    )

    assert result["deleted_rows"]["sync_reports"] == 1
    assert store.inspect_runtime_store()["tables"]["sync_reports"] == 0


def test_delete_report_evidence_filters_in_memory_report_mirrors(tmp_path: Path) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")
    store.sync_reports.extend(
        [
            SimpleNamespace(run_id="run-1", sync_name="customer_sync"),
            SimpleNamespace(run_id="run-1", sync_name="other_sync"),
            SimpleNamespace(run_id="run-2", sync_name="customer_sync"),
        ]
    )

    retl.runner(name="ops", runtime_store=store).operations.delete_report_evidence(
        run_id="run-1",
        sync_name="customer_sync",
    )

    assert [
        (cast(SimpleNamespace, report).run_id, cast(SimpleNamespace, report).sync_name)
        for report in store.sync_reports
    ] == [
        ("run-1", "other_sync"),
        ("run-2", "customer_sync"),
    ]


def _scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name="crm",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )


def _batch() -> DestinationBatchRecord:
    identity = DestinationBatchIdentity(
        scope=_scope(),
        declaration_version_id="declaration-version-one",
        source_page_index=0,
        reconcile_page_index=0,
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=0,
        last_sequence_order=0,
        destination_batch_index=0,
        payload_fingerprint="payload:1",
        target_request_fingerprint="request:1",
    )
    return DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        record_count=1,
    )
