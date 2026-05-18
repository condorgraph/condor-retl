from __future__ import annotations

from datetime import datetime
from pathlib import Path

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.stores.contracts import DestinationProgressScope


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")


def test_cleanup_cursors_deletes_stale_cursor_tables_only(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _insert_progress(store)
    store._connection.execute(
        """
        insert into retl.pending_work_cursors (
            token,
            sync_name,
            destination_name,
            surface,
            family,
            declaration_name,
            collect_id,
            sequence_order,
            created_at
        )
        values
            ('pending-old', 'sync', 'dest', 'profile', 'state', 'decl', 'c1', 0, timestamp '2000-01-01'),
            ('pending-new', 'sync', 'dest', 'profile', 'state', 'decl', 'c2', 0, current_timestamp)
        """
    )
    store._connection.execute(
        """
        insert into retl.state_current_cursors (
            token,
            declaration_name,
            source_name,
            identity_json,
            created_at
        )
        values
            ('state-old', 'decl', 'source', '{}', timestamp '2000-01-01'),
            ('state-new', 'decl', 'source', '{"id": 1}', current_timestamp)
        """
    )

    dry_run = retl.runner(name="ops", runtime_store=store).operations.cleanup_cursors(
        older_than_seconds=1,
        dry_run=True,
    )
    result = retl.runner(name="ops", runtime_store=store).operations.cleanup_cursors(
        older_than_seconds=1,
    )

    assert dry_run["deleted_rows"] == {
        "pending_work_cursors": 1,
        "state_current_cursors": 1,
    }
    assert result["deleted_rows"] == {
        "pending_work_cursors": 1,
        "state_current_cursors": 1,
    }
    assert _count(store, "pending_work_cursors") == 1
    assert _count(store, "state_current_cursors") == 1
    assert _count(store, "destination_progress") == 1


def test_cleanup_evidence_deletes_scoped_old_reports_without_progress_or_batches(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _insert_progress(store)
    store._connection.execute(
        """
        insert into retl.runs (
            run_id,
            runner_name,
            status,
            dry_run,
            started_at,
            created_at
        )
        values
            ('run-old', 'ops', 'failed', false, timestamp '2000-01-01', timestamp '2000-01-01'),
            ('run-new', 'ops', 'failed', false, current_timestamp, current_timestamp)
        """
    )
    _insert_report(store, report_id="report-old", run_id="run-old", sync_name="customer_sync")
    _insert_report(store, report_id="report-other", run_id="run-old", sync_name="other_sync")
    _insert_report(store, report_id="report-new", run_id="run-new", sync_name="customer_sync")

    result = retl.runner(name="ops", runtime_store=store).operations.cleanup_evidence(
        older_than_seconds=1,
        sync_name="customer_sync",
    )

    assert result["deleted_rows"] == {"sync_reports": 1, "runs": 0}
    assert _count(store, "sync_reports") == 2
    assert _count(store, "runs") == 2
    assert _count(store, "destination_progress") == 1


def test_cleanup_evidence_retains_running_run_and_reports(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store._connection.execute(
        """
        insert into retl.runs (
            run_id,
            runner_name,
            status,
            dry_run,
            started_at,
            created_at
        )
        values
            ('run-running', 'ops', 'running', false, timestamp '2000-01-01', timestamp '2000-01-01'),
            ('run-failed', 'ops', 'failed', false, timestamp '2000-01-01', timestamp '2000-01-01')
        """
    )
    _insert_report(
        store,
        report_id="report-running",
        run_id="run-running",
        sync_name="customer_sync",
    )
    _insert_report(store, report_id="report-failed", run_id="run-failed", sync_name="customer_sync")

    result = retl.runner(name="ops", runtime_store=store).operations.cleanup_evidence(
        older_than_seconds=1,
    )

    assert result["deleted_rows"] == {"sync_reports": 1, "runs": 1}
    assert _ids(store, "runs", "run_id") == ["run-running"]
    assert _ids(store, "sync_reports", "report_id") == ["report-running"]


def test_cleanup_evidence_with_sync_filter_does_not_delete_run(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store._connection.execute(
        """
        insert into retl.runs (
            run_id,
            runner_name,
            status,
            dry_run,
            started_at,
            created_at
        )
        values ('run-old', 'ops', 'failed', false, timestamp '2000-01-01', timestamp '2000-01-01')
        """
    )
    _insert_report(store, report_id="report-customer", run_id="run-old", sync_name="customer_sync")
    _insert_report(store, report_id="report-other", run_id="run-old", sync_name="other_sync")

    result = retl.runner(name="ops", runtime_store=store).operations.cleanup_evidence(
        older_than_seconds=1,
        run_id="run-old",
        sync_name="customer_sync",
    )

    assert result["deleted_rows"] == {"sync_reports": 1, "runs": 0}
    assert _ids(store, "runs", "run_id") == ["run-old"]
    assert _ids(store, "sync_reports", "report_id") == ["report-other"]


def _insert_report(
    store: DuckDBRuntimeStore,
    *,
    report_id: str,
    run_id: str,
    sync_name: str,
) -> None:
    created_at = datetime.now() if report_id == "report-new" else datetime(2000, 1, 1)
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
            report_json,
            created_at
        )
        values (
            ?,
            ?,
            ?,
            'ops',
            ?,
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
            '{}',
            ?
        )
        """,
        (report_id, run_id, f"attempt-{report_id}", sync_name, created_at),
    )


def _scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name="crm",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )


def _insert_progress(store: DuckDBRuntimeStore) -> None:
    store._connection.execute(
        """
        insert into retl.destination_progress (
            sync_name,
            destination_name,
            surface,
            family,
            declaration_name,
            position_json
        )
        values ('customer_sync', 'crm', 'profile', 'state', 'customer_state', null)
        """
    )


def _count(store: DuckDBRuntimeStore, relation: str) -> int:
    return int(store._connection.execute(f"select count(*) from retl.{relation}").fetchone()[0])


def _ids(store: DuckDBRuntimeStore, relation: str, column: str) -> list[str]:
    rows = store._connection.execute(
        f"select {column} from retl.{relation} order by {column}"
    ).fetchall()
    return [str(row[0]) for row in rows]
