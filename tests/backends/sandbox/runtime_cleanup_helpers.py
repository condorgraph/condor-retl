from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, cast

import retl
from retl.sql import column, sql_and
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationProgressScope,
    DestinationScanRange,
    EventKeysetScanPosition,
    StateOrderedWorkScanPosition,
)
from retl.stores.sql_runtime.operations import count_rows
from retl.stores.sql_runtime.writes import RowWriteValues, compile_runtime_insert_many

OLD_TIMESTAMP = datetime(2000, 1, 1)
NEW_TIMESTAMP = datetime(2999, 1, 1)


def assert_event_keyset_skip_operation(store: Any) -> None:
    """Exercise Event source-keyset skip through runtime operations."""

    scope = DestinationProgressScope(
        sync_name="sandbox_event_skip_sync",
        destination_name="sandbox_dest",
        surface="sandbox_events",
        family="event",
        declaration_name="sandbox_event",
    )
    scan_range = DestinationScanRange(
        lower_bound_exclusive=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-01T00:00:00"),
            primary_key_value=CanonicalKeyScalar.string("purchase_1"),
        ),
        first_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-02T00:00:00"),
            primary_key_value=CanonicalKeyScalar.string("purchase_2"),
        ),
        last_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-03T00:00:00"),
            primary_key_value=CanonicalKeyScalar.string("purchase_3"),
        ),
        upper_bound_inclusive=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.string("2026-01-03T00:00:00"),
            primary_key_value=CanonicalKeyScalar.string("purchase_3"),
        ),
    )

    result = retl.runner(
        name="event-keyset-skip-live", runtime_store=store
    ).operations.skip_event_keyset_range(
        scope,
        scan_range,
    )

    assert result["progress_advanced"] is True
    skipped_batches = store.list_destination_batches(scope=scope, statuses=("skipped",))
    assert len(skipped_batches) == 1
    assert skipped_batches[0].completion_state == "resolved"
    assert skipped_batches[0].identity.source_range == scan_range
    assert store.get_destination_progress(scope).position == scan_range.upper_bound_inclusive


def assert_runtime_cleanup_operations(store: Any) -> None:
    """Exercise cleanup operations against the current SQL runtime schema."""

    context = store._context()  # noqa: SLF001

    _assert_cursor_cleanup(store)
    _assert_ordered_work_age_cleanup(store)
    _assert_hard_ordered_work_delete(store)
    _assert_evidence_cleanup(store)

    assert _count(context, "destination_progress") >= 1


def assert_runtime_inspect_reset_operations(store: Any) -> None:
    """Exercise destructive runtime-store reset against an isolated sandbox."""

    runner = retl.runner(name="reset-live", runtime_store=store)
    before = runner.operations.inspect_runtime_store()

    assert before["kind"] == "runtime_store"
    assert before["tables"]["ordered_work"] >= 1
    assert before["tables"]["destination_progress"] >= 1
    assert sum(before["tables"].values()) > 0

    reset = runner.operations.reset_runtime_store()
    assert reset["kind"] == "reset_runtime_store"
    assert set(reset["runtime_tables"]) == set(before["tables"])

    after = runner.operations.inspect_runtime_store()
    assert after["kind"] == "runtime_store"
    assert set(after["tables"]) == set(before["tables"])
    assert all(count == 0 for count in after["tables"].values())


def _assert_cursor_cleanup(store: Any) -> None:
    context = store._context()  # noqa: SLF001
    _insert_runtime_rows(
        context,
        "pending_work_cursors",
        cast(
            Any,
            (
                {
                    "token": "cleanup-pending-old",
                    "sync_name": "cleanup_sync",
                    "destination_name": "cleanup_dest",
                    "surface": "cleanup_surface",
                    "family": "state",
                    "declaration_name": "cleanup_cursor_state",
                    "collect_id": "00000000-0100-7000-8000-000000000000",
                    "sequence_order": 0,
                    "created_at": OLD_TIMESTAMP,
                },
                {
                    "token": "cleanup-pending-new",
                    "sync_name": "cleanup_sync",
                    "destination_name": "cleanup_dest",
                    "surface": "cleanup_surface",
                    "family": "state",
                    "declaration_name": "cleanup_cursor_state",
                    "collect_id": "00000000-0101-7000-8000-000000000000",
                    "sequence_order": 0,
                    "created_at": NEW_TIMESTAMP,
                },
            ),
        ),
    )
    _insert_runtime_rows(
        context,
        "state_current_cursors",
        cast(
            Any,
            (
                {
                    "token": "cleanup-state-old",
                    "declaration_name": "cleanup_cursor_state",
                    "source_name": "cleanup_source",
                    "identity_json": '{"key":{"id":"old"},"target":null}',
                    "created_at": OLD_TIMESTAMP,
                },
                {
                    "token": "cleanup-state-new",
                    "declaration_name": "cleanup_cursor_state",
                    "source_name": "cleanup_source",
                    "identity_json": '{"key":{"id":"new"},"target":null}',
                    "created_at": NEW_TIMESTAMP,
                },
            ),
        ),
    )

    result = retl.runner(name="cleanup-live", runtime_store=store).operations.cleanup_cursors(
        older_than_seconds=1
    )

    assert result["deleted_rows"]["pending_work_cursors"] >= 1
    assert result["deleted_rows"]["state_current_cursors"] >= 1
    assert _count(context, "pending_work_cursors", token="cleanup-pending-old") == 0
    assert _count(context, "pending_work_cursors", token="cleanup-pending-new") == 1
    assert _count(context, "state_current_cursors", token="cleanup-state-old") == 0
    assert _count(context, "state_current_cursors", token="cleanup-state-new") == 1


def _assert_ordered_work_age_cleanup(store: Any) -> None:
    context = store._context()  # noqa: SLF001
    declaration = "cleanup_safe_state"
    collect_id = "00000000-0200-7000-8000-000000000000"
    _insert_runtime_rows(
        context,
        "ordered_work",
        cast(
            Any,
            (
                _ordered_work_row(
                    work_id="cleanup-safe-1",
                    collect_id=collect_id,
                    sequence_order=0,
                    declaration_name=declaration,
                    created_at=OLD_TIMESTAMP,
                ),
                _ordered_work_row(
                    work_id="cleanup-safe-2",
                    collect_id=collect_id,
                    sequence_order=1,
                    declaration_name=declaration,
                    created_at=OLD_TIMESTAMP,
                ),
            ),
        ),
    )
    scope = DestinationProgressScope(
        sync_name="cleanup_sync",
        destination_name="cleanup_dest",
        surface="cleanup_surface",
        family="state",
        declaration_name=declaration,
    )
    store.update_destination_progress(
        scope=scope,
        position=StateOrderedWorkScanPosition(collect_id=collect_id, sequence_order=1),
    )

    result = retl.runner(name="cleanup-live", runtime_store=store).operations.cleanup_ordered_work(
        family="state",
        declaration_name=declaration,
        older_than_seconds=1,
    )

    assert result["age_boundary_collect_id"] == collect_id
    assert result["safe_through_collect_id"] == collect_id
    assert result["deleted_rows"] == {"ordered_work": 2}
    assert _count(context, "ordered_work", declaration_name=declaration) == 0


def _assert_hard_ordered_work_delete(store: Any) -> None:
    context = store._context()  # noqa: SLF001
    delete_declaration = "cleanup_delete_state"
    keep_declaration = "cleanup_keep_state"
    _insert_runtime_rows(
        context,
        "ordered_work",
        cast(
            Any,
            (
                _ordered_work_row(
                    work_id="cleanup-delete-1",
                    collect_id="00000000-0300-7000-8000-000000000000",
                    sequence_order=0,
                    declaration_name=delete_declaration,
                    created_at=OLD_TIMESTAMP,
                ),
                _ordered_work_row(
                    work_id="cleanup-keep-1",
                    collect_id="00000000-0301-7000-8000-000000000000",
                    sequence_order=0,
                    declaration_name=keep_declaration,
                    created_at=OLD_TIMESTAMP,
                ),
            ),
        ),
    )

    result = retl.runner(name="cleanup-live", runtime_store=store).operations.delete_ordered_work(
        family="state",
        declaration_name=delete_declaration,
        force=True,
    )

    assert result["deleted_rows"] == {"ordered_work": 1}
    assert _count(context, "ordered_work", declaration_name=delete_declaration) == 0
    assert _count(context, "ordered_work", declaration_name=keep_declaration) == 1


def _assert_evidence_cleanup(store: Any) -> None:
    context = store._context()  # noqa: SLF001
    _insert_runtime_rows(
        context,
        "runs",
        cast(
            Any,
            (
                _run_row("cleanup-run-failed", "failed"),
                _run_row("cleanup-run-running", "running"),
            ),
        ),
    )
    _insert_runtime_rows(
        context,
        "sync_reports",
        cast(
            Any,
            (
                _report_row("cleanup-report-failed", "cleanup-run-failed"),
                _report_row("cleanup-report-running", "cleanup-run-running"),
            ),
        ),
    )

    result = retl.runner(name="cleanup-live", runtime_store=store).operations.cleanup_evidence(
        older_than_seconds=1
    )

    assert result["deleted_rows"]["sync_reports"] >= 1
    assert result["deleted_rows"]["runs"] >= 1
    assert _count(context, "runs", run_id="cleanup-run-failed") == 0
    assert _count(context, "runs", run_id="cleanup-run-running") == 1
    assert _count(context, "sync_reports", report_id="cleanup-report-failed") == 0
    assert _count(context, "sync_reports", report_id="cleanup-report-running") == 1


def _ordered_work_row(
    *,
    work_id: str,
    collect_id: str,
    sequence_order: int,
    declaration_name: str,
    created_at: datetime,
) -> Mapping[str, object]:
    return {
        "work_id": work_id,
        "collect_id": collect_id,
        "sequence_order": sequence_order,
        "family": "state",
        "kind": "upsert",
        "declaration_name": declaration_name,
        "declaration_version_id": f"decl:{declaration_name}",
        "key_json": f'{{"customer":"{work_id}"}}',
        "target_json": None,
        "identifiers_json": "[]",
        "payload_json": "{}",
        "created_at": created_at,
    }


def _run_row(run_id: str, status: str) -> Mapping[str, object]:
    return {
        "run_id": run_id,
        "runner_name": "cleanup-live",
        "status": status,
        "dry_run": False,
        "script_path": None,
        "script_content_hash": None,
        "started_at": OLD_TIMESTAMP,
        "completed_at": OLD_TIMESTAMP if status != "running" else None,
        "created_at": OLD_TIMESTAMP,
    }


def _report_row(report_id: str, run_id: str) -> Mapping[str, object]:
    return {
        "report_id": report_id,
        "report_ref": None,
        "run_id": run_id,
        "attempt_id": f"attempt-{report_id}",
        "runner_name": "cleanup-live",
        "sync_name": "cleanup_sync",
        "declaration_name": "cleanup_state",
        "declaration_version_id": "decl:cleanup_state",
        "declaration_kind": "state",
        "destination_name": "cleanup_dest",
        "surface": "cleanup_surface",
        "status": "failed",
        "dry_run": False,
        "submitted_record_count": 1,
        "succeeded_record_count": 0,
        "accepted_record_count": 0,
        "failed_record_count": 1,
        "retryable_failure_count": 1,
        "terminal_failure_count": 0,
        "pre_acceptance_failure_count": 0,
        "progress_advanced": False,
        "failure_category": "transport",
        "http_status": 503,
        "last_error_summary": "cleanup sandbox",
        "last_error_detail": None,
        "report_json": "{}",
        "created_at": OLD_TIMESTAMP,
    }


def _count(context: Any, relation: str, **filters: object) -> int:
    params = context.new_params()
    conditions = []
    for name, value in filters.items():
        conditions.append(column(name).eq(params.add(value)))
    where = sql_and(*conditions) if conditions else None
    count, _ = count_rows(context, relation, where=where, params=params.params)
    return count


def _insert_runtime_rows(
    context: Any,
    relation: str,
    rows: tuple[RowWriteValues, ...],
) -> None:
    compiled = compile_runtime_insert_many(context, relation, rows)
    context.connection.execute(compiled.sql, compiled.params)
