from __future__ import annotations

from dataclasses import replace
from typing import Any, ClassVar

import pyarrow as pa  # type: ignore[import-untyped]

from retl.destinations.targets import TargetRegistryKey, TargetRegistryRecord
from retl.runtime.recovery import (
    AttemptIdentity,
    AttemptRecord,
    AttemptStatus,
    CommitDecisionRecord,
    ReceiptRecord,
    RemoteHandleRecord,
)
from retl.stores.contracts import (
    DestinationBatchRecord,
    DestinationBatchStatus,
    DestinationProgress,
    DestinationProgressScope,
    DestinationProgressUpdate,
    EventKeysetScanPosition,
    EventProductionResult,
    EventSourceWindowHandle,
    OrderedWorkRetentionCleanup,
    PendingWorkCursor,
    PendingWorkPage,
    ScanPosition,
    StateCurrentCursor,
    StateCurrentPage,
    StateCurrentSnapshotScanPosition,
    StateCurrentSummary,
    StateOrderedWorkScanPosition,
    StateProductionResult,
    StateSnapshotHandle,
    WorkFamily,
)
from retl.stores.sql_runtime import collect as collect_store
from retl.stores.sql_runtime import destination_batches as destination_batch_store
from retl.stores.sql_runtime import ordered_work as ordered_work_store
from retl.stores.sql_runtime import progress as progress_store
from retl.stores.sql_runtime import provenance as provenance_store
from retl.stores.sql_runtime import reports as reports_store
from retl.stores.sql_runtime import state_current as state_current_store
from retl.stores.sql_runtime import target_registry as target_registry_store
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.errors import RuntimeStoreError
from retl.stores.sql_runtime.operations import cleanup as operations_cleanup
from retl.stores.sql_runtime.operations import evidence as operations_evidence
from retl.stores.sql_runtime.operations import inspect as operations_inspect
from retl.stores.sql_runtime.operations import reset as operations_reset
from retl.stores.sql_runtime.operations import skip as operations_skip
from retl.stores.sql_runtime.operations import targets as operations_targets


class SqlRuntimeStore:
    """Shared SQL-backed runtime store method surface."""

    runtime_store_not_initialized_message: ClassVar[str] = "SQL runtime store is not initialized."

    attempts: list[AttemptRecord]
    receipts: list[ReceiptRecord]
    remote_handles: list[RemoteHandleRecord]
    commit_decisions: list[CommitDecisionRecord]
    sync_reports: list[object]
    destination_batches: list[DestinationBatchRecord]
    _next_attempt_number: int
    _connection: Any
    _runtime_context: SqlRuntimeContext | None

    def allocate_collect_id(self) -> str:
        return collect_store.allocate_collect_id(self._context())

    def _context(self) -> SqlRuntimeContext:
        if self._runtime_context is None:
            raise RuntimeStoreError(self.runtime_store_not_initialized_message)
        if self._runtime_context.connection is not self._connection:
            self._runtime_context = replace(self._runtime_context, connection=self._connection)
        return self._runtime_context

    def register_run(self, run: object) -> None:
        provenance_store.register_run(self._context(), run)

    def complete_run(self, *, run_id: str, status: str) -> None:
        provenance_store.complete_run(self._context(), run_id=run_id, status=status)

    def register_declaration(self, metadata: object) -> None:
        provenance_store.register_declaration(self._context(), metadata)

    def get(self, key: TargetRegistryKey) -> TargetRegistryRecord | None:
        return target_registry_store.get_target_registry_record(self._context(), key)

    def put(self, record: TargetRegistryRecord) -> None:
        target_registry_store.put_target_registry_record(self._context(), record)

    def produce_state_collect(
        self,
        *,
        declaration: object,
        snapshot: StateSnapshotHandle,
    ) -> StateProductionResult:
        return collect_store.produce_state_collect(
            self._context(),
            declaration=declaration,
            snapshot=snapshot,
        )

    def state_current_summary(
        self,
        *,
        declaration_name: str,
        source_name: str,
    ) -> StateCurrentSummary:
        return state_current_store.state_current_summary(
            self._context(),
            declaration_name=declaration_name,
            source_name=source_name,
        )

    def read_state_current_upserts(
        self,
        *,
        declaration_name: str,
        source_name: str,
        max_rows: int,
        cursor: StateCurrentCursor | None = None,
        position: StateCurrentSnapshotScanPosition | None = None,
    ) -> StateCurrentPage:
        return state_current_store.read_state_current_upserts(
            self._context(),
            declaration_name=declaration_name,
            source_name=source_name,
            max_rows=max_rows,
            cursor=cursor,
            position=position,
        )

    def produce_event_collect(
        self,
        *,
        declaration: object,
        window: EventSourceWindowHandle,
    ) -> EventProductionResult:
        return collect_store.produce_event_collect(
            self._context(),
            declaration=declaration,
            window=window,
        )

    def read_event_source_window(
        self,
        *,
        declaration: object,
        window: EventSourceWindowHandle,
        max_rows: int,
    ) -> PendingWorkPage:
        return collect_store.read_event_source_window(
            self._context(),
            declaration=declaration,
            window=window,
            max_rows=max_rows,
        )

    def _event_scan_upper_bound_from_window(
        self,
        *,
        cursor_kind: str,
        primary_key_kind: str,
    ) -> EventKeysetScanPosition | None:
        return collect_store.event_scan_upper_bound_from_window(
            self._context(),
            cursor_kind=cursor_kind,
            primary_key_kind=primary_key_kind,
        )

    def _insert_state_upsert_work(
        self,
        *,
        declaration_name: str,
        declaration_version_id: str,
        collect_id: str,
        source_name: str,
        source_identity_json: str,
        sequence_order_offset: int,
    ) -> int:
        return collect_store.insert_state_upsert_work(
            self._context(),
            declaration_name=declaration_name,
            declaration_version_id=declaration_version_id,
            collect_id=collect_id,
            source_name=source_name,
            source_identity_json=source_identity_json,
            sequence_order_offset=sequence_order_offset,
        )

    def _insert_state_remove_work(
        self,
        *,
        declaration_name: str,
        declaration_version_id: str,
        collect_id: str,
        source_name: str,
        source_identity_json: str,
        sequence_order_offset: int,
    ) -> int:
        return collect_store.insert_state_remove_work(
            self._context(),
            declaration_name=declaration_name,
            declaration_version_id=declaration_version_id,
            collect_id=collect_id,
            source_name=source_name,
            source_identity_json=source_identity_json,
            sequence_order_offset=sequence_order_offset,
        )

    def _replace_state_current(
        self,
        *,
        declaration_name: str,
        declaration_version_id: str,
        source_name: str,
        source_identity_json: str,
        collect_id: str,
    ) -> None:
        collect_store.replace_state_current(
            self._context(),
            declaration_name=declaration_name,
            declaration_version_id=declaration_version_id,
            source_name=source_name,
            source_identity_json=source_identity_json,
            collect_id=collect_id,
        )

    def read_pending_work(
        self,
        *,
        scope: DestinationProgressScope,
        max_rows: int,
        cursor: PendingWorkCursor | None = None,
        source_collect_id: str | None = None,
        progress_position: ScanPosition | None = None,
        progress_position_loaded: bool = False,
    ) -> PendingWorkPage:
        return ordered_work_store.read_pending_work(
            self._context(),
            scope=scope,
            max_rows=max_rows,
            cursor=cursor,
            source_collect_id=source_collect_id,
            progress_position=progress_position,
            progress_position_loaded=progress_position_loaded,
        )

    def _read_pending_work_after_cursor(
        self,
        *,
        scope: DestinationProgressScope,
        max_rows: int,
        cursor: PendingWorkCursor,
    ) -> PendingWorkPage:
        return ordered_work_store.read_pending_work_after_cursor(
            self._context(),
            scope=scope,
            max_rows=max_rows,
            cursor=cursor,
        )

    def _validate_stored_pending_work_cursor(
        self,
        *,
        scope: DestinationProgressScope,
        lower_bound: StateOrderedWorkScanPosition | EventKeysetScanPosition | None,
        cursor: PendingWorkCursor,
    ) -> None:
        ordered_work_store.validate_stored_pending_work_cursor(
            self._context(),
            scope=scope,
            lower_bound=lower_bound,
            cursor=cursor,
        )

    def _validate_stored_state_current_cursor(
        self,
        *,
        declaration_name: str,
        source_name: str,
        cursor: StateCurrentCursor,
    ) -> None:
        state_current_store.validate_stored_state_current_cursor(
            self._context(),
            declaration_name=declaration_name,
            source_name=source_name,
            cursor=cursor,
        )

    def _first_pending_collect_id(
        self,
        *,
        scope: DestinationProgressScope,
        lower_bound: StateOrderedWorkScanPosition | EventKeysetScanPosition | None,
    ) -> str | None:
        return ordered_work_store.first_pending_collect_id(
            self._context(),
            scope=scope,
            lower_bound=lower_bound,
        )

    def register_destination_progress(
        self,
        scope: DestinationProgressScope,
    ) -> DestinationProgress:
        return progress_store.register_destination_progress(self._context(), scope)

    def get_destination_progress(
        self,
        scope: DestinationProgressScope,
    ) -> DestinationProgress:
        return progress_store.get_destination_progress(self._context(), scope)

    def update_destination_progress(
        self,
        *,
        scope: DestinationProgressScope,
        position: ScanPosition | None,
        advance: bool = True,
        current_position: ScanPosition | None = None,
        current_position_loaded: bool = False,
    ) -> DestinationProgressUpdate:
        return progress_store.update_destination_progress(
            self._context(),
            scope=scope,
            position=position,
            advance=advance,
            current_position=current_position,
            current_position_loaded=current_position_loaded,
        )

    def retention_watermark(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        progress_positions: tuple[ScanPosition | None, ...] | None = None,
    ) -> str | None:
        return ordered_work_store.retention_watermark(
            self._context(),
            family=family,
            declaration_name=declaration_name,
            progress_positions=progress_positions,
        )

    def cleanup_ordered_work(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        through_collect_id: str | None = None,
        dry_run: bool = False,
    ) -> OrderedWorkRetentionCleanup:
        return ordered_work_store.cleanup_ordered_work(
            self._context(),
            family=family,
            declaration_name=declaration_name,
            through_collect_id=through_collect_id,
            dry_run=dry_run,
        )

    def _ordered_work_count_through(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        through_collect_id: str | None,
    ) -> int:
        return ordered_work_store.ordered_work_count_through(
            self._context(),
            family=family,
            declaration_name=declaration_name,
            through_collect_id=through_collect_id,
        )

    def _ordered_work_count_after(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        after_collect_id: str | None,
    ) -> int:
        return ordered_work_store.ordered_work_count_after(
            self._context(),
            family=family,
            declaration_name=declaration_name,
            after_collect_id=after_collect_id,
        )

    def _retention_collect_id_from_position(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        position: ScanPosition | None,
    ) -> str | None:
        return ordered_work_store.retention_collect_id_from_position(
            self._context(),
            family=family,
            declaration_name=declaration_name,
            position=position,
        )

    def _ordered_work_max_sequence_order(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        collect_id: str,
    ) -> int | None:
        return ordered_work_store.ordered_work_max_sequence_order(
            self._context(),
            family=family,
            declaration_name=declaration_name,
            collect_id=collect_id,
        )

    def begin_attempt(
        self,
        *,
        runner_name: str,
        sync_name: str,
        dry_run: bool,
    ) -> AttemptIdentity:
        attempt_id = f"{runner_name}:{sync_name}:attempt-{self._next_attempt_number}"
        self._next_attempt_number += 1
        identity = AttemptIdentity(
            runner_name=runner_name,
            sync_name=sync_name,
            attempt_id=attempt_id,
        )
        self.attempts.append(
            AttemptRecord(
                identity=identity,
                status="active",
                dry_run=dry_run,
            )
        )
        return identity

    def record_receipt(self, record: ReceiptRecord) -> None:
        self.receipts.append(record)

    def record_remote_handle(self, record: RemoteHandleRecord) -> None:
        self.remote_handles.append(record)

    def record_commit_decision(self, decision: CommitDecisionRecord) -> None:
        self.commit_decisions.append(decision)

    def complete_attempt(self, *, attempt_id: str, status: AttemptStatus) -> None:
        self.attempts = [
            AttemptRecord(
                identity=record.identity,
                status=status if record.identity.attempt_id == attempt_id else record.status,
                dry_run=record.dry_run,
            )
            for record in self.attempts
        ]

    def record_sync_report(self, report: object) -> None:
        reports_store.record_sync_report(self._context(), self.sync_reports, report)

    def upsert_destination_batch(
        self,
        record: DestinationBatchRecord,
    ) -> DestinationBatchRecord:
        return destination_batch_store.upsert_destination_batch(
            self._context(),
            self.destination_batches,
            record,
        )

    def upsert_destination_batches(
        self,
        records: tuple[DestinationBatchRecord, ...],
        *,
        read_back: bool = True,
        existing_batches: tuple[DestinationBatchRecord, ...] | None = None,
    ) -> tuple[DestinationBatchRecord, ...]:
        return destination_batch_store.upsert_destination_batches(
            self._context(),
            self.destination_batches,
            records,
            read_back=read_back,
            existing_batches=existing_batches,
        )

    def get_destination_batch(self, *, batch_id: str) -> DestinationBatchRecord | None:
        return destination_batch_store.get_destination_batch(self._context(), batch_id=batch_id)

    def get_destination_batches(
        self,
        *,
        batch_ids: tuple[str, ...],
    ) -> tuple[DestinationBatchRecord, ...]:
        return destination_batch_store.get_destination_batches(
            self._context(),
            batch_ids=batch_ids,
        )

    def _destination_batches_by_id(
        self,
        batch_ids: tuple[str, ...],
    ) -> dict[str, DestinationBatchRecord]:
        return destination_batch_store.destination_batches_by_id(self._context(), batch_ids)

    def list_destination_batches(
        self,
        *,
        scope: DestinationProgressScope | None = None,
        statuses: tuple[DestinationBatchStatus, ...] = (),
    ) -> tuple[DestinationBatchRecord, ...]:
        return destination_batch_store.list_destination_batches(
            self._context(),
            scope=scope,
            statuses=statuses,
        )

    def list_destination_batch_retry_candidates(
        self,
        *,
        scope: DestinationProgressScope,
        retry_limit: int,
    ) -> tuple[DestinationBatchRecord, ...]:
        return destination_batch_store.list_destination_batch_retry_candidates(
            self._context(),
            scope=scope,
            retry_limit=retry_limit,
        )

    def read_destination_batch_work(self, *, batch: DestinationBatchRecord) -> PendingWorkPage:
        return destination_batch_store.read_destination_batch_work(
            self._context(),
            batch=batch,
        )

    def dismiss_unresolved_destination_batches(
        self,
        *,
        scope: DestinationProgressScope,
    ) -> tuple[DestinationBatchRecord, ...]:
        return destination_batch_store.dismiss_unresolved_destination_batches(
            self._context(),
            self.destination_batches,
            scope=scope,
        )

    def inspect_runtime_store(self) -> dict[str, Any]:
        return operations_inspect.inspect_runtime_store(self._context())

    def inspect_declaration(self, *, declaration_name: str) -> dict[str, Any]:
        return operations_inspect.inspect_declaration(
            self._context(),
            declaration_name=declaration_name,
        )

    def inspect_destination_scope(self, *, scope: DestinationProgressScope) -> dict[str, Any]:
        return operations_inspect.inspect_destination_scope(self._context(), scope=scope)

    def inspect_collect_id(
        self,
        *,
        declaration_name: str,
        collect_id: str,
    ) -> dict[str, Any]:
        return operations_inspect.inspect_collect_id(
            self._context(),
            declaration_name=declaration_name,
            collect_id=collect_id,
        )

    def inspect_target_registry(self, *, destination_name: str | None = None) -> dict[str, Any]:
        return operations_inspect.inspect_target_registry(
            self._context(),
            destination_name=destination_name,
        )

    def inspect_run(self, *, run_id: str) -> dict[str, Any]:
        return operations_inspect.inspect_run(self._context(), run_id=run_id)

    def create_system_skip_batch(
        self,
        *,
        scope: DestinationProgressScope,
        scan_range: Any,
    ) -> DestinationBatchRecord:
        return operations_skip.create_system_skip_batch(
            self._context(),
            self.destination_batches,
            scope=scope,
            scan_range=scan_range,
        )

    def skip_range(
        self,
        *,
        scope: DestinationProgressScope,
        scan_range: Any,
    ) -> dict[str, Any]:
        return operations_skip.skip_range(
            self._context(),
            self.destination_batches,
            scope=scope,
            scan_range=scan_range,
        )

    def reset_runtime_store(self) -> dict[str, Any]:
        context = self._context()
        if context.runtime_reset_uses_transaction():
            with context.transaction():
                result = operations_reset.reset_runtime_store(context)
        else:
            result = operations_reset.reset_runtime_store(context)
        self._clear_runtime_mirrors()
        return result

    def reset_destination_scope(self, *, scope: DestinationProgressScope) -> dict[str, Any]:
        context = self._context()
        with context.transaction():
            result = operations_reset.reset_destination_scope(context, scope=scope)
        self._filter_destination_scope_mirrors(scope=scope)
        return result

    def cleanup_ordered_work_operation(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        through_collect_id: str | None = None,
        older_than_seconds: int | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        context = self._context()
        with context.transaction():
            return operations_cleanup.cleanup_ordered_work(
                context,
                family=family,
                declaration_name=declaration_name,
                through_collect_id=through_collect_id,
                older_than_seconds=older_than_seconds,
                dry_run=dry_run,
            )

    def delete_ordered_work(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        force: bool = False,
    ) -> dict[str, Any]:
        context = self._context()
        with context.transaction():
            return operations_cleanup.delete_ordered_work(
                context,
                family=family,
                declaration_name=declaration_name,
                force=force,
            )

    def cleanup_cursors(
        self,
        *,
        older_than_seconds: int,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        context = self._context()
        with context.transaction():
            return operations_cleanup.cleanup_cursors(
                context,
                older_than_seconds=older_than_seconds,
                dry_run=dry_run,
            )

    def cleanup_evidence(
        self,
        *,
        older_than_seconds: int,
        run_id: str | None = None,
        sync_name: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        context = self._context()
        with context.transaction():
            return operations_cleanup.cleanup_evidence(
                context,
                older_than_seconds=older_than_seconds,
                run_id=run_id,
                sync_name=sync_name,
                dry_run=dry_run,
            )

    def delete_collect_id(
        self,
        *,
        declaration_name: str,
        collect_id: str,
        force: bool = False,
    ) -> dict[str, Any]:
        context = self._context()
        with context.transaction():
            return operations_reset.delete_collect_id(
                context,
                declaration_name=declaration_name,
                collect_id=collect_id,
                force=force,
            )

    def delete_ordered_work_range(
        self,
        *,
        declaration_name: str,
        first_collect_id: str,
        first_sequence_order: int,
        last_collect_id: str,
        last_sequence_order: int,
        family: WorkFamily = "state",
        force: bool = False,
    ) -> dict[str, Any]:
        return operations_reset.delete_ordered_work_range(
            self._context(),
            declaration_name=declaration_name,
            first_collect_id=first_collect_id,
            first_sequence_order=first_sequence_order,
            last_collect_id=last_collect_id,
            last_sequence_order=last_sequence_order,
            family=family,
            force=force,
        )

    def rebaseline_state(
        self,
        *,
        declaration_name: str,
        source_name: str,
        force: bool = False,
    ) -> dict[str, Any]:
        context = self._context()
        with context.transaction():
            return operations_reset.rebaseline_state(
                context,
                declaration_name=declaration_name,
                source_name=source_name,
                force=force,
            )

    def reset_target_registry(
        self,
        *,
        destination_name: str | None = None,
        scope: DestinationProgressScope | None = None,
        target: Any = None,
    ) -> dict[str, Any]:
        return operations_targets.reset_target_registry(
            self._context(),
            destination_name=destination_name,
            scope=scope,
            target=target,
        )

    def delete_run_evidence(self, *, run_id: str) -> dict[str, Any]:
        context = self._context()
        with context.transaction():
            result = operations_evidence.delete_run_evidence(context, run_id=run_id)
        self._filter_run_evidence_mirrors(run_id=run_id)
        return result

    def delete_report_evidence(
        self,
        *,
        run_id: str | None = None,
        sync_name: str | None = None,
    ) -> dict[str, Any]:
        context = self._context()
        with context.transaction():
            result = operations_evidence.delete_report_evidence(
                context,
                run_id=run_id,
                sync_name=sync_name,
            )
        self._filter_report_evidence_mirrors(run_id=run_id, sync_name=sync_name)
        return result

    def _clear_runtime_mirrors(self) -> None:
        self.attempts.clear()
        self.receipts.clear()
        self.remote_handles.clear()
        self.commit_decisions.clear()
        self.sync_reports.clear()
        self.destination_batches.clear()
        self._next_attempt_number = 1

    def _filter_destination_scope_mirrors(
        self,
        *,
        scope: DestinationProgressScope,
    ) -> None:
        self.destination_batches = [
            batch for batch in self.destination_batches if batch.identity.scope != scope
        ]

    def _filter_run_evidence_mirrors(self, *, run_id: str) -> None:
        self.sync_reports = [
            report for report in self.sync_reports if getattr(report, "run_id", None) != run_id
        ]

    def _filter_report_evidence_mirrors(
        self,
        *,
        run_id: str | None,
        sync_name: str | None,
    ) -> None:
        self.sync_reports = [
            report
            for report in self.sync_reports
            if not _report_matches_report_evidence_filter(
                report,
                run_id=run_id,
                sync_name=sync_name,
            )
        ]

    def _persist_sync_report(self, report: object) -> None:
        reports_store.persist_sync_report(self._context(), report)

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._runtime_context = None

    def _next_sequence_order(self, collect_id: str) -> int:
        return collect_store.next_sequence_order(self._context(), collect_id)

    def _oldest_unresolved_destination_batch_collect_id(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        through_collect_id: str,
    ) -> str | None:
        return ordered_work_store.oldest_unresolved_destination_batch_collect_id(
            self._context(),
            family=family,
            declaration_name=declaration_name,
            through_collect_id=through_collect_id,
        )

    def _pending_page_with_cursor(
        self,
        *,
        scope: DestinationProgressScope,
        payload: pa.RecordBatch,
    ) -> PendingWorkPage:
        return ordered_work_store.pending_page_with_cursor(
            self._context(),
            scope=scope,
            payload=payload,
        )

    def _state_current_cursor(
        self,
        *,
        declaration_name: str,
        source_name: str,
        identity: str,
    ) -> StateCurrentCursor:
        return state_current_store.state_current_cursor(
            self._context(),
            declaration_name=declaration_name,
            source_name=source_name,
            identity=identity,
        )


def _report_matches_report_evidence_filter(
    report: object,
    *,
    run_id: str | None,
    sync_name: str | None,
) -> bool:
    if run_id is not None and getattr(report, "run_id", None) != run_id:
        return False
    if sync_name is not None and getattr(report, "sync_name", None) != sync_name:
        return False
    return True
