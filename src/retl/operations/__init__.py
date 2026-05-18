from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from retl.config import configured_runtime_store
from retl.declarations import Sync
from retl.errors import DeclarationValidationError
from retl.operations.reset import OrderedWorkDeleteRange
from retl.operations.skip import OrderedWorkRange, to_ordered_work_scan_range
from retl.runtime.progress import destination_progress_scope
from retl.stores.contracts import DestinationProgressScope, DestinationScanRange, WorkFamily


@dataclass(frozen=True)
class RuntimeOperations:
    runtime_store: object | None

    def _store(self) -> object:
        store = self.runtime_store or configured_runtime_store()
        if store is None:
            raise DeclarationValidationError(
                "runtime operations require a runtime_store or configured runtime store."
            )
        return store

    def inspect_runtime_store(self) -> dict[str, Any]:
        return self._call("inspect_runtime_store")

    def inspect_declaration(self, declaration_name: str) -> dict[str, Any]:
        return self._call("inspect_declaration", declaration_name=declaration_name)

    def inspect_destination_scope(self, sync: Sync | DestinationProgressScope) -> dict[str, Any]:
        return self._call("inspect_destination_scope", scope=_scope(sync))

    def inspect_collect_id(
        self,
        declaration_name: str,
        collect_id: str,
    ) -> dict[str, Any]:
        return self._call(
            "inspect_collect_id",
            declaration_name=declaration_name,
            collect_id=collect_id,
        )

    def inspect_target_registry(self, destination_name: str | None = None) -> dict[str, Any]:
        return self._call("inspect_target_registry", destination_name=destination_name)

    def inspect_run(self, run_id: str) -> dict[str, Any]:
        return self._call("inspect_run", run_id=run_id)

    def dismiss_unresolved(self, sync: Sync | DestinationProgressScope) -> tuple[object, ...]:
        return self._call(
            "dismiss_unresolved_destination_batches",
            scope=_scope(sync),
        )

    def create_system_skip_batch(
        self,
        sync: Sync | DestinationProgressScope,
        range: OrderedWorkRange | DestinationScanRange,
    ) -> object:
        return self._call(
            "create_system_skip_batch",
            scope=_scope(sync),
            scan_range=to_ordered_work_scan_range(range),
        )

    def skip_ordered_work_range(
        self,
        sync: Sync | DestinationProgressScope,
        range: OrderedWorkRange | DestinationScanRange,
    ) -> dict[str, Any]:
        if isinstance(range, DestinationScanRange) and range.family == "event":
            raise DeclarationValidationError(
                "skip_ordered_work_range is State-only; use skip_event_keyset_range for "
                "Event source keyset ranges."
            )
        return self._call(
            "skip_range",
            scope=_scope(sync),
            scan_range=to_ordered_work_scan_range(range),
        )

    def skip_event_keyset_range(
        self,
        sync: Sync | DestinationProgressScope,
        range: DestinationScanRange,
    ) -> dict[str, Any]:
        return self._call(
            "skip_range",
            scope=_scope(sync),
            scan_range=range,
        )

    def reset_runtime_store(self) -> dict[str, Any]:
        return self._call("reset_runtime_store")

    def reset_destination_scope(self, sync: Sync | DestinationProgressScope) -> dict[str, Any]:
        return self._call("reset_destination_scope", scope=_scope(sync))

    def cleanup_ordered_work(
        self,
        *,
        family: WorkFamily,
        declaration_name: str,
        through_collect_id: str | None = None,
        older_than_seconds: int | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        return self._call(
            "cleanup_ordered_work_operation",
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
        return self._call(
            "delete_ordered_work",
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
        return self._call(
            "cleanup_cursors",
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
        return self._call(
            "cleanup_evidence",
            older_than_seconds=older_than_seconds,
            run_id=run_id,
            sync_name=sync_name,
            dry_run=dry_run,
        )

    def delete_collect_id(
        self,
        declaration_name: str,
        collect_id: str,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        return self._call(
            "delete_collect_id",
            declaration_name=declaration_name,
            collect_id=collect_id,
            force=force,
        )

    def delete_ordered_work_range(
        self,
        declaration_name: str,
        range: OrderedWorkDeleteRange,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        return self._call(
            "delete_ordered_work_range",
            declaration_name=declaration_name,
            first_collect_id=range.first_collect_id,
            first_sequence_order=range.first_sequence_order,
            last_collect_id=range.last_collect_id,
            last_sequence_order=range.last_sequence_order,
            family=range.family,
            force=force,
        )

    def rebaseline_state(
        self,
        declaration_name: str,
        source_name: str,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        return self._call(
            "rebaseline_state",
            declaration_name=declaration_name,
            source_name=source_name,
            force=force,
        )

    def reset_target_registry(
        self,
        *,
        destination_name: str | None = None,
        sync: Sync | DestinationProgressScope | None = None,
        target: object | None = None,
    ) -> dict[str, Any]:
        return self._call(
            "reset_target_registry",
            destination_name=destination_name,
            scope=_scope(sync) if sync is not None else None,
            target=target,
        )

    def delete_run_evidence(self, run_id: str) -> dict[str, Any]:
        return self._call("delete_run_evidence", run_id=run_id)

    def delete_report_evidence(
        self,
        *,
        run_id: str | None = None,
        sync_name: str | None = None,
    ) -> dict[str, Any]:
        return self._call("delete_report_evidence", run_id=run_id, sync_name=sync_name)

    def _call(self, method_name: str, **kwargs: object) -> Any:
        store = self._store()
        method = getattr(store, method_name, None)
        if method is None:
            raise DeclarationValidationError(
                f"runtime operations require a SQL runtime store that implements `{method_name}`."
            )
        return method(**kwargs)


def _scope(value: Sync | DestinationProgressScope) -> DestinationProgressScope:
    if isinstance(value, DestinationProgressScope):
        return value
    if isinstance(value, Sync):
        return destination_progress_scope(value)
    raise DeclarationValidationError(
        "runtime operation destination scope must be a Sync or DestinationProgressScope."
    )


__all__ = [
    "OrderedWorkDeleteRange",
    "OrderedWorkRange",
    "RuntimeOperations",
]
