from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from retl.config import configured_runtime_store
from retl.console import ConsoleInput, resolve_console
from retl.declarations import Event, Sync
from retl.errors import DeclarationValidationError
from retl.runtime.defaults import (
    DEFAULT_RECONCILE_BATCH_MAX_BYTES,
    DEFAULT_RECONCILE_BATCH_MAX_ROWS,
    DEFAULT_STAGE_BATCH_MAX_ROWS,
)
from retl.runtime.recovery import InMemoryAttemptRecoveryStore
from retl.runtime.results import RunResult

if TYPE_CHECKING:
    from retl.operations import RuntimeOperations
    from retl.stores.contracts import (
        DestinationBatchRecord,
        RecoveryStore,
        RuntimeStore,
    )


@dataclass(frozen=True)
class Runner:
    name: str
    runtime_store: RuntimeStore | None = field(default=None, repr=False, compare=False)
    stage_batch_max_rows: int = DEFAULT_STAGE_BATCH_MAX_ROWS
    reconcile_batch_max_rows: int = DEFAULT_RECONCILE_BATCH_MAX_ROWS
    reconcile_batch_max_bytes: int | None = DEFAULT_RECONCILE_BATCH_MAX_BYTES
    recovery_store: RecoveryStore = field(
        default_factory=InMemoryAttemptRecoveryStore,
        repr=False,
        compare=False,
    )
    console: ConsoleInput = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise DeclarationValidationError("Runner `name` must be a non-empty string.")
        _validate_stage_batching(max_rows=self.stage_batch_max_rows)
        _validate_reconcile_batching(
            max_rows=self.reconcile_batch_max_rows,
            max_bytes=self.reconcile_batch_max_bytes,
        )
        object.__setattr__(self, "console", resolve_console(self.console))

    def run(
        self,
        sync: Sync,
        *,
        dry_run: bool = False,
        resend_all: bool = False,
    ) -> RunResult:
        _validate_run_options(
            dry_run=dry_run,
            resend_all=resend_all,
        )
        return self.run_many(
            [sync],
            dry_run=dry_run,
            resend_all=resend_all,
        )

    def run_many(
        self,
        syncs: Sequence[Sync],
        *,
        dry_run: bool = False,
        resend_all: bool = False,
    ) -> RunResult:
        if not syncs:
            raise DeclarationValidationError("run_many requires at least one Sync.")
        _validate_unique_sync_names(syncs)
        _validate_run_options(
            dry_run=dry_run,
            resend_all=resend_all,
        )
        _validate_resend_all_syncs(syncs=syncs, resend_all=resend_all)
        from retl.runtime import run_syncs

        return run_syncs(
            runner_name=self.name,
            syncs=syncs,
            dry_run=dry_run,
            resend_all=resend_all,
            runtime_store=self.runtime_store,
            stage_batch_max_rows=self.stage_batch_max_rows,
            reconcile_batch_max_rows=self.reconcile_batch_max_rows,
            reconcile_batch_max_bytes=self.reconcile_batch_max_bytes,
            recovery_store=self.recovery_store,
            console=self.console,
        )

    def dismiss_unresolved(self, sync: Sync) -> tuple[DestinationBatchRecord, ...]:
        return cast("tuple[DestinationBatchRecord, ...]", self.operations.dismiss_unresolved(sync))

    @property
    def operations(self) -> RuntimeOperations:
        from retl.operations import RuntimeOperations

        return RuntimeOperations(self.runtime_store)


def _validate_run_options(
    *,
    dry_run: bool,
    resend_all: bool,
) -> None:
    if not isinstance(dry_run, bool):
        raise DeclarationValidationError("`dry_run` must be a boolean.")
    if not isinstance(resend_all, bool):
        raise DeclarationValidationError("`resend_all` must be a boolean.")


def _validate_unique_sync_names(syncs: Sequence[Sync]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for sync in syncs:
        if sync.name in seen:
            duplicates.add(sync.name)
        seen.add(sync.name)
    if duplicates:
        duplicate_names = ", ".join(sorted(duplicates))
        raise DeclarationValidationError(
            f"run_many requires unique Sync names; duplicates: {duplicate_names}."
        )


def _validate_resend_all_syncs(*, syncs: Sequence[Sync], resend_all: bool) -> None:
    if not resend_all:
        return
    event_syncs = [sync.name for sync in syncs if isinstance(sync.declaration, Event)]
    if event_syncs:
        names = ", ".join(sorted(event_syncs))
        raise DeclarationValidationError(
            "`resend_all=True` is only valid for State runner execution; "
            f"Event Syncs cannot resend current state: {names}."
        )


def _validate_stage_batching(*, max_rows: int) -> None:
    if not isinstance(max_rows, int) or isinstance(max_rows, bool) or max_rows <= 0:
        raise DeclarationValidationError(
            "`stage_batch_max_rows` must be an integer greater than 0."
        )


def _validate_reconcile_batching(*, max_rows: int, max_bytes: int | None) -> None:
    if not isinstance(max_rows, int) or isinstance(max_rows, bool) or max_rows <= 0:
        raise DeclarationValidationError(
            "`reconcile_batch_max_rows` must be an integer greater than 0."
        )
    if max_bytes is not None and (
        not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes <= 0
    ):
        raise DeclarationValidationError(
            "`reconcile_batch_max_bytes` must be an integer greater than 0 when provided."
        )


def runner(
    *,
    name: str,
    runtime_store: RuntimeStore | None = None,
    stage_batch_max_rows: int = DEFAULT_STAGE_BATCH_MAX_ROWS,
    reconcile_batch_max_rows: int = DEFAULT_RECONCILE_BATCH_MAX_ROWS,
    reconcile_batch_max_bytes: int | None = DEFAULT_RECONCILE_BATCH_MAX_BYTES,
    recovery_store: RecoveryStore | None = None,
    console: ConsoleInput = None,
) -> Runner:
    resolved_runtime_store = runtime_store or configured_runtime_store()
    return Runner(
        name=name,
        runtime_store=resolved_runtime_store,
        stage_batch_max_rows=stage_batch_max_rows,
        reconcile_batch_max_rows=reconcile_batch_max_rows,
        reconcile_batch_max_bytes=reconcile_batch_max_bytes,
        recovery_store=recovery_store or resolved_runtime_store or InMemoryAttemptRecoveryStore(),
        console=console,
    )


__all__ = ["Runner", "runner"]
