from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import retl
from retl.backends.bigquery import BIGQUERY_DIALECT
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.errors import DeclarationValidationError
from retl.operations import OrderedWorkDeleteRange
from retl.runtime.recovery import (
    AttemptIdentity,
    AttemptRecord,
    CommitDecisionRecord,
)
from retl.stores.contracts import (
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    OrderedWorkInput,
    SqlRelationSpace,
    destination_batch_id,
)
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.operations.reset import reset_runtime_store
from tests.runtime.ordered_work_helpers import append_ordered_work


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")


class _RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        self.calls.append((sql, tuple(parameters)))
        return _OneRowResult()


class _OneRowResult:
    def fetchone(self) -> tuple[int]:
        return (0,)


def test_delete_collect_id_refuses_shared_batch_evidence_then_force_deletes(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    append_ordered_work(
        store,
        [
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="customer_state",
            )
        ],
    )
    store.upsert_destination_batch(_batch())
    ops = retl.runner(name="ops", runtime_store=store).operations

    with pytest.raises(DeclarationValidationError, match="destination batch evidence"):
        ops.delete_collect_id("customer_state", "00000000-0001-7000-8000-000000000000")

    result = ops.delete_collect_id(
        "customer_state",
        "00000000-0001-7000-8000-000000000000",
        force=True,
    )

    assert result["deleted_rows"]["ordered_work"] == 1
    assert (
        store.inspect_collect_id(
            declaration_name="customer_state", collect_id="00000000-0001-7000-8000-000000000000"
        )["ordered_work_rows"]
        == 0
    )


def test_delete_ordered_work_range_and_reset_destination_scope_are_scoped(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    append_ordered_work(
        store,
        [
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="a",
            ),
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="b",
            ),
        ],
    )
    store.register_destination_progress(_scope())
    ops = retl.runner(name="ops", runtime_store=store).operations

    deleted = ops.delete_ordered_work_range(
        "a",
        OrderedWorkDeleteRange(
            "00000000-0001-7000-8000-000000000000",
            0,
            "00000000-0001-7000-8000-000000000000",
            0,
        ),
    )
    reset = ops.reset_destination_scope(_sync())

    assert deleted["deleted_rows"]["ordered_work"] == 1
    assert store.inspect_declaration(declaration_name="b")["ordered_work_rows"] == 1
    assert reset["deleted_rows"]["destination_progress"] == 0


def test_delete_ordered_work_requires_force_and_deletes_declaration_family(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    append_ordered_work(
        store,
        [
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="a",
            ),
            OrderedWorkInput(
                collect_id="00000000-0002-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="a",
            ),
            OrderedWorkInput(
                collect_id="00000000-0003-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="b",
            ),
        ],
    )
    ops = retl.runner(name="ops", runtime_store=store).operations

    with pytest.raises(DeclarationValidationError, match="destructive"):
        ops.delete_ordered_work(family="state", declaration_name="a")

    result = ops.delete_ordered_work(family="state", declaration_name="a", force=True)

    assert result["deleted_rows"]["ordered_work"] == 2
    assert store.inspect_declaration(declaration_name="a")["ordered_work_rows"] == 0
    assert store.inspect_declaration(declaration_name="b")["ordered_work_rows"] == 1


def test_reset_runtime_store_deletes_authority_without_schema_expansion(tmp_path: Path) -> None:
    store = _store(tmp_path)
    append_ordered_work(
        store,
        [
            OrderedWorkInput(
                collect_id="00000000-0001-7000-8000-000000000000",
                family="state",
                kind="upsert",
                declaration_name="a",
            )
        ],
    )

    result = retl.runner(name="ops", runtime_store=store).operations.reset_runtime_store()

    assert result["deleted_rows"]["ordered_work"] is None
    assert result["deleted_rows_exact"] is False
    assert store.inspect_runtime_store()["tables"]["ordered_work"] == 0


def test_reset_runtime_store_uses_truncate_for_bigquery() -> None:
    connection = _RecordingConnection()
    context = SqlRuntimeContext(
        connection=connection,
        dialect=BIGQUERY_DIALECT,
        runtime_space=SqlRelationSpace(
            backend_name="bigquery",
            database="project",
            schema="runtime",
            access="read_write",
        ),
    )

    result = reset_runtime_store(context)

    truncate_sql = [sql for sql, _ in connection.calls if sql.startswith("TRUNCATE TABLE")]
    assert truncate_sql
    assert not any(sql.startswith("SELECT COUNT(*)") for sql, _ in connection.calls)
    assert not any(sql.startswith("DELETE FROM") for sql, _ in connection.calls)
    assert result["deleted_rows_exact"] is False
    assert all(count is None for count in result["deleted_rows"].values())


def test_reset_runtime_store_clears_in_memory_mirrors(tmp_path: Path) -> None:
    store = _store(tmp_path)
    batch = _batch()
    attempt_identity = AttemptIdentity(
        runner_name="ops",
        sync_name="customer_sync",
        attempt_id="attempt-1",
    )
    store.attempts.append(AttemptRecord(identity=attempt_identity, status="active", dry_run=False))
    store.commit_decisions.append(
        CommitDecisionRecord(
            attempt_id="attempt-1",
            sync_name="customer_sync",
            progress_advanced=True,
            reason="test",
        )
    )
    store.sync_reports.append(SimpleNamespace(run_id="run-1", sync_name="customer_sync"))
    store.destination_batches.append(batch)

    retl.runner(name="ops", runtime_store=store).operations.reset_runtime_store()

    assert store.attempts == []
    assert store.commit_decisions == []
    assert store.sync_reports == []
    assert store.destination_batches == []
    assert store._next_attempt_number == 1


def test_reset_destination_scope_filters_in_memory_mirrors(tmp_path: Path) -> None:
    store = _store(tmp_path)
    scoped = _batch()
    other = _batch(scope=_other_scope(), index=1)
    store.upsert_destination_batches((scoped, other))

    retl.runner(name="ops", runtime_store=store).operations.reset_destination_scope(_sync())

    assert store.destination_batches == [other]


def _sync() -> retl.Sync:
    state = retl.state(
        name="customer_state",
        source=retl.source(name="customers", query="select * from customers"),
        key={"customer": "customer_id"},
        identifiers=[],
        payload={},
    )
    return retl.sync(
        name="customer_sync",
        declaration=state,
        destination=retl.DestinationBinding(binding_name="crm", destination_ref="retl/mock"),
        surface="profile",
    )


def _scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name="crm",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )


def _other_scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="other_sync",
        destination_name="crm",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )


def _batch(
    *,
    scope: DestinationProgressScope | None = None,
    index: int = 0,
) -> DestinationBatchRecord:
    identity = DestinationBatchIdentity(
        scope=scope or _scope(),
        declaration_version_id="declaration-version-one",
        source_page_index=index,
        reconcile_page_index=index,
        first_collect_id="00000000-0001-7000-8000-000000000000",
        last_collect_id="00000000-0001-7000-8000-000000000000",
        first_sequence_order=index,
        last_sequence_order=index,
        destination_batch_index=index,
        payload_fingerprint=f"payload:{index}",
        target_request_fingerprint=f"request:{index}",
    )
    return DestinationBatchRecord(
        batch_id=destination_batch_id(identity),
        identity=identity,
        record_count=1,
    )
