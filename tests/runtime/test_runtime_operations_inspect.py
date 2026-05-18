from __future__ import annotations

from pathlib import Path

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.destinations.targets import RemoteTarget, TargetRegistryKey, TargetRegistryRecord
from retl.stores.contracts import DestinationProgressScope, OrderedWorkInput
from retl.stores.sql_runtime.schema import RUNTIME_TABLE_CATALOG
from tests.runtime.ordered_work_helpers import append_ordered_work


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")


def _scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name="crm",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )


def test_runtime_operations_inspect_returns_compact_inventory_and_sql_context(
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
                key={"customer": "cust_1"},
            )
        ],
    )
    store.register_destination_progress(_scope())
    runner = retl.runner(name="ops", runtime_store=store)

    runtime = runner.operations.inspect_runtime_store()
    declaration = runner.operations.inspect_declaration("customer_state")
    scope = runner.operations.inspect_destination_scope(_sync())
    collect = runner.operations.inspect_collect_id(
        "customer_state",
        "00000000-0001-7000-8000-000000000000",
    )

    assert runtime["kind"] == "runtime_store"
    assert runtime["tables"]["ordered_work"] == 1
    assert runtime["sql_context"]["backend"] == "duckdb"
    assert (
        declaration["ordered_work_bounds"]["first_collect_id"]
        == "00000000-0001-7000-8000-000000000000"
    )
    assert scope["scope"]["sync_name"] == "customer_sync"
    assert collect["has_destination_batch_evidence"] is False


def test_runtime_operations_do_not_expand_runtime_store_schema() -> None:
    table_names = set(RUNTIME_TABLE_CATALOG)

    assert not any(
        "operation" in table_name or "repair" in table_name for table_name in table_names
    )
    for table in RUNTIME_TABLE_CATALOG.values():
        assert "operation_" not in table.definition_sql
        assert "repair_" not in table.definition_sql


def test_inspect_target_registry_destination_name_filters_binding_name(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.put(
        TargetRegistryRecord(
            key=TargetRegistryKey(
                binding_name="crm",
                destination_ref="retl/mock",
                surface="profile",
                logical_target="vip",
            ),
            remote=RemoteTarget(remote_id="aud_1"),
        )
    )

    summary = retl.runner(name="ops", runtime_store=store).operations.inspect_target_registry(
        destination_name="crm"
    )

    assert summary["target_count"] == 1


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
