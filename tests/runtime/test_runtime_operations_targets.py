from __future__ import annotations

from pathlib import Path

import retl
from retl.backends.duckdb import DuckDBRuntimeStore
from retl.destinations.targets import RemoteTarget, TargetRegistryKey, TargetRegistryRecord
from retl.stores.contracts import DestinationProgressScope, OrderedWorkInput
from tests.runtime.ordered_work_helpers import append_ordered_work


def test_reset_target_registry_isolated_from_runtime_authority(tmp_path: Path) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")
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

    result = retl.runner(name="ops", runtime_store=store).operations.reset_target_registry(
        sync=_sync(),
        target="vip",
    )

    assert result["deleted_rows"]["target_registry"] == 1
    assert store.inspect_runtime_store()["tables"]["ordered_work"] == 1


def test_reset_target_registry_destination_name_filters_binding_name(tmp_path: Path) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")
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

    result = retl.runner(name="ops", runtime_store=store).operations.reset_target_registry(
        destination_name="crm"
    )

    assert result["deleted_rows"]["target_registry"] == 1


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
