from __future__ import annotations

from pathlib import Path

import pytest

from retl.backends.duckdb import DuckDBRuntimeStore
from retl.destinations.targets import RemoteTarget, TargetRegistryKey, TargetRegistryRecord
from retl.stores.sql_runtime.errors import RuntimeStoreError


def _key(
    *,
    binding_name: str = "audience_prod",
    destination_ref: str = "retl/audience",
    surface: str = "custom_audience_membership",
    logical_target: str = "vip",
) -> TargetRegistryKey:
    return TargetRegistryKey(
        binding_name=binding_name,
        destination_ref=destination_ref,
        surface=surface,
        logical_target=logical_target,
    )


def _remote(store: DuckDBRuntimeStore, key: TargetRegistryKey) -> RemoteTarget:
    record = store.get(key)
    assert record is not None
    return record.remote


def test_duckdb_target_registry_round_trips_metadata_across_store_instances(
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.duckdb"
    first = DuckDBRuntimeStore(database=database)
    first.put(
        TargetRegistryRecord(
            key=_key(),
            remote=RemoteTarget(
                remote_id="aud_123",
                display_name="VIP Customers",
                metadata={"account_id": "act_123", "retention_days": 180},
            ),
            source="managed_created",
        )
    )
    first.close()

    second = DuckDBRuntimeStore(database=database)

    assert second.get(_key()) == TargetRegistryRecord(
        key=_key(),
        remote=RemoteTarget(
            remote_id="aud_123",
            display_name="VIP Customers",
            metadata={"account_id": "act_123", "retention_days": 180},
        ),
        source="managed_created",
    )


def test_duckdb_target_registry_lookup_is_scoped_by_full_key() -> None:
    store = DuckDBRuntimeStore(database=":memory:")
    records = (
        TargetRegistryRecord(key=_key(binding_name="audience_prod"), remote=RemoteTarget("prod")),
        TargetRegistryRecord(
            key=_key(binding_name="audience_sandbox"), remote=RemoteTarget("sandbox")
        ),
        TargetRegistryRecord(
            key=_key(surface="custom_audience_delete"),
            remote=RemoteTarget("delete_surface"),
        ),
        TargetRegistryRecord(key=_key(logical_target="trial"), remote=RemoteTarget("trial")),
    )
    for record in records:
        store.put(record)

    assert _remote(store, _key(binding_name="audience_prod")) == RemoteTarget("prod")
    assert _remote(store, _key(binding_name="audience_sandbox")) == RemoteTarget("sandbox")
    assert _remote(store, _key(surface="custom_audience_delete")) == RemoteTarget("delete_surface")
    assert _remote(store, _key(logical_target="trial")) == RemoteTarget("trial")


def test_duckdb_target_registry_upserts_existing_record() -> None:
    store = DuckDBRuntimeStore(database=":memory:")
    key = _key()
    store.put(TargetRegistryRecord(key=key, remote=RemoteTarget("old")))
    store.put(
        TargetRegistryRecord(
            key=key,
            remote=RemoteTarget("new", display_name="Updated"),
            source="managed_existing",
        )
    )

    assert store.get(key) == TargetRegistryRecord(
        key=key,
        remote=RemoteTarget("new", display_name="Updated"),
        source="managed_existing",
    )


def test_duckdb_target_registry_lookup_after_close_uses_context_guard() -> None:
    store = DuckDBRuntimeStore(database=":memory:")
    store.close()

    with pytest.raises(RuntimeStoreError, match="DuckDB runtime store is not initialized."):
        store.get(_key())
