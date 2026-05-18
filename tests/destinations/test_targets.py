from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from retl.backends.duckdb import DuckDBRuntimeStore
from retl.declarations import DestinationBinding
from retl.destinations.targets import (
    RemoteTarget,
    TargetMapping,
    TargetRegistryRecord,
    TargetResolutionError,
    TargetResolutionFailure,
    registry_key,
    resolve_targets,
)


def _binding(name: str = "mock_prod") -> DestinationBinding:
    return DestinationBinding(binding_name=name, destination_ref="mock")


def _registry() -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=":memory:")


def test_explicit_target_mapping_resolves_before_registry_or_managed_client() -> None:
    binding = _binding()
    registry = _registry()
    registry.put(
        TargetRegistryRecord(
            key=registry_key(
                binding=binding,
                surface="list_membership",
                logical_target="vip",
            ),
            remote=RemoteTarget(remote_id="registry_vip"),
        )
    )
    client = RecordingManagedClient(existing={"vip": RemoteTarget(remote_id="managed_vip")})

    evidence = resolve_targets(
        logical_targets=["vip", "vip"],
        binding=binding,
        surface="list_membership",
        target_mappings=(
            TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="mapped_vip")),
        ),
        registry=registry,
        managed_client=client,
        managed_targets=True,
        dry_run=False,
    )

    assert evidence.status == "resolved"
    assert evidence.target_count == 1
    assert evidence.mapped_count == 1
    assert evidence.registry_count == 0
    assert evidence.managed_reused_count == 0
    assert evidence.resolved[0].remote == RemoteTarget(remote_id="mapped_vip")
    assert evidence.resolved[0].source == "mapping"
    assert client.find_calls == []
    assert client.create_calls == []


def test_surface_specific_target_mapping_overrides_binding_default() -> None:
    evidence = resolve_targets(
        logical_targets=["vip"],
        binding=_binding(),
        surface="subscription_group_membership",
        target_mappings=(
            TargetMapping(logical_target="vip", remote=RemoteTarget(remote_id="default_list")),
            TargetMapping(
                logical_target="vip",
                remote=RemoteTarget(remote_id="surface_group"),
                surface="subscription_group_membership",
            ),
        ),
    )

    assert evidence.status == "resolved"
    assert evidence.mapped_count == 1
    assert evidence.resolved[0].remote == RemoteTarget(remote_id="surface_group")


def test_target_registry_is_scoped_by_binding_and_surface() -> None:
    first_binding = _binding("mock_prod")
    second_binding = _binding("mock_sandbox")
    registry = _registry()
    for record in (
        TargetRegistryRecord(
            key=registry_key(
                binding=first_binding,
                surface="list_membership",
                logical_target="vip",
            ),
            remote=RemoteTarget(remote_id="prod_list"),
        ),
        TargetRegistryRecord(
            key=registry_key(
                binding=first_binding,
                surface="subscription_group_membership",
                logical_target="vip",
            ),
            remote=RemoteTarget(remote_id="prod_group"),
        ),
        TargetRegistryRecord(
            key=registry_key(
                binding=second_binding,
                surface="list_membership",
                logical_target="vip",
            ),
            remote=RemoteTarget(remote_id="sandbox_list"),
        ),
    ):
        registry.put(record)

    evidence = resolve_targets(
        logical_targets=["vip"],
        binding=first_binding,
        surface="list_membership",
        registry=registry,
    )

    assert evidence.status == "resolved"
    assert evidence.registry_count == 1
    assert evidence.resolved[0].remote == RemoteTarget(remote_id="prod_list")


def test_unresolved_target_fails_when_surface_has_no_managed_targets() -> None:
    evidence = resolve_targets(
        logical_targets=["new_audience"],
        binding=_binding(),
        surface="list_membership",
        registry=_registry(),
        managed_targets=False,
        dry_run=False,
    )

    assert evidence.status == "failed"
    assert evidence.missing == ("new_audience",)
    assert evidence.resolved == ()


def test_managed_find_reuses_existing_target_and_records_registry_evidence() -> None:
    binding = _binding()
    registry = _registry()
    client = RecordingManagedClient(existing={"vip": RemoteTarget(remote_id="remote_vip")})

    evidence = resolve_targets(
        logical_targets=["vip"],
        binding=binding,
        surface="managed_list_membership",
        registry=registry,
        managed_client=client,
        managed_targets=True,
        dry_run=False,
    )

    assert evidence.status == "resolved"
    assert evidence.managed_reused_count == 1
    assert evidence.managed_created_count == 0
    assert evidence.resolved[0].source == "managed_existing"
    assert registry.get(
        registry_key(
            binding=binding,
            surface="managed_list_membership",
            logical_target="vip",
        )
    ) == TargetRegistryRecord(
        key=registry_key(
            binding=binding,
            surface="managed_list_membership",
            logical_target="vip",
        ),
        remote=RemoteTarget(remote_id="remote_vip"),
        source="managed_existing",
    )
    assert client.find_calls == ["vip"]
    assert client.create_calls == []


def test_managed_create_runs_after_missing_find_and_records_registry() -> None:
    binding = _binding()
    registry = _registry()
    client = RecordingManagedClient(created_prefix="new")

    evidence = resolve_targets(
        logical_targets=["vip"],
        binding=binding,
        surface="managed_list_membership",
        registry=registry,
        managed_client=client,
        managed_targets=True,
        dry_run=False,
    )

    assert evidence.status == "resolved"
    assert evidence.managed_reused_count == 0
    assert evidence.managed_created_count == 1
    assert evidence.resolved[0].remote == RemoteTarget(remote_id="new_vip", display_name="vip")
    assert registry.get(
        registry_key(
            binding=binding,
            surface="managed_list_membership",
            logical_target="vip",
        )
    ) == TargetRegistryRecord(
        key=registry_key(
            binding=binding,
            surface="managed_list_membership",
            logical_target="vip",
        ),
        remote=RemoteTarget(remote_id="new_vip", display_name="vip"),
        source="managed_created",
    )
    assert client.find_calls == ["vip"]
    assert client.create_calls == [("vip", "vip")]


def test_dry_run_plans_managed_create_without_create_or_registry_write() -> None:
    binding = _binding()
    registry = _registry()
    client = RecordingManagedClient(created_prefix="new")

    evidence = resolve_targets(
        logical_targets=["vip"],
        binding=binding,
        surface="managed_list_membership",
        registry=registry,
        managed_client=client,
        managed_targets=True,
        dry_run=True,
    )

    assert evidence.status == "planned"
    assert evidence.dry_run is True
    assert evidence.planned_create_count == 1
    assert evidence.managed_created_count == 0
    assert evidence.resolved_count == 0
    assert evidence.resolved[0].source == "managed_planned_create"
    assert evidence.resolved[0].remote is None
    assert (
        registry.get(
            registry_key(
                binding=binding,
                surface="managed_list_membership",
                logical_target="vip",
            )
        )
        is None
    )
    assert client.find_calls == ["vip"]
    assert client.create_calls == []


def test_non_dry_run_managed_create_fails_without_registry() -> None:
    client = RecordingManagedClient(created_prefix="new")

    evidence = resolve_targets(
        logical_targets=["vip"],
        binding=_binding(),
        surface="managed_list_membership",
        managed_client=client,
        managed_targets=True,
        dry_run=False,
    )

    assert evidence.status == "failed"
    assert evidence.managed_created_count == 0
    assert evidence.failures == (
        (
            "vip: action=create_target category=target "
            "summary=Managed target creation requires a writable Target Registry."
        ),
    )
    assert evidence.failure_details[0].action == "create_target"
    assert evidence.failure_details[0].category == "target"
    assert client.find_calls == ["vip"]
    assert client.create_calls == []


def test_managed_client_failures_are_reported_without_creating_targets() -> None:
    client = RecordingManagedClient(create_error=RuntimeError("destination rejected target"))

    evidence = resolve_targets(
        logical_targets=["vip"],
        binding=_binding(),
        surface="managed_list_membership",
        registry=_registry(),
        managed_client=client,
        managed_targets=True,
        dry_run=False,
    )

    assert evidence.status == "failed"
    assert evidence.failures == ("vip: category=target summary=destination rejected target",)
    assert evidence.failure_details[0].summary == "destination rejected target"
    assert client.find_calls == ["vip"]
    assert client.create_calls == [("vip", "vip")]


def test_managed_client_structured_failures_preserve_http_diagnostics() -> None:
    client = RecordingManagedClient(
        create_error=TargetResolutionError(
            TargetResolutionFailure(
                logical_target="vip",
                action="create_target",
                category="auth",
                http_status=401,
                partner_error_code="AuthenticationTokenExpired",
                partner_error_detail="token expired",
                summary="HTTP request was not authorized.",
            )
        )
    )

    evidence = resolve_targets(
        logical_targets=["vip"],
        binding=_binding(),
        surface="managed_list_membership",
        registry=_registry(),
        managed_client=client,
        managed_targets=True,
        dry_run=False,
    )

    assert evidence.status == "failed"
    assert evidence.failure_details[0].http_status == 401
    assert evidence.failure_details[0].category == "auth"
    assert evidence.failure_details[0].partner_error_code == "AuthenticationTokenExpired"
    assert "partner_detail=token expired" in evidence.failures[0]


def test_empty_or_none_targets_are_ignored() -> None:
    evidence = resolve_targets(
        logical_targets=[None, "", "   "],
        binding=_binding(),
        surface="list_membership",
    )

    assert evidence.status == "resolved"
    assert evidence.target_count == 0
    assert evidence.resolved == ()


def test_remote_target_rejects_empty_remote_id() -> None:
    with pytest.raises(ValueError, match="remote_id"):
        RemoteTarget(remote_id="")


@dataclass
class RecordingManagedClient:
    existing: dict[str, RemoteTarget] = field(default_factory=dict)
    created_prefix: str = "created"
    create_error: Exception | None = None
    find_calls: list[str] = field(default_factory=list)
    create_calls: list[tuple[str, str]] = field(default_factory=list)

    def find_target(self, logical_target: str) -> RemoteTarget | None:
        self.find_calls.append(logical_target)
        return self.existing.get(logical_target)

    def create_target(self, logical_target: str, *, display_name: str) -> RemoteTarget:
        self.create_calls.append((logical_target, display_name))
        if self.create_error is not None:
            raise self.create_error
        return RemoteTarget(
            remote_id=f"{self.created_prefix}_{logical_target}",
            display_name=display_name,
        )
