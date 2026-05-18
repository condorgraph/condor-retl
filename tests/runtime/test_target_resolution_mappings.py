from __future__ import annotations

import retl
from retl.declarations import DestinationBinding
from retl.destinations.registry import declarative_connector
from retl.destinations.surfaces import DestinationSurface
from retl.destinations.targets import (
    RemoteTarget,
    ResolvedTarget,
    TargetResolutionEvidence,
)
from retl.sync_runtime.submission import _binding_with_resolved_target_mappings


def test_resolved_registry_targets_are_carried_forward_as_in_memory_mappings() -> None:
    connector = declarative_connector(
        ref="retl/test",
        surfaces=(
            DestinationSurface(
                name="audiences",
                declaration_family="state",
                supported_operations=("upsert",),
                target_mode="required",
            ),
        ),
        auth_modes=(retl.auth.none(),),
    )
    binding = DestinationBinding(
        binding_name="primary",
        destination_ref=connector.connector_ref,
        connector=connector,
    )
    target_resolution = TargetResolutionEvidence(
        status="resolved",
        binding_name=binding.binding_name,
        destination_ref=binding.destination_ref,
        surface="audiences",
        dry_run=False,
        target_count=1,
        resolved=(
            ResolvedTarget(
                logical_target="sample_customers",
                remote=RemoteTarget(remote_id="aud_123"),
                source="registry",
            ),
        ),
        registry_count=1,
    )

    updated = _binding_with_resolved_target_mappings(
        binding=binding,
        target_resolution=target_resolution,
    )

    assert len(updated.target_mappings) == 1
    assert updated.target_mappings[0].logical_target == "sample_customers"
    assert updated.target_mappings[0].surface == "audiences"
    assert updated.target_mappings[0].remote.remote_id == "aud_123"
