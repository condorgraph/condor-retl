from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, Protocol, TypeAlias

from retl.declarations import DestinationBinding, JSONValue
from retl.destinations.receipts import sanitize_partner_error_detail, sanitize_partner_message

TargetResolutionStatus: TypeAlias = Literal["resolved", "planned", "failed"]
TargetResolutionSource: TypeAlias = Literal[
    "mapping",
    "registry",
    "managed_existing",
    "managed_created",
    "managed_planned_create",
]
TargetResolutionFailureCategory: TypeAlias = Literal[
    "transport",
    "auth",
    "schema",
    "rate_limit",
    "submission",
    "target",
]


@dataclass(frozen=True)
class RemoteTarget:
    remote_id: str
    display_name: str | None = None
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.remote_id.strip():
            raise ValueError("Remote Target `remote_id` must be a non-empty string.")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class TargetMapping:
    logical_target: str
    remote: RemoteTarget
    surface: str | None = None

    def __post_init__(self) -> None:
        if not self.logical_target.strip():
            raise ValueError("Target Mapping `logical_target` must be a non-empty string.")
        if self.surface is not None and not self.surface.strip():
            raise ValueError("Target Mapping `surface` must be non-empty when provided.")


@dataclass(frozen=True)
class TargetRegistryKey:
    binding_name: str
    destination_ref: str
    surface: str
    logical_target: str

    def __post_init__(self) -> None:
        for field_name, value in (
            ("binding_name", self.binding_name),
            ("destination_ref", self.destination_ref),
            ("surface", self.surface),
            ("logical_target", self.logical_target),
        ):
            if not value.strip():
                raise ValueError(f"Target Registry `{field_name}` must be a non-empty string.")


@dataclass(frozen=True)
class TargetRegistryRecord:
    key: TargetRegistryKey
    remote: RemoteTarget
    source: Literal["managed_existing", "managed_created"] = "managed_created"


class TargetRegistry(Protocol):
    def get(self, key: TargetRegistryKey) -> TargetRegistryRecord | None: ...

    def put(self, record: TargetRegistryRecord) -> None: ...


class ManagedTargetClient(Protocol):
    def find_target(self, logical_target: str) -> RemoteTarget | None: ...

    def create_target(self, logical_target: str, *, display_name: str) -> RemoteTarget: ...


@dataclass(frozen=True)
class ResolvedTarget:
    logical_target: str
    remote: RemoteTarget | None
    source: TargetResolutionSource


@dataclass(frozen=True)
class TargetResolutionFailure:
    logical_target: str
    summary: str
    action: Literal["find_target", "create_target"] | str | None = None
    category: TargetResolutionFailureCategory | str | None = None
    http_status: int | None = None
    partner_error_code: str | None = None
    partner_error_subcode: str | None = None
    partner_error_detail: str | None = None

    def __post_init__(self) -> None:
        if not self.logical_target.strip():
            raise ValueError("Target resolution failure `logical_target` must be non-empty.")
        if not self.summary.strip():
            raise ValueError("Target resolution failure `summary` must be non-empty.")
        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise ValueError("Target resolution failure `http_status` must be between 100 and 599.")
        object.__setattr__(self, "summary", sanitize_partner_message(self.summary) or self.summary)
        object.__setattr__(
            self,
            "partner_error_detail",
            sanitize_partner_error_detail(self.partner_error_detail),
        )

    def message(self) -> str:
        parts = [f"{self.logical_target}:"]
        if self.action:
            parts.append(f"action={self.action}")
        if self.http_status is not None:
            parts.append(f"http_status={self.http_status}")
        if self.category:
            parts.append(f"category={self.category}")
        if self.partner_error_code:
            parts.append(f"partner_code={self.partner_error_code}")
        if self.partner_error_subcode:
            parts.append(f"partner_subcode={self.partner_error_subcode}")
        parts.append(f"summary={self.summary}")
        if self.partner_error_detail:
            parts.append(f"partner_detail={self.partner_error_detail}")
        return " ".join(parts)


class TargetResolutionError(Exception):
    def __init__(self, failure: TargetResolutionFailure) -> None:
        super().__init__(failure.message())
        self.failure = failure


@dataclass(frozen=True)
class TargetResolutionEvidence:
    status: TargetResolutionStatus
    binding_name: str
    destination_ref: str
    surface: str
    dry_run: bool
    target_count: int
    resolved: tuple[ResolvedTarget, ...]
    mapped_count: int = 0
    registry_count: int = 0
    managed_reused_count: int = 0
    managed_created_count: int = 0
    planned_create_count: int = 0
    missing: tuple[str, ...] = ()
    failures: tuple[str, ...] = ()
    failure_details: tuple[TargetResolutionFailure, ...] = ()

    @property
    def resolved_count(self) -> int:
        return sum(1 for target in self.resolved if target.remote is not None)


def resolve_targets(
    *,
    logical_targets: Iterable[str | None],
    binding: DestinationBinding,
    surface: str,
    target_mappings: Iterable[TargetMapping] = (),
    registry: TargetRegistry | None = None,
    managed_client: ManagedTargetClient | None = None,
    managed_targets: bool = False,
    dry_run: bool = False,
) -> TargetResolutionEvidence:
    """Resolve unique logical Targets before destination mutation submission."""

    targets = _normalize_targets(logical_targets)
    if not targets:
        return TargetResolutionEvidence(
            status="resolved",
            binding_name=binding.binding_name,
            destination_ref=binding.destination_ref,
            surface=surface,
            dry_run=dry_run,
            target_count=0,
            resolved=(),
        )

    mappings_by_target = _mapping_index(target_mappings, surface=surface)
    resolved: list[ResolvedTarget] = []
    missing: list[str] = []
    failures: list[str] = []
    failure_details: list[TargetResolutionFailure] = []
    mapped_count = 0
    registry_count = 0
    managed_reused_count = 0
    managed_created_count = 0
    planned_create_count = 0

    for logical_target in targets:
        mapped = mappings_by_target.get(logical_target)
        if mapped is not None:
            resolved.append(
                ResolvedTarget(
                    logical_target=logical_target,
                    remote=mapped.remote,
                    source="mapping",
                )
            )
            mapped_count += 1
            continue

        key = registry_key(binding=binding, surface=surface, logical_target=logical_target)
        registered = registry.get(key) if registry is not None else None
        if registered is not None:
            resolved.append(
                ResolvedTarget(
                    logical_target=logical_target,
                    remote=registered.remote,
                    source="registry",
                )
            )
            registry_count += 1
            continue

        if not managed_targets or managed_client is None:
            missing.append(logical_target)
            continue

        try:
            existing = managed_client.find_target(logical_target)
            if existing is not None:
                if not dry_run and registry is None:
                    failure = TargetResolutionFailure(
                        logical_target=logical_target,
                        action="find_target",
                        category="target",
                        summary="Managed target lookup requires a writable Target Registry.",
                    )
                    failures.append(failure.message())
                    failure_details.append(failure)
                    continue
                resolved.append(
                    ResolvedTarget(
                        logical_target=logical_target,
                        remote=existing,
                        source="managed_existing",
                    )
                )
                managed_reused_count += 1
                if not dry_run:
                    if registry is None:
                        raise AssertionError("registry is required after managed lookup guard.")
                    registry.put(
                        TargetRegistryRecord(
                            key=key,
                            remote=existing,
                            source="managed_existing",
                        )
                    )
                continue

            if dry_run:
                resolved.append(
                    ResolvedTarget(
                        logical_target=logical_target,
                        remote=None,
                        source="managed_planned_create",
                    )
                )
                planned_create_count += 1
                continue

            if registry is None:
                failure = TargetResolutionFailure(
                    logical_target=logical_target,
                    action="create_target",
                    category="target",
                    summary="Managed target creation requires a writable Target Registry.",
                )
                failures.append(failure.message())
                failure_details.append(failure)
                continue

            try:
                created = managed_client.create_target(
                    logical_target,
                    display_name=logical_target,
                )
            except TargetResolutionError as exc:
                failures.append(exc.failure.message())
                failure_details.append(exc.failure)
                continue
            resolved.append(
                ResolvedTarget(
                    logical_target=logical_target,
                    remote=created,
                    source="managed_created",
                )
            )
            managed_created_count += 1
            registry.put(
                TargetRegistryRecord(
                    key=key,
                    remote=created,
                    source="managed_created",
                )
            )
        except TargetResolutionError as exc:
            failures.append(exc.failure.message())
            failure_details.append(exc.failure)
        except Exception as exc:
            failure = TargetResolutionFailure(
                logical_target=logical_target,
                category="target",
                summary=str(exc),
            )
            failures.append(failure.message())
            failure_details.append(failure)

    status: TargetResolutionStatus = "resolved"
    if failures or missing:
        status = "failed"
    elif planned_create_count:
        status = "planned"

    return TargetResolutionEvidence(
        status=status,
        binding_name=binding.binding_name,
        destination_ref=binding.destination_ref,
        surface=surface,
        dry_run=dry_run,
        target_count=len(targets),
        resolved=tuple(resolved),
        mapped_count=mapped_count,
        registry_count=registry_count,
        managed_reused_count=managed_reused_count,
        managed_created_count=managed_created_count,
        planned_create_count=planned_create_count,
        missing=tuple(missing),
        failures=tuple(failures),
        failure_details=tuple(failure_details),
    )


def registry_key(
    *,
    binding: DestinationBinding,
    surface: str,
    logical_target: str,
) -> TargetRegistryKey:
    return TargetRegistryKey(
        binding_name=binding.binding_name,
        destination_ref=binding.destination_ref,
        surface=surface,
        logical_target=logical_target,
    )


def _normalize_targets(logical_targets: Iterable[str | None]) -> tuple[str, ...]:
    normalized: list[str] = []
    seen: set[str] = set()
    for target in logical_targets:
        if target is None:
            continue
        stripped = target.strip()
        if not stripped or stripped in seen:
            continue
        normalized.append(stripped)
        seen.add(stripped)
    return tuple(normalized)


def _mapping_index(
    mappings: Iterable[TargetMapping],
    *,
    surface: str,
) -> dict[str, TargetMapping]:
    default_mappings: dict[str, TargetMapping] = {}
    surface_mappings: dict[str, TargetMapping] = {}
    for mapping in mappings:
        if mapping.surface is None:
            default_mappings[mapping.logical_target] = mapping
        elif mapping.surface == surface:
            surface_mappings[mapping.logical_target] = mapping
    return default_mappings | surface_mappings


__all__ = [
    "ManagedTargetClient",
    "RemoteTarget",
    "ResolvedTarget",
    "TargetMapping",
    "TargetRegistry",
    "TargetRegistryKey",
    "TargetRegistryRecord",
    "TargetResolutionError",
    "TargetResolutionEvidence",
    "TargetResolutionFailure",
    "TargetResolutionFailureCategory",
    "TargetResolutionSource",
    "TargetResolutionStatus",
    "registry_key",
    "resolve_targets",
]
