"""Destination Surface compatibility validation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from retl.declarations import Event, State, Sync
from retl.destinations.operations import (
    StateOperation,
    normalize_state_operations,
    planned_state_operations,
)
from retl.destinations.surfaces import DestinationConnector, DestinationSurface
from retl.errors import RetlError


class DestinationCompatibilityError(RetlError, ValueError):
    """Raised when a Sync cannot be sent to a selected Destination Surface."""


@dataclass(frozen=True)
class CompatibilityIssue:
    code: str
    message: str


@dataclass(frozen=True)
class DestinationCompatibility:
    sync_name: str
    surface_name: str
    family: Literal["state", "event"]
    operation_kinds: tuple[StateOperation, ...]
    delivery_outcome: str
    issues: tuple[CompatibilityIssue, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.issues


def validate_surface_compatibility(
    *,
    sync: Sync,
    surface: DestinationSurface,
    operation_kinds: Iterable[str] | None = None,
) -> DestinationCompatibility:
    """Validate declaration, Sync options, and planned work against a surface."""

    result = check_surface_compatibility(
        sync=sync,
        surface=surface,
        operation_kinds=operation_kinds,
    )
    if not result.valid:
        raise DestinationCompatibilityError(_format_issues(result.issues))
    return result


def check_surface_compatibility(
    *,
    sync: Sync,
    surface: DestinationSurface,
    operation_kinds: Iterable[str] | None = None,
) -> DestinationCompatibility:
    issues: list[CompatibilityIssue] = []
    declaration = sync.declaration
    family: Literal["state", "event"]
    operations: tuple[StateOperation, ...] = ()

    if isinstance(declaration, State):
        family = "state"
        operations = _state_operations_for_validation(sync=sync, operation_kinds=operation_kinds)
        if not surface.accepts_state:
            issues.append(
                CompatibilityIssue(
                    code="family_mismatch",
                    message=(
                        f"State Sync `{sync.name}` cannot use Event surface `{surface.name}`."
                    ),
                )
            )
        _check_state_surface(
            declaration=declaration,
            surface=surface,
            operations=operations,
            issues=issues,
        )
    elif isinstance(declaration, Event):
        family = "event"
        if not surface.accepts_event:
            issues.append(
                CompatibilityIssue(
                    code="family_mismatch",
                    message=(
                        f"Event Sync `{sync.name}` cannot use State surface `{surface.name}`."
                    ),
                )
            )
        _check_event_surface(declaration=declaration, surface=surface, issues=issues)
    else:
        raise DestinationCompatibilityError("Sync declaration must be State or Event.")

    _check_common_requirements(sync=sync, surface=surface, issues=issues)
    return DestinationCompatibility(
        sync_name=sync.name,
        surface_name=surface.name,
        family=family,
        operation_kinds=operations,
        delivery_outcome=surface.delivery_outcome,
        issues=tuple(issues),
    )


def validate_connector_surface_compatibility(
    *,
    sync: Sync,
    connector: DestinationConnector,
    operation_kinds: Iterable[str] | None = None,
) -> DestinationCompatibility:
    """Look up the Sync-selected surface on a connector, then validate it."""

    try:
        surface = connector.surface(sync.surface)
    except KeyError as exc:
        raise DestinationCompatibilityError(str(exc)) from exc
    return validate_surface_compatibility(
        sync=sync,
        surface=surface,
        operation_kinds=operation_kinds,
    )


def _check_state_surface(
    *,
    declaration: State,
    surface: DestinationSurface,
    operations: tuple[StateOperation, ...],
    issues: list[CompatibilityIssue],
) -> None:
    if declaration.target is None and surface.target_mode == "required":
        issues.append(
            CompatibilityIssue(
                code="target_required",
                message=f"Surface `{surface.name}` requires State target routing.",
            )
        )
    if declaration.target is not None and surface.target_mode == "unsupported":
        issues.append(
            CompatibilityIssue(
                code="target_unsupported",
                message=f"Surface `{surface.name}` does not accept State target routing.",
            )
        )
    unsupported = tuple(
        operation for operation in operations if operation not in surface.supported_operations
    )
    if unsupported:
        issues.append(
            CompatibilityIssue(
                code="unsupported_operation",
                message=(
                    f"Surface `{surface.name}` does not support State Operation(s): "
                    f"{', '.join(unsupported)}."
                ),
            )
        )
    _check_key_fields(
        declaration_fields=declaration.key,
        surface=surface,
        issues=issues,
    )
    _check_identifier_types(
        identifiers=declaration.identifiers,
        surface=surface,
        issues=issues,
    )
    _check_payload_fields(
        payload=declaration.payload,
        surface=surface,
        issues=issues,
    )


def _state_operations_for_validation(
    *,
    sync: Sync,
    operation_kinds: Iterable[str] | None,
) -> tuple[StateOperation, ...]:
    operations: list[StateOperation] = []
    for operation in planned_state_operations(sync):
        operations.append(operation)
    if operation_kinds is not None:
        for operation in normalize_state_operations(operation_kinds):
            if operation not in operations:
                operations.append(operation)
    return tuple(operations)


def _check_event_surface(
    *,
    declaration: Event,
    surface: DestinationSurface,
    issues: list[CompatibilityIssue],
) -> None:
    _check_key_fields(
        declaration_fields=declaration.key,
        surface=surface,
        issues=issues,
    )
    _check_identifier_types(
        identifiers=declaration.identifiers,
        surface=surface,
        issues=issues,
    )
    _check_payload_fields(
        payload=declaration.payload,
        surface=surface,
        issues=issues,
    )


def _check_common_requirements(
    *,
    sync: Sync,
    surface: DestinationSurface,
    issues: list[CompatibilityIssue],
) -> None:
    _ = sync, surface, issues


def _check_key_fields(
    *,
    declaration_fields: Mapping[str, str],
    surface: DestinationSurface,
    issues: list[CompatibilityIssue],
) -> None:
    missing = sorted(set(surface.required_key_fields) - set(declaration_fields))
    if missing:
        issues.append(
            CompatibilityIssue(
                code="missing_key_fields",
                message=(f"Surface `{surface.name}` requires key field(s): {', '.join(missing)}."),
            )
        )


def _check_identifier_types(
    *,
    identifiers: Sequence[Mapping[str, str]],
    surface: DestinationSurface,
    issues: list[CompatibilityIssue],
) -> None:
    available: set[str] = set()
    for identifier in identifiers:
        identifier_type = identifier.get("type")
        if identifier_type:
            available.add(identifier_type)
    accepted = set(surface.accepted_identifier_types)
    unsupported = sorted(
        identifier_type for identifier_type in available if identifier_type not in accepted
    )
    if unsupported:
        issues.append(
            CompatibilityIssue(
                code="unsupported_identifier_type",
                message=(
                    f"Surface `{surface.name}` does not accept identifier type(s): "
                    f"{', '.join(unsupported)}."
                ),
            )
        )
    if not surface.identifier_requirements:
        return

    failed_requirement_issues: list[CompatibilityIssue] = []
    for requirement in surface.identifier_requirements:
        requirement_types = set(requirement.identifier_types)
        if requirement.match == "any_of":
            if not available & requirement_types:
                failed_requirement_issues.append(
                    CompatibilityIssue(
                        code="missing_any_identifier_type",
                        message=(
                            f"Surface `{surface.name}` requires at least one identifier type "
                            f"from: {', '.join(requirement.identifier_types)}."
                        ),
                    )
                )
        elif requirement.match == "all_of":
            missing = tuple(
                identifier_type
                for identifier_type in requirement.identifier_types
                if identifier_type not in available
            )
            if missing:
                failed_requirement_issues.append(
                    CompatibilityIssue(
                        code="missing_all_identifier_types",
                        message=(
                            f"Surface `{surface.name}` requires all identifier type(s): "
                            f"{', '.join(missing)}."
                        ),
                    )
                )
    issues.extend(failed_requirement_issues)


def _check_payload_fields(
    *,
    payload: Mapping[str, str],
    surface: DestinationSurface,
    issues: list[CompatibilityIssue],
) -> None:
    missing = sorted(set(surface.required_payload_fields) - set(payload))
    if missing:
        issues.append(
            CompatibilityIssue(
                code="missing_payload_fields",
                message=(
                    f"Surface `{surface.name}` requires payload field(s): {', '.join(missing)}."
                ),
            )
        )


def _format_issues(issues: tuple[CompatibilityIssue, ...]) -> str:
    return "Destination Surface compatibility failed: " + " ".join(
        issue.message for issue in issues
    )


__all__ = [
    "CompatibilityIssue",
    "DestinationCompatibility",
    "DestinationCompatibilityError",
    "check_surface_compatibility",
    "validate_connector_surface_compatibility",
    "validate_surface_compatibility",
]
