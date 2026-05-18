from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from importlib import metadata
from types import MappingProxyType

from retl.auth import AuthMode, JwtSigner, TokenTransport
from retl.destinations.surfaces import (
    DestinationBatchPlanningHook,
    DestinationConnector,
    DestinationConnectorVisibility,
    DestinationManagedTargetClientHook,
    DestinationSubmissionHook,
    DestinationSurface,
)
from retl.errors import DeclarationValidationError, RetlError


class DestinationRegistryError(RetlError):
    """Base class for destination registry failures."""


class UnknownDestinationConnectorError(DestinationRegistryError, LookupError):
    """Raised when a destination connector ref cannot be resolved."""


class UnknownDestinationSurfaceError(DestinationRegistryError, LookupError):
    """Raised when a connector does not expose a requested surface."""


ENTRY_POINT_GROUP = "retl.destinations"


def _validate_ref(value: str, *, field_name: str) -> None:
    if not value.strip():
        raise DeclarationValidationError(f"`{field_name}` must be a non-empty string.")
    if "/" not in value:
        raise DeclarationValidationError(f"`{field_name}` must be a namespaced ref.")


@dataclass
class DestinationRegistry:
    _connectors: dict[str, DestinationConnector] = field(default_factory=dict)
    _aliases: dict[str, str] = field(default_factory=dict)

    @property
    def connectors(self) -> Mapping[str, DestinationConnector]:
        return MappingProxyType(dict(self._connectors))

    def available_connector_refs(self, *, include_internal: bool = False) -> tuple[str, ...]:
        refs = {
            ref
            for ref, connector in self._connectors.items()
            if include_internal or connector.visibility == "public"
        }
        refs.update(
            alias
            for alias, connector_ref in self._aliases.items()
            if include_internal or self._connectors[connector_ref].visibility == "public"
        )
        return tuple(sorted(refs))

    def register(self, connector: DestinationConnector) -> None:
        connector_ref = connector.connector_ref
        _validate_ref(connector_ref, field_name="connector ref")
        if connector_ref in self._connectors:
            raise DeclarationValidationError(
                f"Destination connector `{connector_ref}` is already registered."
            )
        for alias in connector.aliases:
            _validate_ref(alias, field_name="connector alias")
            if alias in self._connectors or alias in self._aliases:
                raise DeclarationValidationError(
                    f"Destination connector alias `{alias}` is already registered."
                )
        self._connectors[connector_ref] = connector
        for alias in connector.aliases:
            self._aliases[alias] = connector_ref

    def resolve(self, destination_ref: str) -> DestinationConnector:
        ref = destination_ref.strip()
        if not ref:
            raise DeclarationValidationError("`destination_ref` must be a non-empty string.")
        canonical_ref = self._aliases.get(ref, ref)
        try:
            return self._connectors[canonical_ref]
        except KeyError as exc:
            available_refs = self.available_connector_refs()
            available = (
                f" Available connectors: {', '.join(available_refs)}."
                if available_refs
                else " No public destination connectors are registered."
            )
            raise UnknownDestinationConnectorError(
                f"Unknown destination connector `{destination_ref}`.{available}"
            ) from exc

    def surface(self, destination_ref: str, surface_name: str) -> DestinationSurface:
        return self.resolve(destination_ref).surface(surface_name)


def declarative_connector(
    *,
    ref: str,
    display_name: str | None = None,
    surfaces: Sequence[DestinationSurface],
    aliases: Sequence[str] = (),
    auth_modes: Sequence[AuthMode] | None = None,
    visibility: DestinationConnectorVisibility = "public",
    config_namespace_fields: Sequence[str] = (),
    auth_token_transport: TokenTransport | None = None,
    auth_jwt_signer: JwtSigner | None = None,
    batch_planning_hook: DestinationBatchPlanningHook | None = None,
    submission_hook: DestinationSubmissionHook | None = None,
    managed_target_client_hook: DestinationManagedTargetClientHook | None = None,
) -> DestinationConnector:
    return DestinationConnector(
        name=display_name or ref,
        ref=ref,
        display_name=display_name or ref,
        surfaces=surfaces,
        aliases=aliases,
        visibility=visibility,
        auth_modes=auth_modes or (),
        config_namespace_fields=config_namespace_fields,
        auth_token_transport=auth_token_transport,
        auth_jwt_signer=auth_jwt_signer,
        batch_planning_hook=batch_planning_hook,
        submission_hook=submission_hook,
        managed_target_client_hook=managed_target_client_hook,
    )


def entry_point_connectors() -> tuple[DestinationConnector, ...]:
    connectors: list[DestinationConnector] = []
    try:
        entry_points = metadata.entry_points(group=ENTRY_POINT_GROUP)
    except TypeError:
        entry_points = metadata.entry_points().select(group=ENTRY_POINT_GROUP)
    for entry_point in entry_points:
        try:
            loaded = entry_point.load()
        except ModuleNotFoundError:
            continue
        connector = (
            loaded()
            if callable(loaded) and not isinstance(loaded, DestinationConnector)
            else loaded
        )
        if not isinstance(connector, DestinationConnector):
            raise DestinationRegistryError(
                f"Destination entry point `{entry_point.name}` must load a DestinationConnector "
                "or a callable returning one."
            )
        connectors.append(connector)
    return tuple(connectors)


__all__ = [
    "DestinationConnector",
    "DestinationRegistry",
    "DestinationRegistryError",
    "ENTRY_POINT_GROUP",
    "UnknownDestinationConnectorError",
    "UnknownDestinationSurfaceError",
    "declarative_connector",
    "entry_point_connectors",
]
