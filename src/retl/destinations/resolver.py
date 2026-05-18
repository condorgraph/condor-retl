from __future__ import annotations

from functools import cache

from retl.declarations import DestinationBinding
from retl.destinations.builtins import builtin_connectors
from retl.destinations.registry import (
    DestinationConnector,
    DestinationRegistry,
    UnknownDestinationSurfaceError,
    entry_point_connectors,
)
from retl.destinations.surfaces import DestinationSurface


@cache
def default_registry() -> DestinationRegistry:
    registry = DestinationRegistry()
    for connector in builtin_connectors():
        registry.register(connector)
    for connector in entry_point_connectors():
        registry.register(connector)
    return registry


def resolve_connector(
    destination_ref: str,
    *,
    registry: DestinationRegistry | None = None,
) -> DestinationConnector:
    active_registry = registry or default_registry()
    return active_registry.resolve(destination_ref)


def resolve_surface(
    binding: DestinationBinding,
    surface_name: str,
) -> DestinationSurface:
    connector = binding.connector
    if isinstance(connector, DestinationConnector):
        try:
            return connector.surface(surface_name)
        except KeyError as exc:
            raise UnknownDestinationSurfaceError(str(exc)) from exc
    surfaces = binding.surfaces
    try:
        surface = surfaces[surface_name]
    except KeyError as exc:
        available = ", ".join(surfaces)
        raise UnknownDestinationSurfaceError(
            f"Destination binding `{binding.binding_name}` does not expose surface "
            f"`{surface_name}`. Available surfaces: {available}."
        ) from exc
    if isinstance(surface, DestinationSurface):
        return surface
    raise TypeError(f"Destination binding `{binding.binding_name}` exposed an invalid surface.")


__all__ = ["default_registry", "resolve_connector", "resolve_surface"]
