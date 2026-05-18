from __future__ import annotations

from retl.destinations.builtins.mock import mock_connector
from retl.destinations.builtins.reference import reference_connector
from retl.destinations.registry import DestinationConnector


def builtin_connectors() -> tuple[DestinationConnector, ...]:
    return (mock_connector(), reference_connector())


__all__ = ["builtin_connectors", "mock_connector", "reference_connector"]
