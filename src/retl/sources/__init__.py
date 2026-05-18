from __future__ import annotations

from typing import Any

__all__ = [
    "SourceCapabilities",
    "source_identity",
]


def __getattr__(name: str) -> Any:
    if name in {
        "SourceCapabilities",
        "source_identity",
    }:
        from retl.sources import contracts

        return getattr(contracts, name)
    raise AttributeError(f"module `retl.sources` has no attribute `{name}`")
