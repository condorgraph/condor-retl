from __future__ import annotations

from retl.errors import RetlError


class RuntimeStoreError(RetlError):
    """Raised when a runtime store cannot read or write operational state."""


__all__ = ["RuntimeStoreError"]
