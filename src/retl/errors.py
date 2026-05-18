from __future__ import annotations


class RetlError(Exception):
    """Base class for public RETL errors."""


class DeclarationValidationError(RetlError, ValueError):
    """Raised when a public declaration cannot be constructed."""


class RetlRuntimeNotImplementedError(RetlError, NotImplementedError):
    """Raised by public execution stubs until runtime phases exist."""


__all__ = [
    "DeclarationValidationError",
    "RetlError",
    "RetlRuntimeNotImplementedError",
]
