from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from retl.declarations import Source


def source_identity(source: Source) -> str:
    payload: dict[str, object] = {
        "name": source.name,
        "mode": source.mode,
        "query": source.query,
        "checkpoint": _plain_mapping(source.checkpoint),
        "backend": _backend_identity(source.backend),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _plain_mapping(value: Mapping[str, object] | None) -> dict[str, object] | None:
    if value is None:
        return None
    return dict(value)


def _backend_identity(backend: object | None) -> object:
    if backend is None:
        return None
    identity = getattr(backend, "sanitized_identity", None)
    if identity is None:
        identity = getattr(backend, "identity", None)
    if callable(identity):
        identity = identity()
    if isinstance(identity, Mapping):
        return {
            str(key): value
            for key, value in sorted(identity.items(), key=lambda item: str(item[0]))
            if _identity_value_is_safe(value)
        }
    if isinstance(identity, str):
        return identity
    backend_name = getattr(backend, "name", None)
    if isinstance(backend_name, str) and backend_name.strip():
        return backend_name
    return backend.__class__.__qualname__


def _identity_value_is_safe(value: object) -> bool:
    return value is None or isinstance(value, str | int | float | bool)


__all__ = [
    "source_identity",
]
