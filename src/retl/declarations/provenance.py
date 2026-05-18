from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Literal

from retl.declarations._specs import Event, SecretLiteral, SecretRef, Source, State, StaticTarget
from retl.errors import DeclarationValidationError

DeclarationKind = Literal["state", "event"]


@dataclass(frozen=True)
class DeclarationMetadata:
    declaration_version_id: str
    declaration_name: str
    declaration_kind: DeclarationKind
    source_name: str
    source_backend: str | None
    source_location_json: str
    source_query_hash: str
    declaration_json: str


def declaration_metadata(
    declaration: State | Event,
    *,
    source_backend: str | None = None,
    source_location: Mapping[str, Any] | None = None,
) -> DeclarationMetadata:
    source_location_json = _canonical_json(_sanitize(source_location or {}))
    canonical = dict(canonical_declaration(declaration))
    canonical_source = dict(canonical["source"])
    canonical_source["location"] = _sanitize(source_location or {})
    canonical["source"] = canonical_source
    declaration_json = _canonical_json(canonical)
    source = declaration.source
    return DeclarationMetadata(
        declaration_version_id=_sha256(declaration_json),
        declaration_name=declaration.name,
        declaration_kind="state" if isinstance(declaration, State) else "event",
        source_name=source.name,
        source_backend=source_backend or _source_backend_name(source),
        source_location_json=source_location_json,
        source_query_hash=_sha256(source.query),
        declaration_json=declaration_json,
    )


def canonical_declaration(declaration: State | Event) -> Mapping[str, Any]:
    source = declaration.source
    base: dict[str, Any] = {
        "kind": "state" if isinstance(declaration, State) else "event",
        "name": declaration.name,
        "source": {
            "name": source.name,
            "mode": source.mode,
            "query_hash": _sha256(source.query),
            "checkpoint": _sanitize(source.checkpoint),
            "backend": _source_backend_name(source),
        },
        "key": _sanitize(declaration.key),
        "identifiers": _sanitize(tuple(declaration.identifiers)),
        "payload": _sanitize(declaration.payload),
    }
    if isinstance(declaration, State):
        base["target"] = canonical_state_target(declaration.target)
    else:
        base["occurred_at"] = declaration.occurred_at
    return base


def canonical_state_target(value: object) -> object:
    if isinstance(value, StaticTarget):
        return {"kind": "static", "value": value.value}
    return value


def _source_backend_name(source: Source) -> str | None:
    backend = source.backend
    if backend is None:
        return None
    name = getattr(backend, "backend", None) or getattr(backend, "name", None)
    if isinstance(name, str) and name.strip():
        return name
    return type(backend).__name__


def _sanitize(value: object) -> object:
    if isinstance(value, SecretRef):
        return {"secret_ref": value.name}
    if isinstance(value, SecretLiteral):
        return {"secret_literal": "[redacted]"}
    if isinstance(value, Mapping):
        return {str(key): _sanitize(item) for key, item in sorted(value.items())}
    if isinstance(value, tuple | list):
        return [_sanitize(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date | time):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            _sanitize(value),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    except (TypeError, ValueError) as exc:
        raise DeclarationValidationError("Declaration metadata must be JSON serializable.") from exc


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


__all__ = [
    "DeclarationMetadata",
    "canonical_declaration",
    "canonical_state_target",
    "declaration_metadata",
]
