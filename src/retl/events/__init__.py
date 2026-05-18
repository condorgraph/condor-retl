from __future__ import annotations

from typing import Any

from retl.events.producer import (
    EventCollectEvidence,
    produce_event_collect,
)

_LAZY_EXPORTS = {
    "EventImportPage": "retl.events.reconcile",
    "EventReconcileEvidence": "retl.events.reconcile",
    "EventReconcilePageEvidence": "retl.runtime.reconcile",
    "StageEvidence": "retl.runtime.staging",
    "StageWorkPage": "retl.runtime.staging",
    "reconcile_event_imports": "retl.events.reconcile",
    "stage_event_declaration": "retl.events.staging",
}


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


__all__ = [
    "EventCollectEvidence",
    "EventImportPage",
    "EventReconcileEvidence",
    "EventReconcilePageEvidence",
    "StageEvidence",
    "StageWorkPage",
    "produce_event_collect",
    "reconcile_event_imports",
    "stage_event_declaration",
]
