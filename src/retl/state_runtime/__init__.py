from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

from retl.declarations._specs import State
from retl.declarations._specs import state as build_state
from retl.runtime.staging import StageEvidence, StageWorkPage
from retl.state_runtime.producer import StateCollectEvidence, produce_state_collect
from retl.state_runtime.reconcile import (
    SkippedRemoveEvidence,
    StateOperationPage,
    StateReconcileEvidence,
    reconcile_sync,
)
from retl.state_runtime.staging import stage_declaration, stage_resend_all

__all__ = [
    "SkippedRemoveEvidence",
    "StageEvidence",
    "StageWorkPage",
    "StateCollectEvidence",
    "StateOperationPage",
    "StateReconcileEvidence",
    "produce_state_collect",
    "reconcile_sync",
    "stage_declaration",
    "stage_resend_all",
]


class _CallableStateModule(ModuleType):
    def __call__(self, *args: Any, **kwargs: Any) -> State:
        return build_state(*args, **kwargs)


sys.modules[__name__].__class__ = _CallableStateModule
