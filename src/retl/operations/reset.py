from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderedWorkDeleteRange:
    first_collect_id: str
    first_sequence_order: int
    last_collect_id: str
    last_sequence_order: int
    family: str = "state"


__all__ = ["OrderedWorkDeleteRange"]
