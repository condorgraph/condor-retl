from __future__ import annotations

from dataclasses import dataclass

from retl.stores.contracts import (
    DestinationScanRange,
    StateOrderedWorkScanPosition,
)


@dataclass(frozen=True)
class OrderedWorkRange:
    first_collect_id: str
    first_sequence_order: int
    last_collect_id: str
    last_sequence_order: int


def ordered_work_range(
    *,
    first_collect_id: str,
    first_sequence_order: int,
    last_collect_id: str,
    last_sequence_order: int,
) -> DestinationScanRange:
    first = StateOrderedWorkScanPosition(
        collect_id=first_collect_id,
        sequence_order=first_sequence_order,
    )
    last = StateOrderedWorkScanPosition(
        collect_id=last_collect_id,
        sequence_order=last_sequence_order,
    )
    return DestinationScanRange(
        first_record_position=first,
        last_record_position=last,
        upper_bound_inclusive=last,
        lower_bound_exclusive=None,
    )


def to_ordered_work_scan_range(
    value: OrderedWorkRange | DestinationScanRange,
) -> DestinationScanRange:
    if isinstance(value, DestinationScanRange):
        return value
    if isinstance(value, OrderedWorkRange):
        return ordered_work_range(
            first_collect_id=value.first_collect_id,
            first_sequence_order=value.first_sequence_order,
            last_collect_id=value.last_collect_id,
            last_sequence_order=value.last_sequence_order,
        )
    raise TypeError("ordered work skip requires OrderedWorkRange or DestinationScanRange.")


__all__ = [
    "OrderedWorkRange",
    "ordered_work_range",
    "to_ordered_work_scan_range",
]
