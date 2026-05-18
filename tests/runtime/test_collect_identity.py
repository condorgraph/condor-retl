from __future__ import annotations

import uuid

import pytest

import retl.collect_identity as collect_identity
from retl.collect_identity import is_uuidv7, new_collect_id, uuidv7_from_unix_ms


def _reset_monotonic_state() -> None:
    with collect_identity._COLLECT_ID_LOCK:  # noqa: SLF001
        collect_identity._last_collect_unix_ms = -1  # noqa: SLF001
        collect_identity._last_collect_payload = -1  # noqa: SLF001


def _uuid_unix_ms(value: str) -> int:
    return uuid.UUID(value).int >> 80


def test_new_collect_id_sorts_in_generation_order_within_one_millisecond(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_monotonic_state()
    unix_ms = 1_700_000_000_123
    monkeypatch.setattr(collect_identity.time, "time_ns", lambda: unix_ms * 1_000_000)
    monkeypatch.setattr(collect_identity.secrets, "randbits", lambda bits: 41)

    values = [new_collect_id() for _ in range(8)]

    assert all(is_uuidv7(value) for value in values)
    assert len(set(values)) == len(values)
    assert values == sorted(values)
    assert [_uuid_unix_ms(value) for value in values] == [unix_ms] * len(values)


def test_new_collect_id_stays_monotonic_when_process_clock_regresses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_monotonic_state()
    unix_ms_values = iter([1_700_000_000_200, 1_700_000_000_199, 1_700_000_000_199])
    monkeypatch.setattr(
        collect_identity.time,
        "time_ns",
        lambda: next(unix_ms_values) * 1_000_000,
    )
    monkeypatch.setattr(collect_identity.secrets, "randbits", lambda bits: 99)

    values = [new_collect_id() for _ in range(3)]

    assert values == sorted(values)
    assert [_uuid_unix_ms(value) for value in values] == [1_700_000_000_200] * 3


def test_uuidv7_from_unix_ms_sorts_lexically_by_timestamp() -> None:
    first = uuidv7_from_unix_ms(1)
    second = uuidv7_from_unix_ms(2)
    third = uuidv7_from_unix_ms(3)

    assert sorted((third, first, second)) == [first, second, third]
