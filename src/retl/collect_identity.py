from __future__ import annotations

import secrets
import threading
import time
import uuid

_UUIDV7_TIMESTAMP_BITS = 48
_UUIDV7_PAYLOAD_BITS = 74
_UUIDV7_MAX_TIMESTAMP = 1 << _UUIDV7_TIMESTAMP_BITS
_UUIDV7_MAX_PAYLOAD = 1 << _UUIDV7_PAYLOAD_BITS

_COLLECT_ID_LOCK = threading.Lock()
_last_collect_unix_ms = -1
_last_collect_payload = -1


def new_collect_id() -> str:
    """Return a backend-neutral, process-local monotonic UUIDv7 collect identity."""

    unix_ms = time.time_ns() // 1_000_000
    global _last_collect_unix_ms, _last_collect_payload

    with _COLLECT_ID_LOCK:
        if unix_ms > _last_collect_unix_ms:
            collect_unix_ms = unix_ms
            collect_payload = secrets.randbits(_UUIDV7_PAYLOAD_BITS)
        else:
            collect_unix_ms = _last_collect_unix_ms
            collect_payload = _last_collect_payload + 1
            if collect_payload >= _UUIDV7_MAX_PAYLOAD:
                if collect_unix_ms + 1 >= _UUIDV7_MAX_TIMESTAMP:
                    raise OverflowError("UUIDv7 monotonic collect identity space is exhausted.")
                collect_unix_ms += 1
                collect_payload = 0

        _last_collect_unix_ms = collect_unix_ms
        _last_collect_payload = collect_payload

    return _uuidv7_from_unix_ms_and_payload(collect_unix_ms, collect_payload)


def uuidv7_from_unix_ms(unix_ms: int) -> str:
    if not isinstance(unix_ms, int) or isinstance(unix_ms, bool) or unix_ms < 0:
        raise ValueError("UUIDv7 timestamp must be a nonnegative integer millisecond value.")
    if unix_ms >= _UUIDV7_MAX_TIMESTAMP:
        raise ValueError("UUIDv7 timestamp must fit in 48 bits.")

    return _uuidv7_from_unix_ms_and_payload(
        unix_ms,
        secrets.randbits(_UUIDV7_PAYLOAD_BITS),
    )


def _uuidv7_from_unix_ms_and_payload(unix_ms: int, payload: int) -> str:
    rand_a = payload >> 62
    rand_b = payload & ((1 << 62) - 1)
    value = (unix_ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    return str(uuid.UUID(int=value))


def is_uuidv7(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        return False
    return str(parsed) == value.lower() and parsed.version == 7


__all__ = [
    "is_uuidv7",
    "new_collect_id",
    "uuidv7_from_unix_ms",
]
