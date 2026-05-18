from __future__ import annotations

import hashlib
import re
from collections.abc import Callable

_SHA256_HEX = re.compile(r"^[0-9a-fA-F]{64}$")


def is_sha256_hex(value: str) -> bool:
    return _SHA256_HEX.fullmatch(value) is not None


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def hash_or_preserve_sha256_hex(
    value: str,
    *,
    normalizer: Callable[[str], str] | None = None,
) -> str:
    stripped = value.strip()
    if is_sha256_hex(stripped):
        return stripped.lower()
    normalized = normalizer(stripped) if normalizer is not None else stripped
    return sha256_hex(normalized)


__all__ = [
    "hash_or_preserve_sha256_hex",
    "is_sha256_hex",
    "sha256_hex",
]
