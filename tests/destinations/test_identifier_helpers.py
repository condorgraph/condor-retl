from __future__ import annotations

from retl.destinations.identifiers import (
    hash_or_preserve_sha256_hex,
    is_sha256_hex,
    sha256_hex,
)


def test_sha256_hex_detection_requires_exact_64_character_hex_string() -> None:
    assert is_sha256_hex("a" * 64)
    assert is_sha256_hex("A" * 64)
    assert not is_sha256_hex("a" * 63)
    assert not is_sha256_hex("g" * 64)
    assert not is_sha256_hex(f" {'a' * 64} ")


def test_sha256_hex_hashes_utf8_values_deterministically() -> None:
    assert sha256_hex("customer@example.test") == (
        "06c3645baad7d2fd6661e4dce43692e8b0fc79133fbd1582bad9235e7ea668da"
    )


def test_hash_or_preserve_sha256_hex_preserves_lowercase_hashes() -> None:
    hashed = "a" * 64

    assert hash_or_preserve_sha256_hex(hashed) == hashed


def test_hash_or_preserve_sha256_hex_lowercases_uppercase_hashes() -> None:
    assert hash_or_preserve_sha256_hex("A" * 64) == "a" * 64


def test_hash_or_preserve_sha256_hex_hashes_wrong_length_and_non_hex_values() -> None:
    assert hash_or_preserve_sha256_hex("a" * 63) == sha256_hex("a" * 63)
    assert hash_or_preserve_sha256_hex("g" * 64) == sha256_hex("g" * 64)


def test_hash_or_preserve_sha256_hex_normalizes_before_hashing() -> None:
    rendered = hash_or_preserve_sha256_hex(
        "  Customer@Example.Test  ",
        normalizer=lambda value: value.lower(),
    )

    assert rendered == sha256_hex("customer@example.test")
