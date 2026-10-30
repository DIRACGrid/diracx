"""Tests for diracx.core.hashing."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest
import xxhash

from diracx.core.hashing import canonical_json_bytes, hash_json, input_hash


def test_canonical_json_bytes_key_order_independent():
    left = {"b": 1, "a": {"z": True, "y": None}}
    right = {"a": {"y": None, "z": True}, "b": 1}
    assert canonical_json_bytes(left) == canonical_json_bytes(right)
    assert canonical_json_bytes(left) == b'{"a":{"y":null,"z":true},"b":1}'


def test_canonical_json_bytes_unicode():
    assert canonical_json_bytes({"café": "naïve"}) == '{"café":"naïve"}'.encode()


def test_canonical_json_bytes_finite_float():
    assert canonical_json_bytes({"x": 1.5}) == b'{"x":1.5}'
    assert canonical_json_bytes({"x": 1 / 3}) == b'{"x":0.3333333333333333}'


def test_canonical_json_bytes_int64_bounds():
    assert canonical_json_bytes({"x": (1 << 63) - 1}) == (b'{"x":9223372036854775807}')
    assert canonical_json_bytes({"x": -(1 << 63)}) == (b'{"x":-9223372036854775808}')
    with pytest.raises(ValueError, match="64-bit"):
        canonical_json_bytes({"x": 1 << 63})
    with pytest.raises(ValueError, match="64-bit"):
        canonical_json_bytes({"x": -(1 << 63) - 1})
    with pytest.raises(ValueError, match="64-bit"):
        canonical_json_bytes({"x": 2**100})


def test_canonical_json_bytes_rejects_non_json_types():
    with pytest.raises(TypeError, match="datetime"):
        canonical_json_bytes({"t": datetime.now(tz=timezone.utc)})
    with pytest.raises(TypeError, match="set"):
        canonical_json_bytes({1, 2})
    with pytest.raises(TypeError, match="Dict keys must be str"):
        canonical_json_bytes({1: "a"})
    with pytest.raises(ValueError, match="Non-finite"):
        canonical_json_bytes({"x": float("nan")})
    with pytest.raises(TypeError, match="tuple"):
        canonical_json_bytes(("a", "b"))
    with pytest.raises(TypeError, match="bool"):
        canonical_json_bytes({True: "a"})


def test_hash_json_known_vector():
    # Empty object — SHA-256 of b"{}"
    assert hash_json({}) == hashlib.sha256(b"{}").digest()
    assert hash_json({}).hex() == (
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    )
    value = {"Priority": 1, "Sites": ["LCG.CERN.ch"]}
    digest = hash_json(value)
    assert len(digest) == 32
    assert digest == hashlib.sha256(canonical_json_bytes(value)).digest()
    assert digest.hex() == (
        "d150b0d7fd1ff9d4c3436b9280dc743b3d42d4639f3540e34bbabc74035a1d80"
    )


def test_hash_json_stable_for_requirements_shape():
    requirements = {"CPUTime": 86400, "Sites": ["LCG.CERN.ch", "LCG.IN2P3.fr"]}
    hints = {"dirac:Backend": "Pilot"}
    process = {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "baseCommand": ["echo"],
    }
    for value in (requirements, hints, process):
        assert len(hash_json(value)) == 32
        assert hash_json(value) == hash_json(
            {k: value[k] for k in sorted(value, reverse=True)}
        )


def test_input_hash_length_and_stability():
    digest = input_hash("/lhcb/file.dst", {"Run": 1234, "Mask": "1-10"})
    assert len(digest) == 16
    assert digest == input_hash("/lhcb/file.dst", {"Mask": "1-10", "Run": 1234})

    other = input_hash("/lhcb/file.dst", {"Run": 1234, "Mask": "11-20"})
    assert digest != other

    null_lfn = input_hash(None, {"Seed": 42})
    assert len(null_lfn) == 16
    assert null_lfn != input_hash(None, {"Seed": 43})


def test_input_hash_matches_xxh3_over_canonical_envelope():
    lfn = None
    descriptor = {"Seed": 1}
    envelope = {"LFN": lfn, "Descriptor": descriptor}
    expected = xxhash.xxh3_128_digest(canonical_json_bytes(envelope))
    assert input_hash(lfn, descriptor) == expected
