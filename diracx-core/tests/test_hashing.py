"""Tests for ``diracx.core.hashing`` digests."""

from __future__ import annotations

from diracx.core.hashing import compute_input_hash


def test_compute_input_hash_is_sixteen_bytes():
    digest = compute_input_hash(None, {"seed": 1})
    assert isinstance(digest, bytes)
    assert len(digest) == 16


def test_compute_input_hash_is_stable():
    first = compute_input_hash("/lhcb/file.dst", {"Run": 1234, "Mask": "1-10"})
    second = compute_input_hash("/lhcb/file.dst", {"Mask": "1-10", "Run": 1234})
    assert first == second


def test_compute_input_hash_distinct_seeds():
    assert compute_input_hash(None, {"seed": 0}) != compute_input_hash(
        None, {"seed": 1}
    )


def test_compute_input_hash_lfn_and_descriptor_both_matter():
    shared_descriptor = {"Run": 1}
    assert compute_input_hash("/a.dst", shared_descriptor) != compute_input_hash(
        "/b.dst", shared_descriptor
    )
    assert compute_input_hash("/a.dst", {"Run": 1}) != compute_input_hash(
        "/a.dst", {"Run": 2}
    )


def test_compute_input_hash_none_vs_empty_descriptor():
    assert compute_input_hash(None, None) != compute_input_hash(None, {})
