"""Tests for TransformationDB InputHash digests."""

from __future__ import annotations

from diracx.core.hashing import hash_json
from diracx.db.sql.transformation.hashing import _input_hash


def test_input_hash_length_and_stability():
    digest = _input_hash("/lhcb/file.dst", {"Run": 1234, "Mask": "1-10"})
    assert len(digest) == 16
    assert digest == _input_hash("/lhcb/file.dst", {"Mask": "1-10", "Run": 1234})

    other = _input_hash("/lhcb/file.dst", {"Run": 1234, "Mask": "11-20"})
    assert digest != other

    null_lfn = _input_hash(None, {"Seed": 42})
    assert len(null_lfn) == 16
    assert null_lfn != _input_hash(None, {"Seed": 43})

    null_mask = _input_hash("/cms/file1.root", None)
    dict_mask = _input_hash("/cms/file1.root", {})
    assert null_mask == dict_mask


def test_input_hash_none_descriptor_matches_empty():
    assert _input_hash("/a", None) == _input_hash("/a", {})
    assert _input_hash(None, None) == _input_hash(None, {})


def test_input_hash_matches_truncated_hash_json_over_envelope():
    lfn = None
    descriptor = {"Seed": 1}
    envelope = {"LFN": lfn, "Descriptor": descriptor}
    expected = hash_json(envelope)[:16]
    assert _input_hash(lfn, descriptor) == expected
    assert len(expected) == 16
