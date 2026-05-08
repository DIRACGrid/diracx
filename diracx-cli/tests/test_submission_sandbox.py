"""Tests for diracx.cli._submission.sandbox module."""

from __future__ import annotations

from pathlib import Path

import pytest

from diracx.cli._submission.sandbox import (
    group_jobs_by_sandbox,
    rewrite_sandbox_refs,
    scan_file_references,
    validate_file_references,
)

# ---------------------------------------------------------------------------
# scan_file_references
# ---------------------------------------------------------------------------


def test_scan_single_local_file() -> None:
    inputs = {"infile": {"class": "File", "path": "/data/input.txt"}}
    local_files, lfns = scan_file_references(inputs)
    assert local_files == [Path("/data/input.txt")]
    assert lfns == []


def test_scan_lfn_passthrough() -> None:
    inputs = {"infile": {"class": "File", "location": "LFN:/grid/path/file.root"}}
    local_files, lfns = scan_file_references(inputs)
    assert local_files == []
    assert lfns == ["LFN:/grid/path/file.root"]


def test_scan_sb_ignored() -> None:
    inputs = {"infile": {"class": "File", "location": "SB:some-pfn/file.txt"}}
    local_files, lfns = scan_file_references(inputs)
    assert local_files == []
    assert lfns == []


def test_scan_file_array_mixed_local_and_lfn() -> None:
    inputs = {
        "files": [
            {"class": "File", "path": "/local/a.txt"},
            {"class": "File", "location": "LFN:/grid/b.root"},
            {"class": "File", "path": "/local/c.txt"},
        ]
    }
    local_files, lfns = scan_file_references(inputs)
    assert sorted(local_files) == [Path("/local/a.txt"), Path("/local/c.txt")]
    assert lfns == ["LFN:/grid/b.root"]


def test_scan_no_files_scalars_only() -> None:
    inputs = {"count": 42, "name": "hello", "flag": True}
    local_files, lfns = scan_file_references(inputs)
    assert local_files == []
    assert lfns == []


def test_scan_nested_mixed() -> None:
    inputs = {
        "scalar": 99,
        "single": {"class": "File", "path": "/local/file.txt"},
        "multi": [
            {"class": "File", "location": "LFN:/grid/remote.root"},
        ],
    }
    local_files, lfns = scan_file_references(inputs)
    assert local_files == [Path("/local/file.txt")]
    assert lfns == ["LFN:/grid/remote.root"]


# ---------------------------------------------------------------------------
# group_jobs_by_sandbox
# ---------------------------------------------------------------------------


def test_group_all_same_files() -> None:
    jobs = [
        {"f": {"class": "File", "path": "/data/a.txt"}},
        {"f": {"class": "File", "path": "/data/a.txt"}},
        {"f": {"class": "File", "path": "/data/a.txt"}},
    ]
    groups = group_jobs_by_sandbox(jobs)
    assert len(groups) == 1
    file_set, indices = groups[0]
    assert file_set == frozenset([Path("/data/a.txt")])
    assert sorted(indices) == [0, 1, 2]


def test_group_different_files_per_job() -> None:
    jobs = [
        {"f": {"class": "File", "path": "/data/a.txt"}},
        {"f": {"class": "File", "path": "/data/b.txt"}},
        {"f": {"class": "File", "path": "/data/c.txt"}},
    ]
    groups = group_jobs_by_sandbox(jobs)
    assert len(groups) == 3
    all_indices = sorted(idx for _, indices in groups for idx in indices)
    assert all_indices == [0, 1, 2]


def test_group_no_local_files_returns_empty() -> None:
    jobs = [
        {"f": {"class": "File", "location": "LFN:/grid/a.root"}},
        {"count": 5},
    ]
    groups = group_jobs_by_sandbox(jobs)
    assert groups == []


def test_group_mixed_local_and_lfn_lfns_dont_affect_grouping() -> None:
    # Both jobs have the same local file but different LFNs — should be 1 group.
    jobs = [
        {
            "local": {"class": "File", "path": "/data/shared.txt"},
            "remote": {"class": "File", "location": "LFN:/grid/a.root"},
        },
        {
            "local": {"class": "File", "path": "/data/shared.txt"},
            "remote": {"class": "File", "location": "LFN:/grid/b.root"},
        },
    ]
    groups = group_jobs_by_sandbox(jobs)
    assert len(groups) == 1
    file_set, indices = groups[0]
    assert file_set == frozenset([Path("/data/shared.txt")])
    assert sorted(indices) == [0, 1]


# ---------------------------------------------------------------------------
# rewrite_sandbox_refs
# ---------------------------------------------------------------------------


def test_rewrite_single_file() -> None:
    inputs = {"infile": {"class": "File", "path": "/local/file.txt"}}
    sb_ref_map = {
        Path("/local/file.txt"): "SB:SandboxSE|/S3/store/sha256:abc123.tar.zst"
    }
    result = rewrite_sandbox_refs(inputs, sb_ref_map)
    assert result == {
        "infile": {
            "class": "File",
            "location": "SB:SandboxSE|/S3/store/sha256:abc123.tar.zst#file.txt",
        }
    }


def test_rewrite_lfn_not_rewritten() -> None:
    inputs = {"infile": {"class": "File", "location": "LFN:/grid/file.root"}}
    sb_ref_map: dict[Path, str] = {}
    result = rewrite_sandbox_refs(inputs, sb_ref_map)
    assert result == {"infile": {"class": "File", "location": "LFN:/grid/file.root"}}


def test_rewrite_array_mixed() -> None:
    inputs = {
        "files": [
            {"class": "File", "path": "/local/a.txt"},
            {"class": "File", "location": "LFN:/grid/b.root"},
            {"class": "File", "path": "/local/c.txt"},
        ]
    }
    sb_ref_map = {
        Path("/local/a.txt"): "SB:SandboxSE|/S3/store/sha256:aaa.tar.zst",
        Path("/local/c.txt"): "SB:SandboxSE|/S3/store/sha256:ccc.tar.zst",
    }
    result = rewrite_sandbox_refs(inputs, sb_ref_map)
    assert result == {
        "files": [
            {
                "class": "File",
                "location": "SB:SandboxSE|/S3/store/sha256:aaa.tar.zst#a.txt",
            },
            {"class": "File", "location": "LFN:/grid/b.root"},
            {
                "class": "File",
                "location": "SB:SandboxSE|/S3/store/sha256:ccc.tar.zst#c.txt",
            },
        ]
    }


def test_rewrite_sb_not_rewritten() -> None:
    inputs = {"infile": {"class": "File", "location": "SB:some-pfn/file.txt"}}
    sb_ref_map: dict[Path, str] = {}
    result = rewrite_sandbox_refs(inputs, sb_ref_map)
    assert result == {"infile": {"class": "File", "location": "SB:some-pfn/file.txt"}}


def test_rewrite_non_file_values_preserved() -> None:
    inputs = {
        "count": 42,
        "name": "hello",
        "infile": {"class": "File", "path": "/local/file.txt"},
    }
    sb_ref_map = {Path("/local/file.txt"): "SB:SandboxSE|/S3/store/sha256:abc.tar.zst"}
    result = rewrite_sandbox_refs(inputs, sb_ref_map)
    assert result["count"] == 42
    assert result["name"] == "hello"
    assert result["infile"] == {
        "class": "File",
        "location": "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#file.txt",
    }


def test_rewrite_does_not_mutate_input() -> None:
    original = {"infile": {"class": "File", "path": "/local/file.txt"}}
    sb_ref_map = {Path("/local/file.txt"): "SB:SandboxSE|/S3/store/sha256:abc.tar.zst"}
    result = rewrite_sandbox_refs(original, sb_ref_map)
    # Original must be unchanged
    assert original["infile"]["path"] == "/local/file.txt"
    assert (
        result["infile"]["location"]
        == "SB:SandboxSE|/S3/store/sha256:abc.tar.zst#file.txt"
    )
    assert "path" not in result["infile"]


# ---------------------------------------------------------------------------
# validate_file_references
# ---------------------------------------------------------------------------


def test_validate_accepts_local_path() -> None:
    inputs = {"infile": {"class": "File", "path": "/local/file.txt"}}
    validate_file_references(inputs)  # no error


def test_validate_accepts_lfn_in_location() -> None:
    inputs = {"infile": {"class": "File", "location": "LFN:/grid/file.root"}}
    validate_file_references(inputs)  # no error


def test_validate_accepts_sb_in_location() -> None:
    inputs = {"infile": {"class": "File", "location": "SB:SE|/S3/abc#run.sh"}}
    validate_file_references(inputs)  # no error


def test_validate_rejects_lfn_in_path() -> None:
    inputs = {"data": {"class": "File", "path": "LFN:/grid/file.root"}}
    with pytest.raises(ValueError, match="Use 'location'"):
        validate_file_references(inputs)


def test_validate_rejects_sb_in_path() -> None:
    inputs = {"script": {"class": "File", "path": "SB:SE|/S3/abc#run.sh"}}
    with pytest.raises(ValueError, match="Use 'location'"):
        validate_file_references(inputs)


def test_validate_accepts_mixed_correct_usage() -> None:
    inputs = {
        "local": {"class": "File", "path": "/data/file.txt"},
        "remote": {"class": "File", "location": "LFN:/grid/file.root"},
        "count": 42,
    }
    validate_file_references(inputs)  # no error


def test_validate_rejects_lfn_in_array() -> None:
    inputs = {
        "files": [
            {"class": "File", "location": "LFN:/grid/a.root"},
            {"class": "File", "path": "LFN:/grid/b.root"},
        ]
    }
    with pytest.raises(ValueError, match="Use 'location'"):
        validate_file_references(inputs)
