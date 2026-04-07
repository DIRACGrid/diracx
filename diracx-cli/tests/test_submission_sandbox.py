"""Tests for diracx.cli._submission.sandbox module."""

from __future__ import annotations

from pathlib import Path

from diracx.cli._submission.sandbox import (
    group_jobs_by_sandbox,
    rewrite_sandbox_refs,
    scan_file_references,
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
    inputs = {"infile": {"class": "File", "path": "LFN:/grid/path/file.root"}}
    local_files, lfns = scan_file_references(inputs)
    assert local_files == []
    assert lfns == ["LFN:/grid/path/file.root"]


def test_scan_sb_ignored() -> None:
    inputs = {"infile": {"class": "File", "path": "SB:some-pfn/file.txt"}}
    local_files, lfns = scan_file_references(inputs)
    assert local_files == []
    assert lfns == []


def test_scan_file_array_mixed_local_and_lfn() -> None:
    inputs = {
        "files": [
            {"class": "File", "path": "/local/a.txt"},
            {"class": "File", "path": "LFN:/grid/b.root"},
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
            {"class": "File", "path": "LFN:/grid/remote.root"},
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
        {"f": {"class": "File", "path": "LFN:/grid/a.root"}},
        {"count": 5},
    ]
    groups = group_jobs_by_sandbox(jobs)
    assert groups == []


def test_group_mixed_local_and_lfn_lfns_dont_affect_grouping() -> None:
    # Both jobs have the same local file but different LFNs — should be 1 group.
    jobs = [
        {
            "local": {"class": "File", "path": "/data/shared.txt"},
            "remote": {"class": "File", "path": "LFN:/grid/a.root"},
        },
        {
            "local": {"class": "File", "path": "/data/shared.txt"},
            "remote": {"class": "File", "path": "LFN:/grid/b.root"},
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
    pfn_map = {Path("/local/file.txt"): "SB:abc123/file.txt"}
    result = rewrite_sandbox_refs(inputs, pfn_map)
    assert result == {"infile": {"class": "File", "path": "SB:abc123/file.txt"}}


def test_rewrite_lfn_not_rewritten() -> None:
    inputs = {"infile": {"class": "File", "path": "LFN:/grid/file.root"}}
    pfn_map: dict[Path, str] = {}
    result = rewrite_sandbox_refs(inputs, pfn_map)
    assert result == {"infile": {"class": "File", "path": "LFN:/grid/file.root"}}


def test_rewrite_array_mixed() -> None:
    inputs = {
        "files": [
            {"class": "File", "path": "/local/a.txt"},
            {"class": "File", "path": "LFN:/grid/b.root"},
            {"class": "File", "path": "/local/c.txt"},
        ]
    }
    pfn_map = {
        Path("/local/a.txt"): "SB:pfn1/a.txt",
        Path("/local/c.txt"): "SB:pfn2/c.txt",
    }
    result = rewrite_sandbox_refs(inputs, pfn_map)
    assert result == {
        "files": [
            {"class": "File", "path": "SB:pfn1/a.txt"},
            {"class": "File", "path": "LFN:/grid/b.root"},
            {"class": "File", "path": "SB:pfn2/c.txt"},
        ]
    }


def test_rewrite_non_file_values_preserved() -> None:
    inputs = {
        "count": 42,
        "name": "hello",
        "infile": {"class": "File", "path": "/local/file.txt"},
    }
    pfn_map = {Path("/local/file.txt"): "SB:abc/file.txt"}
    result = rewrite_sandbox_refs(inputs, pfn_map)
    assert result["count"] == 42
    assert result["name"] == "hello"
    assert result["infile"] == {"class": "File", "path": "SB:abc/file.txt"}


def test_rewrite_does_not_mutate_input() -> None:
    original = {"infile": {"class": "File", "path": "/local/file.txt"}}
    pfn_map = {Path("/local/file.txt"): "SB:abc/file.txt"}
    result = rewrite_sandbox_refs(original, pfn_map)
    # Original must be unchanged
    assert original["infile"]["path"] == "/local/file.txt"
    assert result["infile"]["path"] == "SB:abc/file.txt"
