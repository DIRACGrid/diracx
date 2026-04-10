"""Sandbox scanning, grouping, and path rewriting for CWL job submission."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _is_file_obj(value: Any) -> bool:
    """Return True if value is a CWL File object dict."""
    return isinstance(value, dict) and value.get("class") == "File"


def _classify_path(path_str: str) -> tuple[Path | None, str | None]:
    """Classify a CWL File path string.

    Returns (local_path, lfn) where exactly one is set, or both None for SB: paths.
    """
    if path_str.startswith("LFN:"):
        return None, path_str
    if path_str.startswith("SB:"):
        return None, None
    return Path(path_str), None


def scan_file_references(inputs: dict) -> tuple[list[Path], list[str]]:
    """Walk an input dict and classify CWL File references.

    Returns:
        local_files: paths to local files (no LFN: or SB: prefix)
        lfns: LFN: prefixed path strings
    SB: paths are silently ignored (already uploaded).
    Non-File values are skipped.

    """
    local_files: list[Path] = []
    lfns: list[str] = []

    for value in inputs.values():
        if _is_file_obj(value):
            path_str: str = value["path"]
            local, lfn = _classify_path(path_str)
            if local is not None:
                local_files.append(local)
            elif lfn is not None:
                lfns.append(lfn)
        elif isinstance(value, list):
            for item in value:
                if _is_file_obj(item):
                    path_str = item["path"]
                    local, lfn = _classify_path(path_str)
                    if local is not None:
                        local_files.append(local)
                    elif lfn is not None:
                        lfns.append(lfn)

    return local_files, lfns


def group_jobs_by_sandbox(
    jobs: list[dict],
) -> list[tuple[frozenset[Path], list[int]]]:
    """Group jobs by their unique set of local files.

    Jobs with no local files are excluded from all groups.

    Returns:
        List of (file_set, job_indices) tuples, one per unique file set.

    """
    groups: dict[frozenset[Path], list[int]] = {}

    for idx, job in enumerate(jobs):
        local_files, _ = scan_file_references(job)
        if not local_files:
            continue
        key = frozenset(local_files)
        groups.setdefault(key, []).append(idx)

    return list(groups.items())


def _rewrite_file_obj(file_obj: dict, sb_ref_map: dict[Path, str]) -> dict:
    """Return a new File dict with the path rewritten if found in sb_ref_map."""
    path_str: str = file_obj["path"]
    if path_str.startswith("LFN:") or path_str.startswith("SB:"):
        return file_obj
    local = Path(path_str)
    if local in sb_ref_map:
        # Append #filename so the wrapper can extract the file from the archive
        return {**file_obj, "path": f"{sb_ref_map[local]}#{local.name}"}
    return file_obj


def rewrite_sandbox_refs(inputs: dict, sb_ref_map: dict[Path, str]) -> dict:
    """Return a new inputs dict with local File paths replaced by SB: references.

    LFN: and SB: paths pass through unchanged.
    Non-File values pass through unchanged.
    The original dict is not mutated.
    """
    result: dict = {}
    for key, value in inputs.items():
        if _is_file_obj(value):
            result[key] = _rewrite_file_obj(value, sb_ref_map)
        elif isinstance(value, list):
            new_list = []
            for item in value:
                if _is_file_obj(item):
                    new_list.append(_rewrite_file_obj(item, sb_ref_map))
                else:
                    new_list.append(item)
            result[key] = new_list
        else:
            result[key] = value
    return result
