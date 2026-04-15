"""Sandbox scanning, grouping, and path rewriting for CWL job submission."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _is_file_obj(value: Any) -> bool:
    """Return True if value is a CWL File object dict."""
    return isinstance(value, dict) and value.get("class") == "File"


def _get_file_ref(file_obj: dict) -> str:
    """Return the file reference string from a CWL File object.

    Checks ``location`` first (for URI schemes), then ``path`` (for local files).
    """
    return file_obj.get("location") or file_obj.get("path", "")


def _classify_ref(ref: str) -> tuple[Path | None, str | None]:
    """Classify a CWL File reference string.

    Returns (local_path, lfn) where exactly one is set, or both None for SB: refs.
    """
    if ref.startswith("LFN:"):
        return None, ref
    if ref.startswith("SB:"):
        return None, None
    return Path(ref), None


_URI_PREFIXES = ("LFN:", "SB:")


def validate_file_references(inputs: dict) -> None:
    """Reject CWL File objects that put URI schemes in ``path`` instead of ``location``.

    Per the CWL spec, ``path`` is a local filesystem path while ``location``
    is an IRI that identifies the resource.  ``LFN:`` and ``SB:`` are custom
    URI schemes and must be placed in ``location``.
    """
    for key, value in inputs.items():
        items = value if isinstance(value, list) else [value]
        for item in items:
            if _is_file_obj(item):
                path_val = item.get("path", "")
                if isinstance(path_val, str) and path_val.startswith(_URI_PREFIXES):
                    raise ValueError(
                        f"CWL File input '{key}' has a URI scheme in 'path' "
                        f"({path_val!r}). Use 'location' for LFN: and SB: "
                        f"references — 'path' is reserved for local "
                        f"filesystem paths."
                    )


def scan_file_references(inputs: dict) -> tuple[list[Path], list[str]]:
    """Walk an input dict and classify CWL File references.

    Returns:
        local_files: paths to local files (no LFN: or SB: prefix)
        lfns: LFN: prefixed location strings
    SB: refs are silently ignored (already uploaded).
    Non-File values are skipped.

    """
    local_files: list[Path] = []
    lfns: list[str] = []

    for value in inputs.values():
        if _is_file_obj(value):
            ref = _get_file_ref(value)
            local, lfn = _classify_ref(ref)
            if local is not None:
                local_files.append(local)
            elif lfn is not None:
                lfns.append(lfn)
        elif isinstance(value, list):
            for item in value:
                if _is_file_obj(item):
                    ref = _get_file_ref(item)
                    local, lfn = _classify_ref(ref)
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
    """Return a new File dict with the local path replaced by an SB: location.

    The SB: reference is a URI and goes into ``location`` per the CWL spec.
    The ``path`` key is removed since the file no longer exists locally.
    """
    ref = _get_file_ref(file_obj)
    if ref.startswith(("LFN:", "SB:")):
        return file_obj
    local = Path(ref)
    if local in sb_ref_map:
        # Append #filename so the wrapper can extract the file from the archive
        result = {k: v for k, v in file_obj.items() if k != "path"}
        result["location"] = f"{sb_ref_map[local]}#{local.name}"
        return result
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
