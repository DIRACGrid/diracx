"""Custom filesystem access for DIRAC file references using replica map."""

from __future__ import annotations

import os
from typing import Any

from cwltool.stdfsaccess import StdFsAccess

from diracx.core.models.replica_map import ReplicaMap


def _is_ref(path: str) -> bool:
    """Check if a path is an LFN: or SB: reference."""
    return path.startswith("LFN:") or path.startswith("SB:")


class DiracReplicaMapFsAccess(StdFsAccess):
    """Use replica map to resolve LFN: and SB: paths to physical file locations.

    This class extends StdFsAccess to handle LFN: and SB: prefixed paths by
    looking them up in the replica map and using the physical file path instead.

    Key difference: LFN keys are stored WITHOUT prefix, SB keys are stored WITH prefix.
    """

    def __init__(self, basedir: str, replica_map: ReplicaMap | None = None):
        """Initialize with optional replica map.

        Args:
            basedir: Base directory for relative paths
            replica_map: ReplicaMap instance for path resolution

        """
        super().__init__(basedir)
        self.replica_map = replica_map or ReplicaMap(root={})

    def _resolve_path(self, path: str) -> tuple[str, bool]:
        """Resolve an LFN or SB reference to a physical file path.

        Args:
            path: File reference (LFN:/path or SB:pfn#relpath)

        Returns:
            Tuple of (physical path/URL, is_remote)

        """
        # SB: keys are stored with prefix; LFN keys without
        if path.startswith("SB:"):
            key = path
        else:
            key = path.removeprefix("LFN:")

        if key in self.replica_map.root:
            entry = self.replica_map[key]
            if entry.replicas:
                url = entry.replicas[0].url
                is_remote = url.scheme != "file"
                return str(url) if is_remote else (url.path or ""), is_remote

        # Not in replica map — return cleaned path for LFN, original for SB
        if path.startswith("SB:"):
            return path, False
        return key, False

    def _abs(self, p: str) -> str:
        """Resolve path, handling LFN/SB references via replica map."""
        if _is_ref(p):
            p, is_remote = self._resolve_path(p)
            if is_remote:
                return p
        return super()._abs(p)

    def glob(self, pattern: str) -> list[str]:
        """Glob with LFN/SB support."""
        if _is_ref(pattern):
            resolved, is_remote = self._resolve_path(pattern)
            if is_remote:
                return [pattern]
            if os.path.exists(resolved):
                return [pattern]
            return []
        return super().glob(pattern)

    def open(self, fn: str, mode: str) -> Any:
        """Open file with LFN/SB resolution."""
        if _is_ref(fn):
            fn, is_remote = self._resolve_path(fn)
            if is_remote:
                raise ValueError(f"Cannot open remote file: {fn}")
        return super().open(fn, mode)

    def exists(self, fn: str) -> bool:
        """Check if file exists, with LFN/SB resolution."""
        if _is_ref(fn):
            fn, is_remote = self._resolve_path(fn)
            if is_remote:
                return True
        return super().exists(fn)

    def isfile(self, fn: str) -> bool:
        """Check if path is a file, with LFN/SB resolution."""
        if _is_ref(fn):
            fn, is_remote = self._resolve_path(fn)
            if is_remote:
                return True
        return super().isfile(fn)

    def isdir(self, fn: str) -> bool:
        """Check if path is a directory, with LFN/SB resolution."""
        if _is_ref(fn):
            return False
        return super().isdir(fn)

    def size(self, fn: str) -> int:
        """Get file size, with LFN/SB resolution."""
        if _is_ref(fn):
            # Try to get size from replica map first
            if fn.startswith("SB:"):
                key = fn
            else:
                key = fn.removeprefix("LFN:")
            if key in self.replica_map.root:
                entry = self.replica_map[key]
                if entry.size_bytes is not None:
                    return entry.size_bytes
            fn, is_remote = self._resolve_path(fn)
            if is_remote:
                raise ValueError(f"Cannot determine size of remote file: {fn}")
        return super().size(fn)
