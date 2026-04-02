"""Custom filesystem access for DIRAC LFNs using replica map."""

from __future__ import annotations

import os
from typing import Any

from cwltool.stdfsaccess import StdFsAccess

from diracx.core.models.replica_map import ReplicaMap


class DiracReplicaMapFsAccess(StdFsAccess):
    """Use replica map to resolve LFNs to physical file locations.

    This class extends StdFsAccess to handle LFN: prefixed paths by looking them up
    in the replica map and using the physical file path (PFN) instead.
    """

    def __init__(self, basedir: str, replica_map: ReplicaMap | None = None):
        """Initialize with optional replica map.

        Args:
            basedir: Base directory for relative paths
            replica_map: ReplicaMap instance for LFN resolution

        """
        super().__init__(basedir)
        self.replica_map = replica_map or ReplicaMap(root={})

    def _resolve_lfn(self, lfn: str) -> tuple[str, bool]:
        """Resolve an LFN to a physical file path using the replica map.

        Args:
            lfn: Logical file name (with or without LFN: prefix)

        Returns:
            Tuple of (physical path/URL, is_remote)
            - physical path/URL: PFN if found in replica map, original path otherwise
            - is_remote: True if the PFN is a remote URL (root://, etc.), False for local files

        """
        # Remove LFN: prefix if present
        clean_lfn = lfn.removeprefix("LFN:")

        # Look up in replica map
        if clean_lfn in self.replica_map.root:
            entry = self.replica_map[clean_lfn]
            if entry.replicas:
                url = entry.replicas[0].url
                is_remote = url.scheme != "file"
                # For local files, return just the path; for remote, return full URL
                return str(url) if is_remote else (url.path or ""), is_remote

        # If not in replica map, return the original LFN (assume local)
        return clean_lfn, False

    def _abs(self, p: str) -> str:
        """Resolve path, handling LFNs via replica map.

        Overrides StdFsAccess._abs to intercept LFN: paths and resolve them
        through the replica map before proceeding with normal path resolution.
        """
        if p.startswith("LFN:"):
            # Resolve LFN to PFN first
            p, is_remote = self._resolve_lfn(p)
            # For remote URLs, return as-is (don't apply basedir logic)
            if is_remote:
                return p

        # Now use parent class logic for the physical path
        return super()._abs(p)

    def glob(self, pattern: str) -> list[str]:
        """Glob with LFN support."""
        # For LFNs, we can't really glob - just check if it exists
        if pattern.startswith("LFN:"):
            resolved, is_remote = self._resolve_lfn(pattern)
            # For remote files, assume they exist (can't check)
            if is_remote:
                return [pattern]  # Return the original LFN
            # For local files, check existence
            if os.path.exists(resolved):
                return [pattern]
            return []
        return super().glob(pattern)

    def open(self, fn: str, mode: str) -> Any:
        """Open file with LFN resolution."""
        if fn.startswith("LFN:"):
            fn, is_remote = self._resolve_lfn(fn)
            # Remote files can't be opened directly - let it fail with clear error
            if is_remote:
                raise ValueError(f"Cannot open remote file: {fn}")
        return super().open(fn, mode)

    def exists(self, fn: str) -> bool:
        """Check if file exists, with LFN resolution."""
        if fn.startswith("LFN:"):
            fn, is_remote = self._resolve_lfn(fn)
            # For remote files, assume they exist (can't check remotely)
            if is_remote:
                return True
        return super().exists(fn)

    def isfile(self, fn: str) -> bool:
        """Check if path is a file, with LFN resolution."""
        if fn.startswith("LFN:"):
            fn, is_remote = self._resolve_lfn(fn)
            # For remote files, assume they are files (not dirs)
            if is_remote:
                return True
        return super().isfile(fn)

    def isdir(self, fn: str) -> bool:
        """Check if path is a directory, with LFN resolution."""
        if fn.startswith("LFN:"):
            # LFNs are always files, never directories
            return False
        return super().isdir(fn)

    def size(self, fn: str) -> int:
        """Get file size, with LFN resolution."""
        if fn.startswith("LFN:"):
            # Try to get size from replica map first
            clean_lfn = fn.removeprefix("LFN:")
            if clean_lfn in self.replica_map.root:
                entry = self.replica_map[clean_lfn]
                if entry.size_bytes is not None:
                    return entry.size_bytes
            # Fall back to checking the physical file (if local)
            fn, is_remote = self._resolve_lfn(fn)
            if is_remote:
                # Can't check remote file size - return 0 or raise error
                raise ValueError(f"Cannot determine size of remote file: {fn}")
        return super().size(fn)
