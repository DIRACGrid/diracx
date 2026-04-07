"""Custom PathMapper for handling DIRAC LFNs in CWL workflows."""
# ruff: noqa: N803

from __future__ import annotations

import logging
from typing import List, Optional, cast

from cwltool.pathmapper import MapperEnt, PathMapper, uri_file_path
from cwltool.utils import CWLObjectType

from diracx.core.models.replica_map import ReplicaMap

logger = logging.getLogger("dirac-cwl-run")


class DiracPathMapper(PathMapper):
    """PathMapper that can resolve LFN: URIs using a replica map.

    This extends PathMapper to intercept files with LFN: URIs and resolve them
    to physical file paths using the replica map before processing.
    """

    def __init__(
        self,
        referenced_files: List[CWLObjectType],
        basedir: str,
        stagedir: str,
        separateDirs: bool = True,
        replica_map: Optional[ReplicaMap] = None,
    ):
        """Initialize with optional replica map.

        Args:
            referenced_files: Files referenced in the CWL workflow
            basedir: Base directory for relative paths
            stagedir: Staging directory for files
            separateDirs: Whether to separate directories
            replica_map: ReplicaMap for LFN resolution

        """
        self.replica_map = replica_map or ReplicaMap(root={})
        super().__init__(referenced_files, basedir, stagedir, separateDirs)

    def visit(
        self,
        obj: CWLObjectType,
        stagedir: str,
        basedir: str,
        copy: bool = False,
        staged: bool = False,
    ) -> None:
        """Visit a file object, handling LFN: URIs.

        LFN: URIs are resolved to their physical file locations (PFNs) using the
        replica map. The PFN can be:
        - A local file path (file://...)
        - A remote URL (root://... for xrootd, etc.)

        CWL will use these paths/URLs directly without staging (copying/linking).
        The files are either already downloaded locally or will be accessed via
        network protocols like xrootd.
        """
        tgt = str(obj.get("location", ""))
        logger.debug("DiracPathMapper.visit: processing location=%s", tgt)

        # Check if this is an LFN or SB: reference that we need to resolve
        if (tgt.startswith("LFN:") or tgt.startswith("SB:")) and obj.get(
            "class"
        ) == "File":
            # For LFN: entries, the replica map key is the bare path (without "LFN:" prefix).
            # For SB: entries, the replica map key is the full "SB:..." string.
            if tgt.startswith("LFN:"):
                lookup_key = tgt[4:]  # Remove "LFN:" prefix
            else:
                lookup_key = tgt  # SB: key includes the prefix
            logger.debug("DiracPathMapper.visit: Found LFN/SB key=%s", lookup_key)

            # Look up in replica map to resolve to PFN
            if lookup_key in self.replica_map.root:
                entry = self.replica_map[lookup_key]
                if entry.replicas:
                    # Get the first replica's URL (can be file:// or root:// or https:// etc.)
                    pfn = str(entry.replicas[0].url)
                    logger.info("DiracPathMapper: Resolved %s -> %s", lookup_key, pfn)

                    # cwltool uses MapperEnt.target as the path passed to the command
                    # (set via file_o["path"] = pathmapper.mapper(location)[1]).
                    # For local file:// URLs we must convert to a plain filesystem path;
                    # for remote protocols (root://, https://) the URL is used as-is.
                    if pfn.startswith("file://"):
                        target = uri_file_path(pfn)
                    else:
                        target = pfn

                    self._pathmap[tgt] = MapperEnt(
                        resolved=pfn,  # The physical URL/path
                        target=target,  # Local path for file://, URL for remote protocols
                        type="File",
                        staged=False,  # We're not staging/copying this file
                    )

                    # Add size from replica map if available
                    if entry.size_bytes is not None and "size" not in obj:
                        obj["size"] = entry.size_bytes

                    # Store checksum if available
                    if entry.checksum and "checksum" not in obj:
                        if entry.checksum.adler32:
                            # Format: "adler32$788c5caa"
                            obj["checksum"] = f"adler32${entry.checksum.adler32}"

                    # Handle secondary files if any
                    self.visitlisting(
                        cast(List[CWLObjectType], obj.get("secondaryFiles", [])),
                        stagedir,
                        basedir,
                        copy=copy,
                        staged=staged,
                    )

                    # Don't call parent visit - we've handled this completely
                    return

                else:
                    logger.warning(
                        "DiracPathMapper: %s in replica map but has no replicas",
                        lookup_key,
                    )
            else:
                # LFN/SB not in replica map - this will likely fail later
                logger.error(
                    "DiracPathMapper: %s NOT in replica map! Available keys: %s",
                    lookup_key,
                    list(self.replica_map.root.keys())[:5],
                )

        # Handle remote protocol URLs (root://, https://, etc.) that should not be staged
        if obj.get("class") == "File" and any(
            tgt.startswith(scheme)
            for scheme in ("root://", "xroot://", "https://", "http://")
        ):
            logger.info("DiracPathMapper: Using remote URL directly: %s", tgt)
            self._pathmap[tgt] = MapperEnt(
                resolved=tgt,
                target=tgt,
                type="File",
                staged=False,
            )
            self.visitlisting(
                cast(List[CWLObjectType], obj.get("secondaryFiles", [])),
                stagedir,
                basedir,
                copy=copy,
                staged=staged,
            )
            return

        # For non-LFN files or when LFN resolution failed, delegate to parent class
        super().visit(obj, stagedir, basedir, copy, staged)

    def mapper(self, src: str) -> MapperEnt:
        """Return the MapperEnt for a given source URI.

        Extends the base implementation to handle DIRAC-specific URI schemes
        (LFN:, SB:) that may contain ``#`` fragment identifiers. cwltool's base
        ``mapper()`` treats ``#`` as a separator and tries to look up the prefix
        before appending the fragment to the target path, which is incorrect for
        SB: keys where the full key (including fragment) is stored in _pathmap.
        We intercept those cases and return the stored entry directly.
        """
        # If the full key is already present, return it directly (handles SB: keys
        # with '#' fragments that are stored under the full key).
        if src in self._pathmap:
            return self._pathmap[src]
        # Fall back to cwltool's default fragment-stripping logic for all other URIs.
        return super().mapper(src)
