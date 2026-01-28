"""Replica Map models for managing file locations in distributed storage.

This module provides Pydantic models for representing and validating a replica
map, which maps Logical File Names (LFNs) to their physical locations
(replicas) across distributed storage elements.

Key concepts:
    - LFN (Logical File Name): A unique logical path identifying a file,
      e.g., "/lhcb/MC/2024/file.dst". May be prefixed with "LFN:".
    - PFN (Physical File Name): The actual URL where a replica is
      stored, e.g., "https://storage.example.com/data/file.dst". May be
      prefixed with "PFN:".
    - Storage Element (SE): An identifier for the storage system hosting
      a replica.
    - Replica: A physical copy of a file at a specific storage element.

Example usage::

    from diracx.core.replica_map import ReplicaMap

    # Create a map from a dictionary
    replica_map = ReplicaMap(
        root={
            "/lhcb/MC/2024/file.dst": {
                "replicas": [
                    {"url": "https://storage1.cern.ch/file.dst", "se": "CERN-DST"},
                    {"url": "https://storage2.in2p3.fr/file.dst", "se": "IN2P3-DST"},
                ],
                "size_bytes": 1048576,
                "checksum": {"adler32": "788c5caa"},
            }
        }
    )

    # Iterate over LFNs
    for lfn in replica_map:
        entry = replica_map[lfn]
        print(f"{lfn}: {len(entry.replicas)} replica(s)")

"""

from __future__ import annotations

import re
from typing import Annotated, Iterator

from pydantic import (
    AnyUrl,
    BaseModel,
    BeforeValidator,
    RootModel,
    field_validator,
)


def _validate_lfn(value: str) -> str:
    """Validate and normalize Logical File Name.

    Removes LFN: prefix if present and ensures it's a valid absolute path or a filename without slashes.
    """
    value = value.removeprefix("LFN:")
    if not value:
        raise ValueError("LFN cannot be empty")
    # Either it has a slash at the start or there can be no slashes at all
    if not value.startswith("/"):
        if "/" in value:
            raise ValueError(
                "LFN must be an absolute path starting with '/' "
                "or have no slashes at all (e.g. refers to a file in the current working directory)."
            )
    return value


def _validate_pfn(value: str) -> str:
    """Validate and normalize Physical File Name.

    Removes PFN: prefix if present before URL validation.
    """
    value = value.removeprefix("PFN:")
    if not value:
        raise ValueError("PFN cannot be empty")
    return value


def _validate_adler32(value: str) -> str:
    """Validate Adler32 checksum format.

    Must be 8 hexadecimal characters.
    """
    value = value.lower()
    if len(value) != 8:
        raise ValueError(
            f"Adler32 checksum must be 8 characters long, got {len(value)}: {value}"
        )
    if not re.match(r"^[0-9a-f]{8}$", value):
        raise ValueError(
            f"Adler32 checksum must contain only hexadecimal characters: {value}"
        )
    return value


def _validate_guid(value: str) -> str:
    """Validate GUID checksum format.

    The format is 8-4-4-4-12 hexadecimal digits with hyphens (UUID format).
    Example: 6032CB7C-32DC-EC11-9A66-D85ED3091D71
    """
    value = value.upper()
    if len(value) != 36:
        raise ValueError(
            f"GUID checksum must be 36 characters long (including hyphens), got {len(value)}: {value}"
        )

    # Validate UUID format: 8-4-4-4-12 with hyphens
    if not re.match(
        r"^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$", value
    ):
        raise ValueError(f"GUID checksum must follow format 8-4-4-4-12 (UUID): {value}")

    return value


# Logical File Name such as LFN:/lhcb/MC/2024/HLT2.DST/00327923/0000/00327923_00000533_1.hlt2.dst
LFN = Annotated[str, BeforeValidator(_validate_lfn)]
PFN = Annotated[AnyUrl, BeforeValidator(_validate_pfn)]
StorageElementId = str

Adler32Checksum = Annotated[str, BeforeValidator(_validate_adler32)]
GUIDChecksum = Annotated[str, BeforeValidator(_validate_guid)]


class ReplicaMap(RootModel):
    """A map of Logical File Names to their physical replicas.

    The map is a dictionary-like structure where keys are LFNs and values
    are MapEntry objects containing replica information, file size, and
    checksums.

    Attributes:
        root: The underlying dictionary mapping LFNs to map entries.

    """

    class MapEntry(BaseModel):
        """Metadata and replica information for a single logical file.

        Attributes:
            replicas: List of physical replicas (at least one required).
            size_bytes: File size in bytes (optional).
            checksum: Checksum information for integrity verification (optional).

        """

        class Replica(BaseModel):
            """A physical copy of a file at a specific storage element.

            Attributes:
                url: The physical file name (URL or path) where the replica is stored.
                se: The storage element identifier hosting this replica.

            """

            url: PFN
            se: StorageElementId

            @field_validator("se")
            @classmethod
            def validate_se(cls, v: str) -> str:
                if not v or not v.strip():
                    raise ValueError("Storage Element ID cannot be empty")
                return v.strip()

        class Checksum(BaseModel):
            """Checksum information for file integrity verification.

            Attributes:
                adler32: Adler-32 checksum as 8 hexadecimal characters (e.g., "788c5caa").
                guid: GUID in UUID format (e.g., "6032CB7C-32DC-EC11-9A66-D85ED3091D71").

            """

            adler32: Adler32Checksum | None = None
            guid: GUIDChecksum | None = None

        replicas: list[Replica]
        size_bytes: int | None = None
        checksum: Checksum | None = None

        @field_validator("replicas")
        @classmethod
        def validate_replicas(cls, v: list) -> list:
            if not v:
                raise ValueError("At least one replica is required")
            return v

        @field_validator("size_bytes")
        @classmethod
        def validate_size_bytes(cls, v: int | None) -> int | None:
            if v is not None and v <= 0:
                raise ValueError(f"Size in bytes cannot be zero or negative: {v}")
            return v

    root: dict[LFN, MapEntry]

    def __iter__(self) -> Iterator[LFN]:  # type: ignore[override]
        """Iterate over the Logical File Names in the map."""
        return iter(self.root)

    def __getitem__(self, item: LFN) -> MapEntry:
        """Get the map entry for a given LFN."""
        return self.root[item]
