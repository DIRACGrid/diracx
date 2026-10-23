"""Models describing resource status information for the RSS service."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import Field

from ._base import BaseModel


class AllowedStatus(BaseModel):
    """Status indicating that a resource is allowed."""

    allowed: Literal[True]
    """Literal indicating that the resource is allowed."""
    warnings: str | None = None
    """Optional warning associated with the allowed status."""

    def __bool__(self) -> bool:
        """Return whether the resource is allowed.

        Returns:
            Always ``True`` for an allowed status.
        """
        return True


class BannedStatus(BaseModel):
    """Status indicating that a resource is not allowed."""

    allowed: Literal[False]
    """Literal indicating that the resource is banned."""
    reason: str = "Unknown"
    """Reason associated with the banned status."""

    def __bool__(self) -> bool:
        """Return whether the resource is allowed.

        Returns:
            Always ``False`` for a banned status.
        """
        return False


ResourceStatus = Annotated[
    Union[AllowedStatus, BannedStatus],
    Field(discriminator="allowed"),
]


class ResourceType(StrEnum):
    """Types of resources tracked by the RSS service."""

    Compute = "ComputeElement"
    Storage = "StorageElement"
    FTS = "FTS"


class StorageElementStatus(BaseModel):
    """Status of the operations supported by a storage element."""

    read: ResourceStatus
    """Read operation status."""
    write: ResourceStatus
    """Write operation status."""
    check: ResourceStatus
    """Check operation status."""
    remove: ResourceStatus
    """Remove operation status."""


class ComputeElementStatus(BaseModel):
    """Status of a compute element."""

    all: ResourceStatus
    """Overall compute element status."""


class FTSStatus(BaseModel):
    """Status of an FTS resource."""

    all: ResourceStatus
    """Overall FTS status."""


class SiteStatus(BaseModel):
    """Status of a site."""

    all: ResourceStatus
    """Overall site status."""


ALLOWED = {"Active", "Degraded"}
BANNED = {"Banned", "Probing", "Error", "Unknown"}
