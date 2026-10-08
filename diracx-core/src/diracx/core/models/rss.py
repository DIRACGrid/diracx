"""Models describing resource status information for the RSS service."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


class AllowedStatus(BaseModel):
    """Status indicating that a resource is allowed.

    Attributes:
        allowed: Literal indicating that the resource is allowed.
        warnings: Optional warning associated with the allowed status.
    """

    allowed: Literal[True]
    warnings: str | None = None

    def __bool__(self) -> bool:
        """Return whether the resource is allowed.

        Returns:
            Always ``True`` for an allowed status.
        """
        return True


class BannedStatus(BaseModel):
    """Status indicating that a resource is not allowed.

    Attributes:
        allowed: Literal indicating that the resource is banned.
        reason: Reason associated with the banned status.
    """

    allowed: Literal[False]
    reason: str = "Unknown"

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
    """Status of the operations supported by a storage element.

    Attributes:
        read: Read operation status.
        write: Write operation status.
        check: Check operation status.
        remove: Remove operation status.
    """

    read: ResourceStatus
    write: ResourceStatus
    check: ResourceStatus
    remove: ResourceStatus


class ComputeElementStatus(BaseModel):
    """Status of a compute element.

    Attributes:
        all: Overall compute element status.
    """

    all: ResourceStatus


class FTSStatus(BaseModel):
    """Status of an FTS resource.

    Attributes:
        all: Overall FTS status.
    """

    all: ResourceStatus


class SiteStatus(BaseModel):
    """Status of a site.

    Attributes:
        all: Overall site status.
    """

    all: ResourceStatus


ALLOWED = {"Active", "Degraded"}
BANNED = {"Banned", "Probing", "Error", "Unknown"}
