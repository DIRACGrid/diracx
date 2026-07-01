"""Resource status models for DIRACX RSS data.

This module defines the data models used to describe the availability and
status of computing, storage, and transfer resources.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


class AllowedStatus(BaseModel):
    """Status indicating that a resource is permitted to operate."""

    allowed: Literal[True]
    warnings: str | None = None

    def __bool__(self) -> bool:
        return True


class BannedStatus(BaseModel):
    """Status indicating that a resource is not currently permitted to operate."""

    allowed: Literal[False]
    reason: str = "Unknown"

    def __bool__(self) -> bool:
        return False


ResourceStatus = Annotated[
    Union[AllowedStatus, BannedStatus],
    Field(discriminator="allowed"),
]


class ResourceType(StrEnum):
    """Supported resource types in the RSS model."""

    Compute = "ComputeElement"
    Storage = "StorageElement"
    FTS = "FTS"


class StorageElementStatus(BaseModel):
    """Status for the operations supported by a storage element."""

    read: ResourceStatus
    write: ResourceStatus
    check: ResourceStatus
    remove: ResourceStatus


class ComputeElementStatus(BaseModel):
    """Status for a compute element and its associated operations."""

    all: ResourceStatus


class FTSStatus(BaseModel):
    """Status for an FTS service endpoint."""

    all: ResourceStatus


class SiteStatus(BaseModel):
    """Status for a site-level resource entry."""

    all: ResourceStatus


ALLOWED = {"Active", "Degraded"}
BANNED = {"Banned", "Probing", "Error", "Unknown"}
