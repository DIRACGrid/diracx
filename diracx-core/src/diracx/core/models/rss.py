from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, PrivateAttr


class CachedModel(BaseModel):
    """Base class for models that are cached."""

    # hash for a unique representation of the status version
    _hexsha: str = PrivateAttr()
    # modification date
    _modified: datetime = PrivateAttr()


class AllowedStatus(BaseModel):
    allowed: Literal[True]
    warnings: str | None = None

    def __bool__(self) -> bool:
        return True


class BannedStatus(BaseModel):
    allowed: Literal[False]
    reason: str = "Unknown"

    def __bool__(self) -> bool:
        return False


ResourceStatus = Annotated[
    Union[AllowedStatus, BannedStatus],
    Field(discriminator="allowed"),
]


class ResourceType(StrEnum):
    Compute = "ComputeElement"
    Storage = "StorageElement"
    FTS = "FTS"


class StorageElementStatus(CachedModel):
    read: ResourceStatus
    write: ResourceStatus
    check: ResourceStatus
    remove: ResourceStatus


class ComputeElementStatus(CachedModel):
    all: ResourceStatus


class FTSStatus(CachedModel):
    all: ResourceStatus


class SiteStatus(CachedModel):
    all: ResourceStatus


ALLOWED = {"Active", "Degraded"}
BANNED = {"Banned", "Probing", "Error", "Unknown"}
