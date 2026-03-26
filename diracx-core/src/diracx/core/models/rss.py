from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


class AllowedStatus(BaseModel):
    allowed: Literal[True]
    warnings: list[str] = []

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


class StorageElementStatus(BaseModel):
    read: ResourceStatus
    write: ResourceStatus
    check: ResourceStatus
    remove: ResourceStatus


class ComputeElementStatus(BaseModel):
    all: ResourceStatus


class FTSStatus(BaseModel):
    all: ResourceStatus


class SiteStatus(BaseModel):
    all: ResourceStatus


ALLOWED = {"Active", "Degraded"}
BANNED = {"Banned", "Probing", "Error", "Unknown"}


def map_status(db_status: str, reason: str | None = None) -> ResourceStatus:
    if db_status in ALLOWED:
        return AllowedStatus(allowed=True)

    if db_status in BANNED:
        return BannedStatus(
            allowed=False,
            reason=reason or db_status,
        )

    return BannedStatus(
        allowed=False,
        reason=f"Unknown status: {db_status}",
    )
