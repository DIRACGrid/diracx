from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Generic, Literal, TypeVar, Union

from pydantic import BaseModel, Field

T = TypeVar("T")


@dataclass(frozen=True)
class Snapshot(Generic[T]):
    """Wraps a cached data payload with its cache metadata."""

    data: T
    hexsha: str
    modified: datetime


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
