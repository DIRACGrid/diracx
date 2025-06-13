"""Models used to define the data structure of the requests and responses for the DiracX API.

They are shared between the client components (cli, api) and services components (db, logic, routers).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator
from typing_extensions import TypedDict


class PilotFieldsMapping(BaseModel, extra="forbid"):
    """All the fields that a user can modify on a Pilot (except PilotStamp)."""

    PilotStamp: str
    StatusReason: Optional[str] = None
    Status: Optional[PilotStatus] = None
    BenchMark: Optional[float] = None
    DestinationSite: Optional[str] = None
    Queue: Optional[str] = None
    GridSite: Optional[str] = None
    GridType: Optional[str] = None
    AccountingSent: Optional[bool] = None
    CurrentJobID: Optional[int] = None


class PilotStatus(StrEnum):
    #: The pilot has been generated and is transferred to a remote site:
    SUBMITTED = "Submitted"
    #: The pilot is waiting for a computing resource in a batch queue:
    WAITING = "Waiting"
    #: The pilot is running a payload on a worker node:
    RUNNING = "Running"
    #: The pilot finished its execution:
    DONE = "Done"
    #: The pilot execution failed:
    FAILED = "Failed"
    #: The pilot was deleted:
    DELETED = "Deleted"
    #: The pilot execution was aborted:
    ABORTED = "Aborted"
    #: Cannot get information about the pilot status:
    UNKNOWN = "Unknown"
