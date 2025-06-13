"""Pilot-related models shared between client, logic, and services."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel


class PilotStatus(StrEnum):
    SUBMITTED = "Submitted"
    WAITING = "Waiting"
    RUNNING = "Running"
    DONE = "Done"
    FAILED = "Failed"
    DELETED = "Deleted"
    ABORTED = "Aborted"
    UNKNOWN = "Unknown"


class PilotFieldsMapping(BaseModel, extra="forbid"):
    """All the fields that a user can modify on a Pilot (except PilotStamp)."""

    PilotStamp: str
    StatusReason: str | None = None
    Status: PilotStatus | None = None
    BenchMark: float | None = None
    DestinationSite: str | None = None
    Queue: str | None = None
    GridSite: str | None = None
    GridType: str | None = None
    AccountingSent: bool | None = None
    CurrentJobID: int | None = None
