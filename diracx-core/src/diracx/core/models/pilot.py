"""Pilot-related models shared between client, logic, and services."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class PilotStatus(StrEnum):
    SUBMITTED = "Submitted"
    WAITING = "Waiting"
    RUNNING = "Running"
    DONE = "Done"
    FAILED = "Failed"
    DELETED = "Deleted"
    ABORTED = "Aborted"
    UNKNOWN = "Unknown"


class PilotMetadata(BaseModel, extra="forbid"):
    """Mutable metadata attached to a pilot.

    ``PilotStamp`` identifies the pilot and cannot be changed. Every other
    field is optional; when absent it is left untouched by an update.
    """

    PilotStamp: str = Field(description="Immutable stamp identifying the pilot.")
    StatusReason: str | None = Field(
        default=None, description="Human-readable reason for the current status."
    )
    Status: PilotStatus | None = Field(
        default=None, description="Current pilot status."
    )
    BenchMark: float | None = Field(default=None, description="Pilot benchmark value.")
    DestinationSite: str | None = Field(default=None, description="Destination site.")
    Queue: str | None = Field(default=None, description="Batch queue name.")
    GridSite: str | None = Field(default=None, description="Grid site.")
    GridType: str | None = Field(default=None, description="Grid type.")
    AccountingSent: bool | None = Field(
        default=None,
        description="Whether accounting has been sent for this pilot.",
    )
    CurrentJobID: int | None = Field(
        default=None,
        description="ID of the job currently running on this pilot.",
    )
