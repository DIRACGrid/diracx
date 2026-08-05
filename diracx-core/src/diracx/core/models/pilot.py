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


class PilotMetadata(BaseModel, populate_by_name=True, extra="forbid"):
    """Mutable metadata attached to a pilot.

    The pilot is identified by its stamp, passed alongside this model
    (e.g. as the mapping key on ``PATCH /api/pilots/metadata``). Every
    field is optional; when absent it is left untouched by an update.
    """

    status_reason: str | None = Field(
        None,
        alias="StatusReason",
        description="Human-readable reason for the current status.",
    )
    status: PilotStatus | None = Field(
        None, alias="Status", description="Current pilot status."
    )
    benchmark: float | None = Field(
        None, alias="BenchMark", description="Pilot benchmark value."
    )
    destination_site: str | None = Field(
        None, alias="DestinationSite", description="Destination site."
    )
    queue: str | None = Field(None, alias="Queue", description="Batch queue name.")
    grid_site: str | None = Field(None, alias="GridSite", description="Grid site.")
    grid_type: str | None = Field(None, alias="GridType", description="Grid type.")
    accounting_sent: bool | None = Field(
        None,
        alias="AccountingSent",
        description="Whether accounting has been sent for this pilot.",
    )
    current_job_id: int | None = Field(
        None,
        alias="CurrentJobID",
        description="ID of the job currently running on this pilot.",
    )
