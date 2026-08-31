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


class PilotRegistrationParams(BaseModel, extra="forbid"):
    """Body of ``POST /api/pilots/`` to register a single pilot.

    The ``max_length`` constraints mirror the column sizes of the legacy
    ``PilotAgents`` table so that oversized values are rejected with a 422
    instead of a backend-dependent database error.
    """

    pilot_stamp: str = Field(
        min_length=1, max_length=32, description="Stamp of the pilot to create."
    )
    vo: str = Field(
        min_length=1, max_length=128, description="Pilot virtual organization."
    )
    grid_type: str = Field(
        "DIRAC", max_length=32, description="Grid type of the pilot."
    )
    grid_site: str = Field("Unknown", max_length=128, description="Pilot grid site.")
    destination_site: str = Field(
        "NotAssigned", max_length=128, description="Pilot destination site."
    )
    pilot_reference: str | None = Field(
        None,
        max_length=255,
        description="CE job reference of the pilot; defaults to the stamp.",
    )
    pilot_status: PilotStatus = Field(
        PilotStatus.SUBMITTED, description="Initial status of the pilot."
    )


class PilotMetadata(
    BaseModel, populate_by_name=True, extra="forbid", allow_inf_nan=False
):
    """Mutable metadata attached to a pilot.

    The pilot is identified by its stamp, passed alongside this model
    (e.g. as the mapping key on ``PATCH /api/pilots/metadata``). Every
    field is optional; when absent it is left untouched by an update.
    """

    status_reason: str | None = Field(
        None,
        alias="StatusReason",
        max_length=255,
        description="Human-readable reason for the current status.",
    )
    status: PilotStatus | None = Field(
        None, alias="Status", description="Current pilot status."
    )
    benchmark: float | None = Field(
        None, alias="BenchMark", description="Pilot benchmark value."
    )
    destination_site: str | None = Field(
        None, alias="DestinationSite", max_length=128, description="Destination site."
    )
    queue: str | None = Field(
        None, alias="Queue", max_length=128, description="Batch queue name."
    )
    grid_site: str | None = Field(
        None, alias="GridSite", max_length=128, description="Grid site."
    )
    grid_type: str | None = Field(
        None, alias="GridType", max_length=32, description="Grid type."
    )
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
