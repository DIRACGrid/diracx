"""Pilot agents schema definitions.

Defines the SQLAlchemy ORM mappings for pilot agent records, mappings from
jobs to pilots and pilot output blobs.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Double,
    Index,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from diracx.db.sql.utils import (
    EnumBackedBool,
    str32,
    str128,
    str255,
)
from diracx.db.sql.utils.types import SmarterDateTime


class PilotAgentsDBBase(DeclarativeBase):
    """Base declarative class for the pilot agents schema.

    The :attr:`type_annotation_map` provides compact annotation aliases like
    ``str32`` that are translated to concrete SQLAlchemy ``String`` types.
    """

    type_annotation_map = {
        str32: String(32),
        str128: String(128),
        str255: String(255),
    }


class PilotAgents(PilotAgentsDBBase):
    """ORM mapping for the ``PilotAgents`` table.

    Stores pilot job reference rows and associated metadata such as site,
    queue, timestamps and status.

    Attributes:
        pilot_id (int): Auto-incrementing primary key.
        initial_job_id (int): Initial job identifier associated with the pilot.
        current_job_id (int): Current job id the pilot is handling.
        pilot_job_reference (str): External pilot job reference string.
        pilot_stamp (str): Optional pilot stamp value.
        destination_site (str): Destination site name.
        queue (str): Queue name.
        grid_site (str): Grid site identifier.
        vo (str): Virtual organization.
        grid_type (str): Grid type (for example, "LCG").
        benchmark (float): Measured benchmark value.
        submission_time (datetime | None): Submission timestamp.
        last_update_time (datetime | None): Last update timestamp.
        status (str): Current status string.
        status_reason (str): Optional human-readable reason.
        accounting_sent (bool): Flag whether accounting was sent.
    """

    __tablename__ = "PilotAgents"

    pilot_id: Mapped[int] = mapped_column(
        "PilotID", autoincrement=True, primary_key=True
    )
    initial_job_id: Mapped[int] = mapped_column("InitialJobID", default=0)
    current_job_id: Mapped[int] = mapped_column("CurrentJobID", default=0)
    pilot_job_reference: Mapped[str255] = mapped_column(
        "PilotJobReference", default="Unknown"
    )
    pilot_stamp: Mapped[str32] = mapped_column("PilotStamp", default="")
    destination_site: Mapped[str128] = mapped_column(
        "DestinationSite", default="NotAssigned"
    )
    queue: Mapped[str128] = mapped_column("Queue", default="Unknown")
    grid_site: Mapped[str128] = mapped_column("GridSite", default="Unknown")
    vo: Mapped[str128] = mapped_column("VO")
    grid_type: Mapped[str32] = mapped_column("GridType", default="LCG")
    benchmark: Mapped[float] = mapped_column("BenchMark", Double, default=0.0)
    submission_time: Mapped[Optional[datetime]] = mapped_column(
        "SubmissionTime", SmarterDateTime
    )
    last_update_time: Mapped[Optional[datetime]] = mapped_column(
        "LastUpdateTime", SmarterDateTime
    )
    status: Mapped[str32] = mapped_column("Status", default="Unknown")
    status_reason: Mapped[str255] = mapped_column("StatusReason", default="Unknown")
    accounting_sent: Mapped[bool] = mapped_column(
        "AccountingSent", EnumBackedBool(), default=False
    )

    __table_args__ = (
        Index("PilotJobReference", "PilotJobReference"),
        Index("Status", "Status"),
        Index("Statuskey", "GridSite", "DestinationSite", "Status"),
    )


class JobToPilotMapping(PilotAgentsDBBase):
    __tablename__ = "JobToPilotMapping"

    """Mapping table from jobs to pilots.

    Each row associates a pilot with a job and records when the pilot started
    handling the job.
    """
    pilot_id: Mapped[int] = mapped_column("PilotID", primary_key=True)
    job_id: Mapped[int] = mapped_column("JobID", primary_key=True)
    start_time: Mapped[datetime] = mapped_column("StartTime", SmarterDateTime)

    __table_args__ = (Index("JobID", "JobID"), Index("PilotID", "PilotID"))


class PilotOutput(PilotAgentsDBBase):
    __tablename__ = "PilotOutput"

    """Storage for pilot standard output and error blobs.

    Attributes:
        pilot_id (int): Pilot identifier (primary key).
        std_output (str): Captured standard output text.
        std_error (str): Captured standard error text.
    """
    pilot_id: Mapped[int] = mapped_column("PilotID", primary_key=True)
    std_output: Mapped[str] = mapped_column("StdOutput", Text)
    std_error: Mapped[str] = mapped_column("StdError", Text)
