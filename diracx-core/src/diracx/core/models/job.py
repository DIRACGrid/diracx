"""Models used to define the data structure of the requests and responses for the DiracX API.

They are shared between the client components (cli, api) and services components (db, logic, routers).
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from .types import UTCDatetime


class InsertedJob(BaseModel):
    """Response model for a newly inserted job."""

    job_id: int = Field(alias="JobID")
    status: str = Field(alias="Status")
    minor_status: str = Field(alias="MinorStatus")
    time_stamp: UTCDatetime = Field(alias="TimeStamp")


class HeartbeatData(BaseModel, extra="forbid"):
    """Job heartbeat metrics reported by worker nodes."""

    load_average: float | None = Field(None, alias="LoadAverage")
    memory_used: float | None = Field(None, alias="MemoryUsed")
    vsize: float | None = Field(None, alias="Vsize")
    available_disk_space: float | None = Field(None, alias="AvailableDiskSpace")
    cpu_consumed: float | None = Field(None, alias="CPUConsumed")
    wall_clock_time: float | None = Field(None, alias="WallClockTime")
    standard_output: str | None = Field(None, alias="StandardOutput")


class JobCommand(BaseModel):
    """Command request for performing an action on a job."""

    job_id: int
    command: Literal["Kill"]
    arguments: str | None = None


class JobParameters(BaseModel, populate_by_name=True, extra="allow"):
    """Job parameter values that are passed to or stored with a job."""

    timestamp: UTCDatetime | None = None
    cpu_normalization_factor: int | None = Field(None, alias="CPUNormalizationFactor")
    norm_cpu_time_s: int | None = Field(None, alias="NormCPUTime(s)")
    total_cpu_time_s: int | None = Field(None, alias="TotalCPUTime(s)")
    host_name: str | None = Field(None, alias="HostName")
    grid_ce: str | None = Field(None, alias="GridCE")
    model_name: str | None = Field(None, alias="ModelName")
    pilot_agent: str | None = Field(None, alias="PilotAgent")
    pilot_reference: str | None = Field(None, alias="Pilot_Reference")
    memory_mb: int | None = Field(None, alias="Memory(MB)")
    local_account: str | None = Field(None, alias="LocalAccount")
    payload_pid: int | None = Field(None, alias="PayloadPID")
    ce_queue: str | None = Field(None, alias="CEQueue")
    batch_system: str | None = Field(None, alias="BatchSystem")
    job_type: str | None = Field(None, alias="JobType")
    job_status: str | None = Field(None, alias="JobStatus")

    @field_validator(
        "cpu_normalization_factor", "norm_cpu_time_s", "total_cpu_time_s", mode="before"
    )
    @classmethod
    def convert_cpu_fields_to_int(cls, v):
        """Normalize CPU-related values to integers before validation.

        Args:
            v: The raw field value, which may be a string or numeric type.

        Returns:
            int | None: The normalized integer value or None.
        """
        if v is None:
            return v
        if isinstance(v, str):
            try:
                return int(float(v))
            except (ValueError, TypeError) as e:
                raise ValueError(f"Cannot convert '{v}' to integer") from e
        if isinstance(v, (int, float)):
            return int(v)
        return v


class JobAttributes(BaseModel, populate_by_name=True, extra="forbid"):
    """Job attribute fields representing metadata about a job."""

    job_type: str | None = Field(None, alias="JobType")
    job_group: str | None = Field(None, alias="JobGroup")
    site: str | None = Field(None, alias="Site")
    job_name: str | None = Field(None, alias="JobName")
    owner: str | None = Field(None, alias="Owner")
    owner_group: str | None = Field(None, alias="OwnerGroup")
    vo: str | None = Field(None, alias="VO")
    submission_time: UTCDatetime | None = Field(None, alias="SubmissionTime")
    reschedule_time: UTCDatetime | None = Field(None, alias="RescheduleTime")
    last_update_time: UTCDatetime | None = Field(None, alias="LastUpdateTime")
    start_exec_time: UTCDatetime | None = Field(None, alias="StartExecTime")
    heart_beat_time: UTCDatetime | None = Field(None, alias="HeartBeatTime")
    end_exec_time: UTCDatetime | None = Field(None, alias="EndExecTime")
    status: str | None = Field(None, alias="Status")
    minor_status: str | None = Field(None, alias="MinorStatus")
    application_status: str | None = Field(None, alias="ApplicationStatus")
    user_priority: int | None = Field(None, alias="UserPriority")
    reschedule_counter: int | None = Field(None, alias="RescheduleCounter")
    verified_flag: bool | None = Field(None, alias="VerifiedFlag")
    accounted_flag: bool | str | None = Field(None, alias="AccountedFlag")


class JobMetaData(JobAttributes, JobParameters, extra="allow"):
    """Combined job metadata and parameter model."""


class JobStatus(StrEnum):
    SUBMITTING = "Submitting"
    RECEIVED = "Received"
    CHECKING = "Checking"
    STAGING = "Staging"
    WAITING = "Waiting"
    MATCHED = "Matched"
    RUNNING = "Running"
    STALLED = "Stalled"
    COMPLETING = "Completing"
    DONE = "Done"
    COMPLETED = "Completed"
    FAILED = "Failed"
    DELETED = "Deleted"
    KILLED = "Killed"
    RESCHEDULED = "Rescheduled"


class JobMinorStatus(StrEnum):
    MAX_RESCHEDULING = "Maximum of reschedulings reached"
    RESCHEDULED = "Job Rescheduled"


class JobLoggingRecord(BaseModel):
    """Log entry captured when a job status changes."""

    job_id: int
    status: JobStatus | Literal["idem"]
    minor_status: str
    application_status: str
    date: UTCDatetime
    source: str


class JobStatusUpdate(BaseModel):
    """Request model to update an existing job's status."""

    status: JobStatus | None = Field(None, alias="Status")
    minor_status: str | None = Field(None, alias="MinorStatus")
    application_status: str | None = Field(None, alias="ApplicationStatus")
    source: str = Field("Unknown", alias="Source")


class LimitedJobStatusReturn(BaseModel):
    """Minimal job status response model returned for status-only queries."""

    status: JobStatus = Field(alias="Status")
    minor_status: str = Field(alias="MinorStatus")
    application_status: str = Field(alias="ApplicationStatus")


class JobStatusReturn(LimitedJobStatusReturn):
    """Extended job status response including timestamp and source."""

    status_time: UTCDatetime = Field(alias="StatusTime")
    source: str = Field(alias="Source")


class SetJobStatusReturn(BaseModel):
    """Response model for the outcome of setting job statuses."""

    class SetJobStatusReturnSuccess(BaseModel):
        """Successful new status change."""

        status: JobStatus | None = Field(None, alias="Status")
        minor_status: str | None = Field(None, alias="MinorStatus")
        application_status: str | None = Field(None, alias="ApplicationStatus")
        heart_beat_time: UTCDatetime | None = Field(None, alias="HeartBeatTime")
        start_exec_time: UTCDatetime | None = Field(None, alias="StartExecTime")
        end_exec_time: UTCDatetime | None = Field(None, alias="EndExecTime")
        last_update_time: UTCDatetime | None = Field(None, alias="LastUpdateTime")

    success: dict[int, SetJobStatusReturnSuccess]
    failed: dict[int, dict[str, str]]
