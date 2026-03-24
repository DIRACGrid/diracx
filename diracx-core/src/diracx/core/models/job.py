"""Models used to define the data structure of the requests and responses for the DiracX API.

They are shared between the client components (cli, api) and services components (db, logic, routers).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field, field_validator
from typing_extensions import TypedDict


class InsertedJob(TypedDict):
    JobID: int
    Status: str
    MinorStatus: str
    TimeStamp: datetime


class HeartbeatData(BaseModel, extra="forbid"):
    LoadAverage: float | None = None
    MemoryUsed: float | None = None
    Vsize: float | None = None
    AvailableDiskSpace: float | None = None
    CPUConsumed: float | None = None
    WallClockTime: float | None = None
    StandardOutput: str | None = None


class JobCommand(BaseModel):
    job_id: int
    command: Literal["Kill"]
    arguments: str | None = None


class JobParameters(BaseModel, populate_by_name=True, extra="allow"):
    """Some of the most important parameters that can be set for a job."""

    timestamp: datetime | None = None
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
        """Convert string representation of float to integer for CPU-related fields."""
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
    """All the attributes that can be set for a job."""

    job_type: str | None = Field(None, alias="JobType")
    job_group: str | None = Field(None, alias="JobGroup")
    site: str | None = Field(None, alias="Site")
    job_name: str | None = Field(None, alias="JobName")
    owner: str | None = Field(None, alias="Owner")
    owner_group: str | None = Field(None, alias="OwnerGroup")
    vo: str | None = Field(None, alias="VO")
    submission_time: datetime | None = Field(None, alias="SubmissionTime")
    reschedule_time: datetime | None = Field(None, alias="RescheduleTime")
    last_update_time: datetime | None = Field(None, alias="LastUpdateTime")
    start_exec_time: datetime | None = Field(None, alias="StartExecTime")
    heart_beat_time: datetime | None = Field(None, alias="HeartBeatTime")
    end_exec_time: datetime | None = Field(None, alias="EndExecTime")
    status: str | None = Field(None, alias="Status")
    minor_status: str | None = Field(None, alias="MinorStatus")
    application_status: str | None = Field(None, alias="ApplicationStatus")
    user_priority: int | None = Field(None, alias="UserPriority")
    reschedule_counter: int | None = Field(None, alias="RescheduleCounter")
    verified_flag: bool | None = Field(None, alias="VerifiedFlag")
    accounted_flag: bool | str | None = Field(None, alias="AccountedFlag")


class JobMetaData(JobAttributes, JobParameters, extra="allow"):
    """A model that combines both JobAttributes and JobParameters."""


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
    job_id: int
    status: JobStatus | Literal["idem"]
    minor_status: str
    application_status: str
    date: datetime
    source: str


class JobStatusUpdate(BaseModel):
    Status: JobStatus | None = None
    MinorStatus: str | None = None
    ApplicationStatus: str | None = None
    Source: str = "Unknown"


class LimitedJobStatusReturn(BaseModel):
    Status: JobStatus
    MinorStatus: str
    ApplicationStatus: str


class JobStatusReturn(LimitedJobStatusReturn):
    StatusTime: datetime
    Source: str


class SetJobStatusReturn(BaseModel):
    class SetJobStatusReturnSuccess(BaseModel):
        """Successful new status change."""

        Status: JobStatus | None = None
        MinorStatus: str | None = None
        ApplicationStatus: str | None = None
        HeartBeatTime: datetime | None = None
        StartExecTime: datetime | None = None
        EndExecTime: datetime | None = None
        LastUpdateTime: datetime | None = None

    success: dict[int, SetJobStatusReturnSuccess]
    failed: dict[int, dict[str, str]]
