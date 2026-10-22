"""Models used to define the data structure of the requests and responses for the DiracX API.

They are shared between the client components (cli, api) and services components (db, logic, routers).
"""

from __future__ import annotations

import math
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import Field, field_validator, model_validator

from ._base import BaseModel
from .types import UTCDatetime


class InsertedJob(BaseModel):
    """Information returned for a newly inserted job."""

    job_id: int = Field(alias="JobID")
    """Identifier assigned to the inserted job."""
    status: str = Field(alias="Status")
    """Current job status."""
    minor_status: str = Field(alias="MinorStatus")
    """More detailed job status."""
    time_stamp: UTCDatetime = Field(alias="TimeStamp")
    """Time associated with the insertion."""


class HeartbeatData(BaseModel, extra="forbid", allow_inf_nan=False):
    """Runtime resource and output data reported by a job heartbeat."""

    load_average: float | None = Field(None, alias="LoadAverage")
    """System load average."""
    memory_used: float | None = Field(None, alias="MemoryUsed")
    """Memory used by the job."""
    vsize: float | None = Field(None, alias="Vsize")
    """Virtual memory size used by the job."""
    available_disk_space: float | None = Field(None, alias="AvailableDiskSpace")
    """Available disk space."""
    cpu_consumed: float | None = Field(None, alias="CPUConsumed")
    """CPU time consumed by the job."""
    wall_clock_time: float | None = Field(None, alias="WallClockTime")
    """Wall-clock time consumed by the job."""
    standard_output: str | None = Field(None, alias="StandardOutput")
    """Standard output reported by the job."""


class JobCommand(BaseModel):
    """Command to apply to a job."""

    job_id: int
    """Identifier of the target job."""
    command: Literal["Kill"]
    """Command to execute."""
    arguments: str | None = None
    """Optional command arguments."""


def _is_non_finite(value: Any) -> bool:
    return isinstance(value, float) and not math.isfinite(value)


def _ensure_finite_numbers(value: Any, path: str) -> None:
    """Raise ValueError if a (possibly nested) value contains NaN or infinity.

    Only needed for the extra fields, as ``allow_inf_nan`` cannot apply to them.
    """
    if _is_non_finite(value):
        raise ValueError(f"{path}: non-finite numbers are not supported")
    if isinstance(value, dict):
        for key, item in value.items():
            _ensure_finite_numbers(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            _ensure_finite_numbers(item, f"{path}[{i}]")


class JobParameters(
    BaseModel,
    populate_by_name=True,
    extra="allow",
    allow_inf_nan=False,
):
    """Some of the most important parameters that can be set for a job.

    Extra fields are allowed and must contain values that can be represented
    safely in JSON.
    """

    timestamp: UTCDatetime | None = None
    """Time associated with the job parameters."""
    cpu_normalization_factor: int | None = Field(None, alias="CPUNormalizationFactor")
    """CPU normalization factor."""
    norm_cpu_time_s: int | None = Field(None, alias="NormCPUTime(s)")
    """Normalized CPU time in seconds."""
    total_cpu_time_s: int | None = Field(None, alias="TotalCPUTime(s)")
    """Total CPU time in seconds."""
    host_name: str | None = Field(None, alias="HostName")
    """Host running the job."""
    grid_ce: str | None = Field(None, alias="GridCE")
    """Grid computing element."""
    model_name: str | None = Field(None, alias="ModelName")
    """Computing model name."""
    pilot_agent: str | None = Field(None, alias="PilotAgent")
    """Pilot agent name."""
    pilot_reference: str | None = Field(None, alias="Pilot_Reference")
    """Pilot reference."""
    memory_mb: int | None = Field(None, alias="Memory(MB)")
    """Memory used in megabytes."""
    local_account: str | None = Field(None, alias="LocalAccount")
    """Local account used by the job."""
    payload_pid: int | None = Field(None, alias="PayloadPID")
    """Payload process identifier."""
    ce_queue: str | None = Field(None, alias="CEQueue")
    """Computing element queue."""
    batch_system: str | None = Field(None, alias="BatchSystem")
    """Batch system name."""

    @field_validator(
        "cpu_normalization_factor", "norm_cpu_time_s", "total_cpu_time_s", mode="before"
    )
    @classmethod
    def convert_cpu_fields_to_int(cls, v):
        """Convert CPU-related values to integers.

        Args:
            v: CPU-related value to convert.

        Returns:
            The converted integer value, or the original value when conversion
            is not applicable.
        """
        if v is None:
            return v
        if isinstance(v, str):
            try:
                v = float(v)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Cannot convert '{v}' to integer") from e
        # int() raises OverflowError for infinity, which pydantic does not
        # report as a validation error
        if _is_non_finite(v):
            raise ValueError("non-finite numbers are not supported")
        if isinstance(v, (int, float)):
            return int(v)
        return v

    @model_validator(mode="after")
    def validate_extra_fields_are_json_safe(self) -> Self:
        """Reject extra field values which cannot be represented in strict JSON.

        Python's JSON parser accepts NaN and (-)Infinity so such values survive
        request parsing, but OpenSearch rejects documents containing them.

        Returns:
            The validated job parameters.
        """
        if self.model_extra:
            for name, value in self.model_extra.items():
                _ensure_finite_numbers(value, name)
        return self


class JobAttributes(BaseModel, populate_by_name=True, extra="forbid"):
    """All the attributes that can be set for a job."""

    job_type: str | None = Field(None, alias="JobType")
    """Type of the job."""
    job_group: str | None = Field(None, alias="JobGroup")
    """Group associated with the job."""
    site: str | None = Field(None, alias="Site")
    """Site associated with the job."""
    job_name: str | None = Field(None, alias="JobName")
    """User-defined job name."""
    owner: str | None = Field(None, alias="Owner")
    """User who owns the job."""
    owner_group: str | None = Field(None, alias="OwnerGroup")
    """Group of the job owner."""
    vo: str | None = Field(None, alias="VO")
    """Virtual organization associated with the job."""
    submission_time: UTCDatetime | None = Field(None, alias="SubmissionTime")
    """Time when the job was submitted."""
    reschedule_time: UTCDatetime | None = Field(None, alias="RescheduleTime")
    """Time when the job was rescheduled."""
    last_update_time: UTCDatetime | None = Field(None, alias="LastUpdateTime")
    """Time of the last job update."""
    start_exec_time: UTCDatetime | None = Field(None, alias="StartExecTime")
    """Time when job execution started."""
    heart_beat_time: UTCDatetime | None = Field(None, alias="HeartBeatTime")
    """Time of the last heartbeat."""
    end_exec_time: UTCDatetime | None = Field(None, alias="EndExecTime")
    """Time when job execution ended."""
    status: str | None = Field(None, alias="Status")
    """Current job status."""
    minor_status: str | None = Field(None, alias="MinorStatus")
    """More detailed job status."""
    application_status: str | None = Field(None, alias="ApplicationStatus")
    """Application-specific job status."""
    user_priority: int | None = Field(None, alias="UserPriority")
    """Priority assigned by the user."""
    reschedule_counter: int | None = Field(None, alias="RescheduleCounter")
    """Number of times the job was rescheduled."""
    verified_flag: bool | None = Field(None, alias="VerifiedFlag")
    """Whether the job has been verified."""
    accounted_flag: bool | str | None = Field(None, alias="AccountedFlag")
    """Whether the job has been accounted for."""


class JobMetaData(JobAttributes, JobParameters, extra="allow"):
    """A model that combines both job attributes and job parameters."""


class JobStatus(StrEnum):
    """Lifecycle statuses for a job."""

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
    """Additional status values describing job scheduling outcomes."""

    MAX_RESCHEDULING = "Maximum of reschedulings reached"
    RESCHEDULED = "Job Rescheduled"


class JobLoggingRecord(BaseModel):
    """Record of a job status change written to the logging store."""

    job_id: int
    """Identifier of the job."""
    status: JobStatus | Literal["idem"]
    """New job status."""
    minor_status: str
    """More detailed job status."""
    application_status: str
    """Application-specific status."""
    date: UTCDatetime
    """Time of the status change."""
    source: str
    """Source of the status change."""


class JobStatusUpdate(BaseModel):
    """Requested update to a job's status information."""

    status: JobStatus | None = Field(None, alias="Status")
    """New job status."""
    minor_status: str | None = Field(None, alias="MinorStatus")
    """More detailed job status."""
    application_status: str | None = Field(None, alias="ApplicationStatus")
    """Application-specific status."""
    source: str = Field("Unknown", alias="Source")
    """Source of the status update."""


class LimitedJobStatusReturn(BaseModel):
    """Status information returned without timing or source details."""

    status: JobStatus = Field(alias="Status")
    """Current job status."""
    minor_status: str = Field(alias="MinorStatus")
    """More detailed job status."""
    application_status: str = Field(alias="ApplicationStatus")
    """Application-specific status."""


class JobStatusReturn(LimitedJobStatusReturn):
    """Status information returned with timing and source details."""

    status_time: UTCDatetime = Field(alias="StatusTime")
    """Time associated with the status."""
    source: str = Field(alias="Source")
    """Source of the status information."""


class SetJobStatusReturn(BaseModel):
    """Result of applying a status update to one or more jobs."""

    class SetJobStatusReturnSuccess(BaseModel):
        """Status information for a successful status change."""

        status: JobStatus | None = Field(None, alias="Status")
        """New job status."""
        minor_status: str | None = Field(None, alias="MinorStatus")
        """More detailed job status."""
        application_status: str | None = Field(None, alias="ApplicationStatus")
        """Application-specific status."""
        heart_beat_time: UTCDatetime | None = Field(None, alias="HeartBeatTime")
        """Time of the heartbeat."""
        start_exec_time: UTCDatetime | None = Field(None, alias="StartExecTime")
        """Time when execution started."""
        end_exec_time: UTCDatetime | None = Field(None, alias="EndExecTime")
        """Time when execution ended."""
        last_update_time: UTCDatetime | None = Field(None, alias="LastUpdateTime")
        """Time of the last update."""

    success: dict[int, SetJobStatusReturnSuccess]
    """Successful status updates keyed by job identifier."""
    failed: dict[int, dict[str, str]]
    """Failed status updates keyed by job identifier."""
