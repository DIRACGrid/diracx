"""Models used to define the data structure of the requests and responses for the DiracX API.

They are shared between the client components (cli, api) and services components (db, logic, routers).
"""

from __future__ import annotations

import math
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, field_validator, model_validator

from .types import UTCDatetime


class InsertedJob(BaseModel):
    """Information returned for a newly inserted job.

    Attributes:
        job_id: Identifier assigned to the inserted job.
        status: Current job status.
        minor_status: More detailed job status.
        time_stamp: Time associated with the insertion.
    """

    job_id: int = Field(alias="JobID")
    status: str = Field(alias="Status")
    minor_status: str = Field(alias="MinorStatus")
    time_stamp: UTCDatetime = Field(alias="TimeStamp")


class HeartbeatData(BaseModel, extra="forbid", allow_inf_nan=False):
    """Runtime resource and output data reported by a job heartbeat.

    Attributes:
        load_average: System load average.
        memory_used: Memory used by the job.
        vsize: Virtual memory size used by the job.
        available_disk_space: Available disk space.
        cpu_consumed: CPU time consumed by the job.
        wall_clock_time: Wall-clock time consumed by the job.
        standard_output: Standard output reported by the job.
    """

    load_average: float | None = Field(None, alias="LoadAverage")
    memory_used: float | None = Field(None, alias="MemoryUsed")
    vsize: float | None = Field(None, alias="Vsize")
    available_disk_space: float | None = Field(None, alias="AvailableDiskSpace")
    cpu_consumed: float | None = Field(None, alias="CPUConsumed")
    wall_clock_time: float | None = Field(None, alias="WallClockTime")
    standard_output: str | None = Field(None, alias="StandardOutput")


class JobCommand(BaseModel):
    """Command to apply to a job.

    Attributes:
        job_id: Identifier of the target job.
        command: Command to execute.
        arguments: Optional command arguments.
    """

    job_id: int
    command: Literal["Kill"]
    arguments: str | None = None


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
    BaseModel, populate_by_name=True, extra="allow", allow_inf_nan=False
):
    """Some of the most important parameters that can be set for a job.

    Extra fields are allowed and must contain values that can be represented
    safely in JSON.

    Attributes:
        timestamp: Time associated with the job parameters.
        cpu_normalization_factor: CPU normalization factor.
        norm_cpu_time_s: Normalized CPU time in seconds.
        total_cpu_time_s: Total CPU time in seconds.
        host_name: Host running the job.
        grid_ce: Grid computing element.
        model_name: Computing model name.
        pilot_agent: Pilot agent name.
        pilot_reference: Pilot reference.
        memory_mb: Memory used in megabytes.
        local_account: Local account used by the job.
        payload_pid: Payload process identifier.
        ce_queue: Computing element queue.
        batch_system: Batch system name.
        job_type: Type of the job.
        job_status: Current job status.
    """

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
    """All the attributes that can be set for a job.

    Attributes:
        job_type: Type of the job.
        job_group: Group associated with the job.
        site: Site associated with the job.
        job_name: User-defined job name.
        owner: User who owns the job.
        owner_group: Group of the job owner.
        vo: Virtual organization associated with the job.
        submission_time: Time when the job was submitted.
        reschedule_time: Time when the job was rescheduled.
        last_update_time: Time of the last job update.
        start_exec_time: Time when job execution started.
        heart_beat_time: Time of the last heartbeat.
        end_exec_time: Time when job execution ended.
        status: Current job status.
        minor_status: More detailed job status.
        application_status: Application-specific job status.
        user_priority: Priority assigned by the user.
        reschedule_counter: Number of times the job was rescheduled.
        verified_flag: Whether the job has been verified.
        accounted_flag: Whether the job has been accounted for.
    """

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
    """A model that combines both job attributes and job parameters.

    Attributes:
        The attributes and parameters inherited from ``JobAttributes`` and
        ``JobParameters``.
    """


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
    """Record of a job status change written to the logging store.

    Attributes:
        job_id: Identifier of the job.
        status: New job status.
        minor_status: More detailed job status.
        application_status: Application-specific status.
        date: Time of the status change.
        source: Source of the status change.
    """

    job_id: int
    status: JobStatus | Literal["idem"]
    minor_status: str
    application_status: str
    date: UTCDatetime
    source: str


class JobStatusUpdate(BaseModel):
    """Requested update to a job's status information.

    Attributes:
        status: New job status.
        minor_status: More detailed job status.
        application_status: Application-specific status.
        source: Source of the status update.
    """

    status: JobStatus | None = Field(None, alias="Status")
    minor_status: str | None = Field(None, alias="MinorStatus")
    application_status: str | None = Field(None, alias="ApplicationStatus")
    source: str = Field("Unknown", alias="Source")


class LimitedJobStatusReturn(BaseModel):
    """Status information returned without timing or source details.

    Attributes:
        status: Current job status.
        minor_status: More detailed job status.
        application_status: Application-specific status.
    """

    status: JobStatus = Field(alias="Status")
    minor_status: str = Field(alias="MinorStatus")
    application_status: str = Field(alias="ApplicationStatus")


class JobStatusReturn(LimitedJobStatusReturn):
    """Status information returned with timing and source details.

    Attributes:
        status_time: Time associated with the status.
        source: Source of the status information.
    """

    status_time: UTCDatetime = Field(alias="StatusTime")
    source: str = Field(alias="Source")


class SetJobStatusReturn(BaseModel):
    """Result of applying a status update to one or more jobs.

    Attributes:
        success: Successful status updates keyed by job identifier.
        failed: Failed status updates keyed by job identifier.
    """

    class SetJobStatusReturnSuccess(BaseModel):
        """Status information for a successful status change.

        Attributes:
            status: New job status.
            minor_status: More detailed job status.
            application_status: Application-specific status.
            heart_beat_time: Time of the heartbeat.
            start_exec_time: Time when execution started.
            end_exec_time: Time when execution ended.
            last_update_time: Time of the last update.
        """

        status: JobStatus | None = Field(None, alias="Status")
        minor_status: str | None = Field(None, alias="MinorStatus")
        application_status: str | None = Field(None, alias="ApplicationStatus")
        heart_beat_time: UTCDatetime | None = Field(None, alias="HeartBeatTime")
        start_exec_time: UTCDatetime | None = Field(None, alias="StartExecTime")
        end_exec_time: UTCDatetime | None = Field(None, alias="EndExecTime")
        last_update_time: UTCDatetime | None = Field(None, alias="LastUpdateTime")

    success: dict[int, SetJobStatusReturnSuccess]
    failed: dict[int, dict[str, str]]
