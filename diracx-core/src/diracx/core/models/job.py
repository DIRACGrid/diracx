"""Models used to define the data structure of the requests and responses for the DiracX API.

They are shared between the client components (cli, api) and services components (db, logic, routers).
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from .types import UTCDatetime


class InsertedJob(BaseModel):
    JobID: int
    Status: str
    MinorStatus: str
    TimeStamp: UTCDatetime


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
    APPLICATION = "Executing Payload"
    APP_ERRORS = "Application Finished With Errors"
    APP_NOT_FOUND = "Application not found"
    APP_SUCCESS = "Application Finished Successfully"
    APP_THREAD_FAILED = "Application thread failed"
    APP_THREAD_NOT_COMPLETE = "Application thread did not complete"
    DOWNLOADING_INPUT_SANDBOX = "Downloading InputSandbox"
    DOWNLOADING_INPUT_SANDBOX_LFN = "Downloading InputSandbox LFN(s)"
    EXCEPTION_DURING_EXEC = "Exception During Execution"
    EXEC_COMPLETE = "Execution Complete"
    FAILED_DOWNLOADING_INPUT_SANDBOX = "Failed Downloading InputSandbox"
    FAILED_DOWNLOADING_INPUT_SANDBOX_LFN = "Failed Downloading InputSandbox LFN(s)"
    FAILED_SENDING_REQUESTS = "Failed sending requests"
    GOING_RESCHEDULE = "Going to reschedule job"
    ILLEGAL_JOB_JDL = "Illegal Job JDL"
    INPUT_DATA_RESOLUTION = "Resolving Input Data"
    INPUT_NOT_AVAILABLE = "Input Data Not Available"
    JOB_EXCEEDED_CPU = "Job has reached the CPU limit of the queue"
    JOB_EXCEEDED_WALL_CLOCK = "Job has exceeded maximum wall clock time"
    JOB_INITIALIZATION = "Initializing Job"
    JOB_INSUFFICIENT_DISK = "Job has insufficient disk space to continue"
    JOB_WRAPPER_EXECUTION = "JobWrapper execution"
    JOB_WRAPPER_INITIALIZATION = "Job Wrapper Initialization"
    MARKED_FOR_TERMINATION = "Marked for termination"
    MAX_RESCHEDULING = "Maximum of reschedulings reached"
    NO_CANDIDATE_SITE_FOUND = "No candidate sites available"
    OUTPUT_DATA_UPLOADED = "Output Data Uploaded"
    OUTPUT_SANDBOX_UPLOADED = "Output Sandbox Uploaded"
    PENDING_REQUESTS = "Pending Requests"
    PILOT_AGENT_SUBMISSION = "Pilot Agent Submission"
    RECEIVED_KILL_SIGNAL = "Received Kill signal"
    REQUESTS_DONE = "Requests done"
    RESCHEDULED = "Job Rescheduled"
    RESOLVING_OUTPUT_SANDBOX = "Resolving Output Sandbox"
    STALLED_PILOT_NOT_RUNNING = "Job stalled: pilot not running"
    UPLOADING_JOB_OUTPUTS = "Uploading Outputs"
    UPLOADING_OUTPUT_DATA = "Uploading Output Data"
    UPLOADING_OUTPUT_SANDBOX = "Uploading Output Sandbox"
    WATCHDOG_STALLED = "Watchdog identified this job as stalled"


class JobLoggingRecord(BaseModel):
    job_id: int
    status: JobStatus | Literal["idem"]
    minor_status: str
    application_status: str
    date: UTCDatetime
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
    StatusTime: UTCDatetime
    Source: str


class SetJobStatusReturn(BaseModel):
    class SetJobStatusReturnSuccess(BaseModel):
        """Successful new status change."""

        Status: JobStatus | None = None
        MinorStatus: str | None = None
        ApplicationStatus: str | None = None
        HeartBeatTime: UTCDatetime | None = None
        StartExecTime: UTCDatetime | None = None
        EndExecTime: UTCDatetime | None = None
        LastUpdateTime: UTCDatetime | None = None

    success: dict[int, SetJobStatusReturnSuccess]
    failed: dict[int, dict[str, str]]
