from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, TypedDict

from pydantic import BaseModel, Field


class ScalarSearchOperator(StrEnum):
    EQUAL = "eq"
    NOT_EQUAL = "neq"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    LIKE = "like"


class VectorSearchOperator(StrEnum):
    IN = "in"
    NOT_IN = "not in"


# TODO: TypedDict vs pydantic?
class SortSpec(TypedDict):
    parameter: str
    direction: Literal["asc"] | Literal["dsc"]


class ScalarSearchSpec(TypedDict):
    parameter: str
    operator: ScalarSearchOperator
    value: str | int


class VectorSearchSpec(TypedDict):
    parameter: str
    operator: VectorSearchOperator
    values: list[str] | list[int]


SearchSpec = ScalarSearchSpec | VectorSearchSpec


class TokenResponse(BaseModel):
    # Based on RFC 6749
    access_token: str
    expires_in: int
    token_type: str = "Bearer"
    refresh_token: str | None


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


class JobStatusUpdate(BaseModel):
    status: JobStatus | None = Field(
        default=None,
        alias="Status",
    )
    minor_status: str | None = Field(
        default=None,
        alias="MinorStatus",
    )
    application_status: str | None = Field(
        default=None,
        alias="ApplicationStatus",
    )
    status_source: str = Field(
        alias="StatusSource",
        default="Unknown",
    )


class LimitedJobStatusReturn(BaseModel):
    status: JobStatus = Field(alias="Status")
    minor_status: str = Field(alias="MinorStatus")
    application_status: str = Field(alias="ApplicationStatus")


class JobStatusReturn(LimitedJobStatusReturn):
    status_time: datetime = Field(alias="StatusTime")
    status_source: str = Field(alias="StatusSource")


class SetJobStatusReturn(BaseModel):
    status: JobStatus | None = Field(alias="Status")
    minor_status: str | None = Field(alias="MinorStatus")
    application_status: str | None = Field(alias="ApplicationStatus")
    heartbeat_time: datetime | None = Field(alias="HeartBeatTime")
    start_exec_time: datetime | None = Field(alias="StartExecTime")
    end_exec_time: datetime | None = Field(alias="EndExecTime")
    last_update_time: datetime | None = Field(alias="LastUpdateTime")


class UserInfo(BaseModel):
    sub: str  # dirac generated vo:sub
    preferred_username: str
    dirac_group: str
    vo: str


class ChecksumAlgorithm(StrEnum):
    SHA256 = "sha256"


class SandboxFormat(StrEnum):
    TAR_BZ2 = "tar.bz2"


class SandboxInfo(BaseModel):
    checksum_algorithm: ChecksumAlgorithm
    checksum: str = Field(pattern=r"^[0-f]{64}$")
    size: int = Field(ge=1)
    format: SandboxFormat
