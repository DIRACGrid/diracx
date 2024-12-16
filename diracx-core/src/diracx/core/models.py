from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field
from typing_extensions import TypedDict


class ScalarSearchOperator(StrEnum):
    EQUAL = "eq"
    NOT_EQUAL = "neq"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    LIKE = "like"


class VectorSearchOperator(StrEnum):
    IN = "in"
    NOT_IN = "not in"


class ScalarSearchSpec(TypedDict):
    parameter: str
    operator: ScalarSearchOperator
    value: str | int


class VectorSearchSpec(TypedDict):
    parameter: str
    operator: VectorSearchOperator
    values: list[str] | list[int]


SearchSpec = ScalarSearchSpec | VectorSearchSpec


class SortDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


# TODO: TypedDict vs pydantic?
class SortSpec(TypedDict):
    parameter: str
    direction: SortDirection


class TokenResponse(BaseModel):
    # Based on RFC 6749
    access_token: str
    expires_in: int
    token_type: str = "Bearer"
    refresh_token: str | None = None


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
    checksum: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    size: int = Field(ge=1)
    format: SandboxFormat


class SandboxType(StrEnum):
    Input: str = "Input"
    Output: str = "Output"
