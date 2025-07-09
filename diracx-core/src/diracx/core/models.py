"""Models are used to define the data structure of the requests and responses
for the DiracX API. They are shared between the client components (cli, api) and
services components (db, logic, routers).
"""

from __future__ import annotations

import uuid as std_uuid
from datetime import datetime
from enum import StrEnum, auto
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic_core import CoreSchema, core_schema
from typing_extensions import TypedDict
from uuid_utils import UUID as _UUID


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
    value: str | int | datetime | bytes


class VectorSearchSpec(TypedDict):
    parameter: str
    operator: VectorSearchOperator
    values: list[str] | list[int] | list[bytes]


SearchSpec = ScalarSearchSpec | VectorSearchSpec


class SortDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


# TODO: TypedDict vs pydantic?
class SortSpec(TypedDict):
    parameter: str
    direction: SortDirection


class InsertedJob(TypedDict):
    JobID: int
    Status: str
    MinorStatus: str
    TimeStamp: datetime


class JobSummaryParams(BaseModel):
    grouping: list[str]
    search: list[SearchSpec] = []
    # TODO: Add more validation


class SearchParams(BaseModel):
    parameters: list[str] | None = None
    search: list[SearchSpec] = []
    sort: list[SortSpec] = []
    distinct: bool = False
    # TODO: Add more validation


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
    Input = "Input"
    Output = "Output"


class SandboxDownloadResponse(BaseModel):
    url: str
    expires_in: int


class SandboxUploadResponse(BaseModel):
    pfn: str
    url: str | None = None
    fields: dict[str, str] = {}


class UUID(_UUID):
    """Subclass of uuid_utils.UUID to add pydantic support."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Use the stdlib uuid.UUID schema for validation and serialization."""
        std_schema = handler(std_uuid.UUID)

        def to_uuid_utils(u: std_uuid.UUID) -> UUID:
            return cls(str(u))

        return core_schema.no_info_after_validator_function(to_uuid_utils, std_schema)

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> dict[str, Any]:
        """Return the stdlib uuid.UUID schema for JSON serialization."""
        return handler(core_schema)


class GrantType(StrEnum):
    """Grant types for OAuth2."""

    authorization_code = "authorization_code"
    device_code = "urn:ietf:params:oauth:grant-type:device_code"
    refresh_token = "refresh_token"  # noqa: S105   # False positive of Bandit about hard coded password


class InitiateDeviceFlowResponse(TypedDict):
    """Response for the device flow initiation."""

    user_code: str
    device_code: str
    verification_uri_complete: str
    verification_uri: str
    expires_in: int


class OpenIDConfiguration(TypedDict):
    issuer: str
    token_endpoint: str
    userinfo_endpoint: str
    authorization_endpoint: str
    device_authorization_endpoint: str
    revocation_endpoint: str
    jwks_uri: str
    grant_types_supported: list[str]
    scopes_supported: list[str]
    response_types_supported: list[str]
    token_endpoint_auth_signing_alg_values_supported: list[str]
    token_endpoint_auth_methods_supported: list[str]
    code_challenge_methods_supported: list[str]


class BaseTokenPayload(TypedDict):
    """This class helps having pilot and user tokens without code duplication."""

    jti: str
    exp: datetime


class TokenPayload(BaseTokenPayload):
    dirac_policies: dict


class TokenResponse(BaseModel):
    # Based on RFC 6749
    access_token: str
    expires_in: int
    token_type: str = "Bearer"  # noqa: S105
    refresh_token: str | None = None


class AccessTokenPayload(TokenPayload):
    sub: str
    vo: str
    iss: str
    dirac_properties: list[str]
    preferred_username: str
    dirac_group: str


class RefreshTokenPayload(TokenPayload):
    legacy_exchange: bool


class SupportInfo(TypedDict):
    message: str
    webpage: str | None
    email: str | None


class GroupInfo(TypedDict):
    properties: list[str]


class VOInfo(TypedDict):
    groups: dict[str, GroupInfo]
    support: SupportInfo
    default_group: str


class Metadata(TypedDict):
    virtual_organizations: dict[str, VOInfo]


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


class PilotFieldsMapping(BaseModel, extra="forbid"):
    """All the fields that a user can modify on a Pilot (except PilotStamp)."""

    PilotStamp: str
    StatusReason: Optional[str] = None
    Status: Optional[PilotStatus] = None
    BenchMark: Optional[float] = None
    DestinationSite: Optional[str] = None
    Queue: Optional[str] = None
    GridSite: Optional[str] = None
    GridType: Optional[str] = None
    AccountingSent: Optional[bool] = None
    CurrentJobID: Optional[int] = None


class PilotStatus(StrEnum):
    #: The pilot has been generated and is transferred to a remote site:
    SUBMITTED = "Submitted"
    #: The pilot is waiting for a computing resource in a batch queue:
    WAITING = "Waiting"
    #: The pilot is running a payload on a worker node:
    RUNNING = "Running"
    #: The pilot finished its execution:
    DONE = "Done"
    #: The pilot execution failed:
    FAILED = "Failed"
    #: The pilot was deleted:
    DELETED = "Deleted"
    #: The pilot execution was aborted:
    ABORTED = "Aborted"
    #: Cannot get information about the pilot status:
    UNKNOWN = "Unknown"


class PilotSecretConstraints(TypedDict, total=False):
    VOs: list[str]  # Authorize only a list of VOs
    PilotStamps: list[str]  # Authorize only a list of stamps
    Sites: list[str]  # Authorize only a list of sites
    # ...
    # We can add constraints here


class TokenType(StrEnum):
    # Pilot token
    PILOT_TOKEN = auto()
    # User token
    USER_TOKEN = auto()


class PilotSecretsInfo(BaseModel):
    pilot_secret: str
    pilot_secret_expires_in: int


class PilotAccessTokenPayload(BaseTokenPayload):
    sub: str
    vo: str
    iss: str
    pilot_stamp: str


class PilotInfo(BaseModel):
    pilot_stamp: str
    vo: str
    sub: str


class PilotRefreshTokenPayload(BaseTokenPayload):
    legacy_exchange: bool


class PilotCredentialsInfo(PilotSecretsInfo):
    pilot_stamp: str
