"""Models used to define the data structure of the requests and responses for the DiracX API.

Shared between the client components (cli, api) and services components (db, logic, routers).
"""

# in order to avoid DIRAC from failing to import TokenResponse
# TODO: remove after DIRACGrid/DIRAC#8433
from __future__ import annotations

__all__ = [
    # Auth
    "AccessTokenPayload",
    "GrantType",
    "GroupInfo",
    "InitiateDeviceFlowResponse",
    "Metadata",
    "OpenIDConfiguration",
    "RefreshTokenPayload",
    "TokenPayload",
    "TokenResponse",
    "TokenTypeHint",
    "UserInfo",
    # Job
    "HeartbeatData",
    "InsertedJob",
    "JobAttributes",
    "JobCommand",
    "JobLoggingRecord",
    "JobMetaData",
    "JobMinorStatus",
    "JobParameters",
    "JobStatus",
    "JobStatusReturn",
    "JobStatusUpdate",
    "SetJobStatusReturn",
    # Replica Map
    "ReplicaMap",
    # Sanbox
    "ChecksumAlgorithm",
    "SandboxDownloadResponse",
    "SandboxFormat",
    "SandboxInfo",
    "SandboxType",
    "SandboxUploadResponse",
    # Search
    "ScalarSearchOperator",
    "ScalarSearchSpec",
    "SearchParams",
    "SearchSpec",
    "SortDirection",
    "SortSpec",
    "SummaryParams",
    "VectorSearchOperator",
    "VectorSearchSpec",
]

from .auth import (
    AccessTokenPayload,
    GrantType,
    GroupInfo,
    InitiateDeviceFlowResponse,
    Metadata,
    OpenIDConfiguration,
    RefreshTokenPayload,
    TokenPayload,
    TokenResponse,
    TokenTypeHint,
    UserInfo,
)
from .job import (
    HeartbeatData,
    InsertedJob,
    JobAttributes,
    JobCommand,
    JobLoggingRecord,
    JobMetaData,
    JobMinorStatus,
    JobParameters,
    JobStatus,
    JobStatusReturn,
    JobStatusUpdate,
    SetJobStatusReturn,
)
from .replica_map import ReplicaMap
from .sandbox import (
    ChecksumAlgorithm,
    SandboxDownloadResponse,
    SandboxFormat,
    SandboxInfo,
    SandboxType,
    SandboxUploadResponse,
)
from .search import (
    ScalarSearchOperator,
    ScalarSearchSpec,
    SearchParams,
    SearchSpec,
    SortDirection,
    SortSpec,
    SummaryParams,
    VectorSearchOperator,
    VectorSearchSpec,
)
