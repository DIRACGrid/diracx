"""Models used to define the data structure of the requests and responses for the DiracX API.

Shared between the client components (cli, api) and services components (db, logic, routers).
"""

# in order to avoid DIRAC from failing to import TokenResponse
from __future__ import annotations

__all__ = [
    "AccessTokenPayload",
    "ChecksumAlgorithm",
    "GrantType",
    "GroupInfo",
    "HeartbeatData",
    "InitiateDeviceFlowResponse",
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
    "Metadata",
    "OpenIDConfiguration",
    "RefreshTokenPayload",
    "ReplicaMap",
    "SandboxDownloadResponse",
    "SandboxFormat",
    "SandboxInfo",
    "SandboxType",
    "SandboxUploadResponse",
    "ScalarSearchOperator",
    "ScalarSearchSpec",
    "SearchParams",
    "SearchSpec",
    "SetJobStatusReturn",
    "SortDirection",
    "SortSpec",
    "SummaryParams",
    "TokenPayload",
    "TokenResponse",
    "TokenTypeHint",
    "UserInfo",
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
