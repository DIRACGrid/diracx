from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel as _BaseModel
from pydantic import EmailStr, PrivateAttr, root_validator

from chrishackaton.properties import SecurityProperty


class BaseModel(_BaseModel, extra="forbid", allow_mutation=False):
    @root_validator(pre=True)
    def legacy_adaptor(cls, v):
        """Applies transformations to interpret the legacy DIRAC CFG format"""
        if not os.environ.get("DIRAC_COMPAT_ENABLE_CS_CONVERSION"):
            return v
        for field, hint in cls.__annotations__.items():
            if hint in {"list[str]", "list[SecurityProperty]"} and isinstance(v.get(field), str):
                v[field] = [x.strip() for x in v[field].split(",") if x.strip()]
        return v


class UserConfig(BaseModel):
    CA: str
    DN: str
    Email: EmailStr
    Suspended: Optional[str]
    Quota: Optional[int]
    # TODO: These should be LHCbDIRAC specific
    CERNAccountType: str
    PrimaryCERNAccount: str


class GroupConfig(BaseModel):
    AutoAddVOMS: bool = False
    AutoUploadPilotProxy: bool = False
    AutoUploadProxy: bool = False
    JobShare: Optional[int]
    Properties: list[SecurityProperty]
    Quota: Optional[int]
    Users: list[str]
    AllowBackgroundTQs: bool = False
    VOMSRole: Optional[str]
    AutoSyncVOMS: bool = False


class RegistryConfig(BaseModel):
    DefaultGroup: dict[str, list[str]]
    DefaultVOMSAttribute: str
    DefaultStorageQuota: int
    DefaultProxyLifeTime: int

    Users: dict[str,dict[str,UserConfig]]
    Groups: dict[str,dict[str,GroupConfig]]

    BannedIPs: Any
    Hosts: Any
    VO: Any


class DIRACConfig(BaseModel):
    DefaultGroup: dict[str, str]


class JobMonitoringConfig(BaseModel):
    GlobalJobsInfo: bool = True
    useESForJobParametersFlag: bool = False


class ServicesConfig(BaseModel):
    Catalogs: dict[str, Any] | None
    JobMonitoring: JobMonitoringConfig = JobMonitoringConfig()


class OperationsConfig(BaseModel):
    EnableSecurityLogging: bool = False
    Services: ServicesConfig = ServicesConfig()

    DataConsistency: dict[str, Any] | None
    DataManagement: dict[str, Any] | None
    EMail: dict[str, Any] | None
    ExternalsPolicy: dict[str, Any] | None
    GaudiExecution: dict[str, Any] | None
    Hospital: dict[str, Any] | None
    InputDataPolicy: dict[str, Any] | None
    JobDescription: dict[str, Any] | None
    JobScheduling: dict[str, Any] | None
    JobTypeMapping: dict[str, Any] | None
    LogFiles: dict[str, Any] | None
    LogStorage: dict[str, Any] | None
    Logging: dict[str, Any] | None
    Matching: dict[str, Any] | None
    MonitoringBackends: dict[str, Any] | None
    NagiosConnector: dict[str, Any] | None
    Pilot: dict[str, Any] | None
    Productions: dict[str, Any] | None
    Shares: dict[str, Any] | None
    Shifter: dict[str, Any] | None
    SiteSEMappingByProtocol: dict[str, Any] | None
    TransformationPlugins: dict[str, Any] | None
    Transformations: dict[str, Any] | None
    ResourceStatus: dict[str, Any] | None


class Config(BaseModel):
    Registry: RegistryConfig
    DIRAC: DIRACConfig
    # TODO: Should this be split by vo rather than setup?
    Operations: dict[str, OperationsConfig]

    LocalSite: Any
    LogLevel: Any
    MCTestingDestination: Any
    Resources: Any
    Systems: Any
    WebApp: Any

    _hexsha: str = PrivateAttr()
    _modified: datetime = PrivateAttr()
