from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from pydantic import BaseModel as _BaseModel
from pydantic import EmailStr, PrivateAttr, root_validator

from ..properties import SecurityProperty


class BaseModel(_BaseModel, extra="forbid", allow_mutation=False):
    @root_validator(pre=True)
    def legacy_adaptor(cls, v):
        """Applies transformations to interpret the legacy DIRAC CFG format"""
        if not os.environ.get("DIRAC_COMPAT_ENABLE_CS_CONVERSION"):
            return v
        # If we're running with DIRAC_COMPAT_ENABLE_CS_CONVERSION set we apply
        # some hacky transformations to the content to ease the transition from
        # a CFG file. This is done by analysing the type hints as strings
        # though ideally we should parse the type hints properly.
        for field, hint in cls.__annotations__.items():
            # Convert comma separated lists to actual lists
            if hint in {"list[str]", "list[SecurityProperty]"} and isinstance(v.get(field), str):
                v[field] = [x.strip() for x in v[field].split(",") if x.strip()]
            # If the field is optional and the value is "None" convert it to None
            if "| None" in hint and field in v:
                if v[field] == "None":
                    v[field] = None
        return v


class UserConfig(BaseModel):
    CA: str
    DN: str
    PreferedUsername: str
    Email: EmailStr | None
    Suspended: list[str] = []
    Quota: int | None = None
    # TODO: These should be LHCbDIRAC specific
    CERNAccountType: str | None = None
    PrimaryCERNAccount: str | None = None


class GroupConfig(BaseModel):
    AutoAddVOMS: bool = False
    AutoUploadPilotProxy: bool = False
    AutoUploadProxy: bool = False
    JobShare: int | None
    Properties: list[SecurityProperty]
    Quota: int | None
    Users: list[str]
    AllowBackgroundTQs: bool = False
    VOMSRole: str | None
    AutoSyncVOMS: bool = False


class IdpConfig(BaseModel):
    URL: str
    ClientID: str

    @property
    def server_metadata_url(self):
        return f"{self.URL}/.well-known/openid-configuration"


class RegistryConfig(BaseModel):
    IdP: IdpConfig
    DefaultGroup: str
    DefaultStorageQuota: float = 0
    DefaultProxyLifeTime: int = 12 * 60 * 60

    Users: dict[str, UserConfig]
    Groups: dict[str, GroupConfig]


class DIRACConfig(BaseModel):
    pass


class JobMonitoringConfig(BaseModel):
    GlobalJobsInfo: bool = True
    useESForJobParametersFlag: bool = False


class ServicesConfig(BaseModel):
    Catalogs: dict[str, Any] | None
    JobMonitoring: JobMonitoringConfig = JobMonitoringConfig()


class OperationsConfig(BaseModel):
    EnableSecurityLogging: bool = False
    Services: ServicesConfig = ServicesConfig()

    Cloud: dict[str, Any] | None = None
    DataConsistency: dict[str, Any] | None = None
    DataManagement: dict[str, Any] | None = None
    EMail: dict[str, Any] | None = None
    ExternalsPolicy: dict[str, Any] | None = None
    GaudiExecution: dict[str, Any] | None = None
    Hospital: dict[str, Any] | None = None
    InputDataPolicy: dict[str, Any] | None = None
    JobDescription: dict[str, Any] | None = None
    JobScheduling: dict[str, Any] | None = None
    JobTypeMapping: dict[str, Any] | None = None
    LogFiles: dict[str, Any] | None = None
    LogStorage: dict[str, Any] | None = None
    Logging: dict[str, Any] | None = None
    Matching: dict[str, Any] | None = None
    MonitoringBackends: dict[str, Any] | None = None
    NagiosConnector: dict[str, Any] | None = None
    Pilot: dict[str, Any] | None = None
    Productions: dict[str, Any] | None = None
    Shares: dict[str, Any] | None = None
    Shifter: dict[str, Any] | None = None
    SiteSEMappingByProtocol: dict[str, Any] | None = None
    TransformationPlugins: dict[str, Any] | None = None
    Transformations: dict[str, Any] | None = None
    ResourceStatus: dict[str, Any] | None = None


class Config(BaseModel):
    Registry: dict[str, RegistryConfig]
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
