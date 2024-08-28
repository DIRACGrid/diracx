from __future__ import annotations

import os
from datetime import datetime
from typing import Annotated, Any, TypeVar

from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict, EmailStr, Field, PrivateAttr, model_validator
from pydantic.functional_serializers import PlainSerializer

from ..properties import SecurityProperty

# By default the serialization of set doesn't have a well defined ordering so
# we have to use a custom type to make sure the values are always sorted.
T = TypeVar("T")
SerializableSet = Annotated[
    set[T], PlainSerializer(sorted, return_type=list[T], when_used="json-unless-none")
]


class BaseModel(_BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def legacy_adaptor(cls, v):
        """Apply transformations to interpret the legacy DIRAC CFG format."""
        if not os.environ.get("DIRAC_COMPAT_ENABLE_CS_CONVERSION"):
            return v
        # If we're running with DIRAC_COMPAT_ENABLE_CS_CONVERSION set we apply
        # some hacky transformations to the content to ease the transition from
        # a CFG file. This is done by analysing the type hints as strings
        # though ideally we should parse the type hints properly.
        for field, hint in cls.__annotations__.items():
            # Convert comma separated lists to actual lists
            if hint.startswith("set"):
                raise NotImplementedError("Use SerializableSet instead!")
            if hint in {
                "list[str]",
                "SerializableSet[str]",
                "SerializableSet[SecurityProperty]",
            } and isinstance(v.get(field), str):
                v[field] = [x.strip() for x in v[field].split(",") if x.strip()]
            # If the field is optional and the value is "None" convert it to None
            if "| None" in hint and field in v:
                if v[field] == "None":
                    v[field] = None
        return v


class UserConfig(BaseModel):
    PreferedUsername: str
    DNs: list[str] = []
    Email: EmailStr | None = None
    Suspended: list[str] = []
    Quota: int | None = None
    # TODO: These should be LHCbDIRAC specific
    CERNAccountType: str | None = None
    PrimaryCERNAccount: str | None = None


class GroupConfig(BaseModel):
    AutoAddVOMS: bool = False
    AutoUploadPilotProxy: bool = False
    AutoUploadProxy: bool = False
    JobShare: int = 1000
    Properties: SerializableSet[SecurityProperty]
    Quota: int | None = None
    Users: SerializableSet[str]
    AllowBackgroundTQs: bool = False
    VOMSRole: str | None = None
    AutoSyncVOMS: bool = False


class IdpConfig(BaseModel):
    URL: str
    ClientID: str

    @property
    def server_metadata_url(self):
        return f"{self.URL}/.well-known/openid-configuration"


class SupportInfo(BaseModel):
    Email: str | None = None
    Webpage: str | None = None
    Message: str = "Please contact system administrator"


class RegistryConfig(BaseModel):
    IdP: IdpConfig
    Support: SupportInfo = Field(default_factory=SupportInfo)
    DefaultGroup: str
    DefaultStorageQuota: float = 0
    DefaultProxyLifeTime: int = 12 * 60 * 60
    VOMSName: str | None = None

    Users: dict[str, UserConfig]
    Groups: dict[str, GroupConfig]

    def sub_from_preferred_username(self, preferred_username: str) -> str:
        """Get the user sub from the preferred username.

        TODO: This could easily be cached or optimised
        """
        for sub, user in self.Users.items():
            if user.PreferedUsername == preferred_username:
                return sub
        raise KeyError(f"User {preferred_username} not found in registry")


class DIRACConfig(BaseModel):
    pass


class JobMonitoringConfig(BaseModel):
    GlobalJobsInfo: bool = True
    useESForJobParametersFlag: bool = False


class JobSchedulingConfig(BaseModel):
    EnableSharesCorrection: bool = False


class ServicesConfig(BaseModel):
    Catalogs: dict[str, Any] | None = None
    JobMonitoring: JobMonitoringConfig = JobMonitoringConfig()
    JobScheduling: JobSchedulingConfig = JobSchedulingConfig()


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

    LocalSite: Any = None
    LogLevel: Any = None
    MCTestingDestination: Any = None
    Resources: Any = None
    Systems: Any = None
    WebApp: Any = None

    # These 2 parameters are used for client side caching
    # see the "/config/" route for details

    # hash for a unique representation of the config version
    _hexsha: str = PrivateAttr()
    # modification date
    _modified: datetime = PrivateAttr()
