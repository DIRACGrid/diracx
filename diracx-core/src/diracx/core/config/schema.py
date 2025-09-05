from __future__ import annotations

import os
from datetime import datetime
from typing import Annotated, Any, MutableMapping, TypeVar

from pydantic import BaseModel as _BaseModel
from pydantic import (
    ConfigDict,
    EmailStr,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)
from pydantic.functional_serializers import PlainSerializer

from ..properties import SecurityProperty
from ..utils import recursive_merge

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

    Users: MutableMapping[str, UserConfig]
    Groups: MutableMapping[str, GroupConfig]

    def sub_from_preferred_username(self, preferred_username: str) -> str:
        """Get the user sub from the preferred username.

        TODO: This could easily be cached or optimised
        """
        for sub, user in self.Users.items():
            if user.PreferedUsername == preferred_username:
                return sub
        raise KeyError(f"User {preferred_username} not found in registry")


class DIRACConfig(BaseModel):
    NoSetup: bool = False


class JobMonitoringConfig(BaseModel):
    GlobalJobsInfo: bool = True


class JobSchedulingConfig(BaseModel):
    EnableSharesCorrection: bool = False
    MaxRescheduling: int = 3


class ServicesConfig(BaseModel):
    Catalogs: MutableMapping[str, Any] | None = None
    JobMonitoring: JobMonitoringConfig = JobMonitoringConfig()
    JobScheduling: JobSchedulingConfig = JobSchedulingConfig()


class JobDescriptionConfig(BaseModel):
    DefaultCPUTime: int = 86400
    DefaultPriority: int = 1
    MinCPUTime: int = 100
    MinPriority: int = 0
    MaxCPUTime: int = 500000
    MaxPriority: int = 10
    MaxInputData: int = 100
    AllowedJobTypes: list[str] = ["User", "Test", "Hospital"]


class InputDataPolicyConfig(BaseModel):
    InputDataModule: str = "LocalInputData"


class ExternalsPolicyConfig(BaseModel):
    SoftwareDistModule: str = "LocalSoftwareDist"


class OperationsConfig(BaseModel):
    EnableSecurityLogging: bool = False
    ExternalsPolicy: ExternalsPolicyConfig = ExternalsPolicyConfig()
    InputDataPolicy: InputDataPolicyConfig = InputDataPolicyConfig()
    JobDescription: JobDescriptionConfig = JobDescriptionConfig()
    Services: ServicesConfig = ServicesConfig()

    Cloud: MutableMapping[str, Any] | None = None
    DataConsistency: MutableMapping[str, Any] | None = None
    DataManagement: MutableMapping[str, Any] | None = None
    EMail: MutableMapping[str, Any] | None = None
    GaudiExecution: MutableMapping[str, Any] | None = None
    Hospital: MutableMapping[str, Any] | None = None
    JobScheduling: MutableMapping[str, Any] | None = None
    JobTypeMapping: MutableMapping[str, Any] | None = None
    LogFiles: MutableMapping[str, Any] | None = None
    LogStorage: MutableMapping[str, Any] | None = None
    Logging: MutableMapping[str, Any] | None = None
    Matching: MutableMapping[str, Any] | None = None
    MonitoringBackends: MutableMapping[str, Any] | None = None
    NagiosConnector: MutableMapping[str, Any] | None = None
    Pilot: MutableMapping[str, Any] | None = None
    Productions: MutableMapping[str, Any] | None = None
    Shares: MutableMapping[str, Any] | None = None
    Shifter: MutableMapping[str, Any] | None = None
    SiteSEMappingByProtocol: MutableMapping[str, Any] | None = None
    TransformationPlugins: MutableMapping[str, Any] | None = None
    Transformations: MutableMapping[str, Any] | None = None
    ResourceStatus: MutableMapping[str, Any] | None = None


class ResourcesComputingConfig(BaseModel):
    # TODO: Remove this once the model is extended to support everything
    model_config = ConfigDict(extra="ignore", frozen=True)

    # TODO: Figure out how to remove this in LHCbDIRAC and then consider
    # constraining there to be at least one entry
    OSCompatibility: MutableMapping[str, set[str]] = {}

    @field_validator("OSCompatibility", mode="before")
    @classmethod
    def legacy_adaptor_os_compatibility(cls, v: Any) -> Any:
        """Apply transformations to interpret the legacy DIRAC CFG format."""
        if not os.environ.get("DIRAC_COMPAT_ENABLE_CS_CONVERSION"):
            return v
        os_compatibility = v.get("OSCompatibility", {})
        for k, v in os_compatibility.items():
            os_compatibility[k] = set(v.replace(" ", "").split(","))
        return os_compatibility

    @field_validator("OSCompatibility")
    @classmethod
    def ensure_self_compatibility(cls, v: dict[str, set[str]]) -> dict[str, set[str]]:
        """Ensure platforms are compatible with themselves."""
        for platform, compatible_platforms in v.items():
            compatible_platforms.add(platform)
        return v


class ResourcesConfig(BaseModel):
    # TODO: Remove this once the model is extended to support everything
    model_config = ConfigDict(extra="ignore", frozen=True)

    Computing: ResourcesComputingConfig = ResourcesComputingConfig()


class Config(BaseModel):
    DIRAC: DIRACConfig
    Operations: MutableMapping[str, OperationsConfig]
    Registry: MutableMapping[str, RegistryConfig]
    Resources: ResourcesConfig = ResourcesConfig()

    LocalSite: Any = None
    LogLevel: Any = None
    MCTestingDestination: Any = None
    Systems: Any | None = None
    WebApp: Any = None

    @model_validator(mode="before")
    @classmethod
    def ensure_operations_defaults(cls, v: dict[str, Any]):
        """Merge the Defaults entry into the VO-specific config under Operations."""
        operations = v.setdefault("Operations", {})
        if os.environ.get("DIRAC_COMPAT_ENABLE_CS_CONVERSION"):
            # The Defaults entry should be kept and not merged into the VO-specific
            # config as we want the "human readable" config to still contain it
            defaults = {}
        else:
            # Remove the Defaults entry
            defaults = operations.pop("Defaults", {})
        # Ensure an Operations entry exists for each VO
        # Defaults are automatically merged into each VO-specific config
        for vo in v.get("Registry", {}):
            operations[vo] = recursive_merge(defaults, operations.get(vo, {}))
        return v

    # These 2 parameters are used for client side caching
    # see the "/config/" route for details

    # hash for a unique representation of the config version
    _hexsha: str = PrivateAttr()
    # modification date
    _modified: datetime = PrivateAttr()
