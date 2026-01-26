from __future__ import annotations

import os
from datetime import datetime
from functools import cached_property
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
    model_config = ConfigDict(
        extra="forbid", frozen=True, use_attribute_docstrings=True
    )

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
        convertible_hints = {
            "list[str]",
            "SerializableSet[str]",
            "SerializableSet[SecurityProperty]",
        }

        # To support inheritance, we have to find the annotation of all the MRO
        # Skip the first classes (object, Pydantic.BaseModel, our BaseModel )
        mro_annotations = {}
        for mro_cls in cls.__mro__[::-1]:
            if issubclass(mro_cls, BaseModel) and mro_cls != BaseModel:
                mro_annotations.update(mro_cls.__annotations__)

        for field, hint in mro_annotations.items():
            # Convert comma separated lists to actual lists
            if hint.startswith("set"):
                raise NotImplementedError("Use SerializableSet instead!")

            if field not in v:
                continue

            # Get the base hint without the optional part
            base_hint = hint.split(" | ")[0].strip()

            # Convert comma-separated strings to lists
            if base_hint in convertible_hints and isinstance(v[field], str):
                v[field] = [x.strip() for x in v[field].split(",") if x.strip()]

            # If the field is optional and the value is "None" convert it to None
            if "| None" in hint and v[field] == "None":
                v[field] = None
        return v


class UserConfig(BaseModel):
    PreferedUsername: str
    """Preferred username for the user account."""
    DNs: list[str] = []
    """Distinguished Names of the user's certificates (Mandatory for certificate-based authentication)."""
    Email: EmailStr | None = None
    """User e-mail address (Mandatory for user registration)."""
    Suspended: list[str] = []
    """List of VOs where the user is suspended."""
    Quota: int | None = None
    """Quota assigned to the user, expressed in MBs."""


class GroupConfig(BaseModel):
    AutoAddVOMS: bool = False
    """Controls automatic addition of VOMS extension when creating proxies."""
    AutoUploadPilotProxy: bool = False
    """Controls automatic Proxy upload for Pilot groups."""
    AutoUploadProxy: bool = False
    """Controls automatic Proxy upload for users in this group."""
    JobShare: int = 1000
    """Share of computing resources allocated to this group for fair share scheduling."""
    Properties: SerializableSet[SecurityProperty]
    """Group properties (set permissions of the group users).

    Examples: NormalUser, GenericPilot, ServiceAdministrator.
    """
    Quota: int | None = None
    """Group-specific quota override."""
    Users: SerializableSet[str]
    """DIRAC user logins that belong to this group."""
    AllowBackgroundTQs: bool = False
    """Allow background Task Queues for this group."""
    VOMSRole: str | None = None
    """Role of the users in the VO (e.g., '/lhcb' for LHCb VO)."""
    AutoSyncVOMS: bool = False
    """Automatically synchronize group membership with VOMS server."""


class IdpConfig(BaseModel):
    URL: str
    """The authorization server's issuer identifier.

    This is a URL that uses the 'https' scheme and has no query or fragment components.
    """
    ClientID: str
    """OAuth 2.0 client identifier received after client registration with the identity provider."""

    @property
    def server_metadata_url(self):
        return f"{self.URL}/.well-known/openid-configuration"


class SupportInfo(BaseModel):
    Email: str | None = None
    """Support contact email address."""
    Webpage: str | None = None
    """Support webpage URL."""
    Message: str = "Please contact system administrator"
    """Default support message displayed to users."""


class RegistryConfig(BaseModel):
    IdP: IdpConfig
    """Registered identity provider associated with this VO."""
    Support: SupportInfo = Field(default_factory=SupportInfo)
    """Support contact information for this VO."""
    DefaultGroup: str
    """Default user group to be used for new users in this VO."""
    DefaultStorageQuota: float = 0
    """Default storage quota in GB for users in this VO."""
    DefaultProxyLifeTime: int = 12 * 60 * 60
    """Default proxy time expressed in seconds (default: 43200 = 12 hours)."""
    VOMSName: str | None = None
    """Real VOMS VO name, if this VO is associated with VOMS VO."""

    Users: MutableMapping[str, UserConfig]
    """DIRAC users section, subsections represent the name of the user."""
    Groups: MutableMapping[str, GroupConfig]
    """DIRAC groups section, subsections represent the name of the group."""

    @cached_property
    def _preferred_username_to_sub(self) -> dict[str, str]:
        """Compute reverse lookup map from preferred username to user sub."""
        return {user.PreferedUsername: sub for sub, user in self.Users.items()}

    def sub_from_preferred_username(self, preferred_username: str) -> str:
        """Get the user sub from the preferred username.

        Args:
            preferred_username: The preferred username to look up.

        Returns:
            The user sub (subject identifier) for the given username.

        Raises:
            KeyError: If no user with the given preferred username is found.

        """
        try:
            return self._preferred_username_to_sub[preferred_username]
        except KeyError:
            raise KeyError(f"User {preferred_username} not found in registry") from None


class DIRACConfig(BaseModel):
    NoSetup: bool = False
    """Flag to skip setup procedures during DIRAC initialization. Takes a boolean value. By default false."""


class JobMonitoringConfig(BaseModel):
    GlobalJobsInfo: bool = True
    """Enable global job information monitoring across all VOs."""


class JobSchedulingConfig(BaseModel):
    EnableSharesCorrection: bool = False
    """Enable correction of job shares based on historical usage."""
    MaxRescheduling: int = 3
    """Maximum number of times a job can be rescheduled."""


class ServicesConfig(BaseModel):
    Catalogs: MutableMapping[str, Any] | None = None
    """Configuration for data catalog services."""
    JobMonitoring: JobMonitoringConfig = JobMonitoringConfig()
    """Job monitoring service configuration."""
    JobScheduling: JobSchedulingConfig = JobSchedulingConfig()
    """Job scheduling service configuration."""


class JobDescriptionConfig(BaseModel):
    DefaultCPUTime: int = 86400
    """Default CPU time limit for jobs in seconds (default: 24 hours)."""
    DefaultPriority: int = 1
    """Default job priority."""
    MinCPUTime: int = 100
    """Minimum allowed CPU time for jobs in seconds."""
    MinPriority: int = 0
    """Minimum allowed job priority."""
    MaxCPUTime: int = 500000
    """Maximum allowed CPU time for jobs in seconds."""
    MaxPriority: int = 10
    """Maximum allowed job priority."""
    MaxInputData: int = 100
    """Maximum number of input data files per job."""
    AllowedJobTypes: list[str] = ["User", "Test", "Hospital"]
    """List of allowed job types."""


class InputDataPolicyProtocolsConfig(BaseModel):
    Remote: list[str] = []
    """List of protocols that should be considered as remote access methods (e.g., 'https', 'gsiftp', 'srm')."""
    Local: list[str] = []
    """List of protocols that should be considered as local access methods (e.g., 'file', 'root')."""


class InputDataPolicyConfig(BaseModel):
    # TODO: Remove this once the model is extended to support everything
    model_config = ConfigDict(
        extra="ignore", frozen=True, use_attribute_docstrings=True
    )

    Default: str = "Default = DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol"
    """Default input data access policy. This is the fallback policy when no specific protocol is matched."""
    Download: str = "DIRAC.WorkloadManagementSystem.Client.DownloadInputData"
    """Policy for downloading input data files to the local worker node before job execution."""
    Protocol: str = "DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol"
    """Policy for accessing input data directly via supported protocols without downloading."""
    AllReplicas: bool = True
    """Whether to consider all available replicas when resolving input data locations."""
    Protocols: InputDataPolicyProtocolsConfig = InputDataPolicyProtocolsConfig()
    """Protocol-specific configuration defining which protocols are available for remote and local access."""
    InputDataModule: str = "DIRAC.Core.Utilities.InputDataResolution"
    """Module responsible for resolving input data locations and determining access methods."""


class OperationsConfig(BaseModel):
    EnableSecurityLogging: bool = False
    """Flag for globally disabling the use of the SecurityLogging service.

    This is False by default, as should be migrated to use centralized logging.
    """
    InputDataPolicy: InputDataPolicyConfig = InputDataPolicyConfig()
    """Specify how jobs access their data. See InputDataResolution documentation for details."""
    JobDescription: JobDescriptionConfig = JobDescriptionConfig()
    """Configuration for job description defaults and limits."""
    Services: ServicesConfig = ServicesConfig()
    """Configuration for various DIRAC services."""
    SoftwareDistModule: str = "LocalSoftwareDist"
    """Module used for software distribution."""

    Cloud: MutableMapping[str, Any] | None = None
    """Cloud computing configuration."""
    DataConsistency: MutableMapping[str, Any] | None = None
    """Data consistency checking configuration."""
    DataManagement: MutableMapping[str, Any] | None = None
    """Data management operations configuration."""
    EMail: MutableMapping[str, Any] | None = None
    """Email notification configuration."""
    GaudiExecution: MutableMapping[str, Any] | None = None
    """Gaudi framework execution configuration."""
    Hospital: MutableMapping[str, Any] | None = None
    """Job recovery and hospital configuration."""
    JobScheduling: MutableMapping[str, Any] | None = None
    """Advanced job scheduling configuration."""
    JobTypeMapping: MutableMapping[str, Any] | None = None
    """Mapping of job types to execution environments."""
    LogFiles: MutableMapping[str, Any] | None = None
    """Log file management configuration."""
    LogStorage: MutableMapping[str, Any] | None = None
    """Log storage backend configuration."""
    Logging: MutableMapping[str, Any] | None = None
    """General logging configuration."""
    Matching: MutableMapping[str, Any] | None = None
    """Job matching configuration."""
    MonitoringBackends: MutableMapping[str, Any] | None = None
    """Monitoring backend configuration."""
    NagiosConnector: MutableMapping[str, Any] | None = None
    """Nagios monitoring integration configuration."""
    Pilot: MutableMapping[str, Any] | None = None
    """Pilot job configuration."""
    Productions: MutableMapping[str, Any] | None = None
    """Production management configuration."""
    Shares: MutableMapping[str, Any] | None = None
    """Resource sharing configuration."""
    Shifter: MutableMapping[str, Any] | None = None
    """Shifter proxy configuration."""
    SiteSEMappingByProtocol: MutableMapping[str, Any] | None = None
    """Site storage element mapping by protocol."""
    TransformationPlugins: MutableMapping[str, Any] | None = None
    """Data transformation plugin configuration."""
    Transformations: MutableMapping[str, Any] | None = None
    """Data transformation system configuration."""
    ResourceStatus: MutableMapping[str, Any] | None = None
    """Resource status monitoring configuration."""


class ResourcesComputingConfig(BaseModel):
    # TODO: Remove this once the model is extended to support everything
    model_config = ConfigDict(extra="ignore", frozen=True)

    # TODO: Figure out how to remove this in LHCbDIRAC and then consider
    # constraining there to be at least one entry
    OSCompatibility: MutableMapping[str, set[str]] = {}
    """Compatibility matrix between DIRAC platforms and OS versions.

    Used by SiteDirector to match TaskQueues to Computing Element capabilities.
    """

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
    model_config = ConfigDict(
        extra="ignore", frozen=True, use_attribute_docstrings=True
    )

    Computing: ResourcesComputingConfig = ResourcesComputingConfig()
    """Computing resource configuration."""


class Config(BaseModel):
    DIRAC: DIRACConfig
    """The DIRAC section contains general parameters needed in most installation types."""
    Operations: MutableMapping[str, OperationsConfig]
    """Operations configuration per VO. The Defaults entry is automatically merged into each VO-specific config."""
    Registry: MutableMapping[str, RegistryConfig]
    """Registry sections to register VOs, groups, users and hosts. See UserManagement documentation for details."""
    Resources: ResourcesConfig = ResourcesConfig()
    """Resources configuration including computing elements, storage elements, and sites."""

    LocalSite: Any = None
    """Local site-specific configuration parameters."""
    LogLevel: Any = None
    """Global logging level configuration."""
    MCTestingDestination: Any = None
    """Monte Carlo testing destination configuration."""
    Systems: Any | None = None
    """Systems configuration."""
    WebApp: Any = None
    """Web application configuration parameters."""

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
