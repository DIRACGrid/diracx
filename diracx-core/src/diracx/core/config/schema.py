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

from diracx.core.properties import SecurityProperty
from diracx.core.utils import recursive_merge

# By default the serialization of set doesn't have a well defined ordering so
# we have to use a custom type to make sure the values are always sorted.
T = TypeVar("T")
SerializableSet = Annotated[
    set[T], PlainSerializer(sorted, return_type=list[T], when_used="json-unless-none")
]


class BaseModel(_BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        use_attribute_docstrings=True,
        populate_by_name=True,
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

        for field_name, field_info in cls.model_fields.items():
            hint = mro_annotations[field_name]
            field = field_info.alias or field_name

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
    prefered_username: str = Field(alias="PreferedUsername")
    """Preferred username for the user account."""
    dns: list[str] = Field([], alias="DNs")
    """Distinguished Names of the user's certificates (Mandatory for certificate-based authentication)."""
    email: EmailStr | None = Field(None, alias="Email")
    """User e-mail address (Mandatory for user registration)."""
    suspended: list[str] = Field([], alias="Suspended")
    """List of VOs where the user is suspended."""
    quota: int | None = Field(None, alias="Quota")
    """Quota assigned to the user, expressed in MBs."""


class GroupConfig(BaseModel):
    auto_add_voms: bool = Field(False, alias="AutoAddVOMS")
    """Controls automatic addition of VOMS extension when creating proxies."""
    auto_upload_pilot_proxy: bool = Field(False, alias="AutoUploadPilotProxy")
    """Controls automatic Proxy upload for Pilot groups."""
    auto_upload_proxy: bool = Field(False, alias="AutoUploadProxy")
    """Controls automatic Proxy upload for users in this group."""
    job_share: int = Field(1000, alias="JobShare")
    """Share of computing resources allocated to this group for fair share scheduling."""
    properties: SerializableSet[SecurityProperty] = Field(alias="Properties")
    """Group properties (set permissions of the group users).

    Examples: NormalUser, GenericPilot, ServiceAdministrator.
    """
    quota: int | None = Field(None, alias="Quota")
    """Group-specific quota override."""
    users: SerializableSet[str] = Field(alias="Users")
    """DIRAC user logins that belong to this group."""
    allow_background_tqs: bool = Field(False, alias="AllowBackgroundTQs")
    """Allow background Task Queues for this group."""
    voms_role: str | None = Field(None, alias="VOMSRole")
    """Role of the users in the VO (e.g., '/lhcb' for LHCb VO)."""
    auto_sync_voms: bool = Field(False, alias="AutoSyncVOMS")
    """Automatically synchronize group membership with VOMS server."""


class IdpConfig(BaseModel):
    url: str = Field(alias="URL")
    """The authorization server's issuer identifier.

    This is a URL that uses the 'https' scheme and has no query or fragment components.
    """
    client_id: str = Field(alias="ClientID")
    """OAuth 2.0 client identifier received after client registration with the identity provider."""

    @property
    def server_metadata_url(self):
        return f"{self.url}/.well-known/openid-configuration"


class SupportInfo(BaseModel):
    email: str | None = Field(None, alias="Email")
    """Support contact email address."""
    webpage: str | None = Field(None, alias="Webpage")
    """Support webpage URL."""
    message: str = Field("Please contact system administrator", alias="Message")
    """Default support message displayed to users."""


class RegistryConfig(BaseModel):
    idp: IdpConfig = Field(alias="IdP")
    """Registered identity provider associated with this VO."""
    support: SupportInfo = Field(default_factory=SupportInfo, alias="Support")
    """Support contact information for this VO."""
    default_group: str = Field(alias="DefaultGroup")
    """Default user group to be used for new users in this VO."""
    default_storage_quota: float = Field(0, alias="DefaultStorageQuota")
    """Default storage quota in GB for users in this VO."""
    default_proxy_life_time: int = Field(12 * 60 * 60, alias="DefaultProxyLifeTime")
    """Default proxy time expressed in seconds (default: 43200 = 12 hours)."""
    voms_name: str | None = Field(None, alias="VOMSName")
    """Real VOMS VO name, if this VO is associated with VOMS VO."""

    users: MutableMapping[str, UserConfig] = Field(alias="Users")
    """DIRAC users section, subsections represent the name of the user."""
    groups: MutableMapping[str, GroupConfig] = Field(alias="Groups")
    """DIRAC groups section, subsections represent the name of the group."""

    @cached_property
    def _preferred_username_to_sub(self) -> dict[str, str]:
        """Compute reverse lookup map from preferred username to user sub."""
        return {user.prefered_username: sub for sub, user in self.users.items()}

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
    no_setup: bool = Field(False, alias="NoSetup")
    """Flag to skip setup procedures during DIRAC initialization. Takes a boolean value. By default false."""


class JobMonitoringConfig(BaseModel):
    global_jobs_info: bool = Field(True, alias="GlobalJobsInfo")
    """Enable global job information monitoring across all VOs."""


class JobSchedulingConfig(BaseModel):
    enable_shares_correction: bool = Field(False, alias="EnableSharesCorrection")
    """Enable correction of job shares based on historical usage."""
    max_rescheduling: int = Field(3, alias="MaxRescheduling")
    """Maximum number of times a job can be rescheduled."""


class ServicesConfig(BaseModel):
    catalogs: MutableMapping[str, Any] | None = Field(None, alias="Catalogs")
    """Configuration for data catalog services."""
    job_monitoring: JobMonitoringConfig = Field(
        JobMonitoringConfig(), alias="JobMonitoring"
    )
    """Job monitoring service configuration."""
    job_scheduling: JobSchedulingConfig = Field(
        JobSchedulingConfig(), alias="JobScheduling"
    )
    """Job scheduling service configuration."""


class JobDescriptionConfig(BaseModel):
    default_cpu_time: int = Field(86400, alias="DefaultCPUTime")
    """Default CPU time limit for jobs in seconds (default: 24 hours)."""
    default_priority: int = Field(1, alias="DefaultPriority")
    """Default job priority."""
    min_cpu_time: int = Field(100, alias="MinCPUTime")
    """Minimum allowed CPU time for jobs in seconds."""
    min_priority: int = Field(0, alias="MinPriority")
    """Minimum allowed job priority."""
    max_cpu_time: int = Field(500000, alias="MaxCPUTime")
    """Maximum allowed CPU time for jobs in seconds."""
    max_priority: int = Field(10, alias="MaxPriority")
    """Maximum allowed job priority."""
    max_input_data: int = Field(100, alias="MaxInputData")
    """Maximum number of input data files per job."""
    allowed_job_types: list[str] = Field(
        ["User", "Test", "Hospital"], alias="AllowedJobTypes"
    )
    """List of allowed job types."""


class InputDataPolicyProtocolsConfig(BaseModel):
    remote: list[str] = Field([], alias="Remote")
    """List of protocols that should be considered as remote access methods (e.g., 'https', 'gsiftp', 'srm')."""
    local: list[str] = Field([], alias="Local")
    """List of protocols that should be considered as local access methods (e.g., 'file', 'root')."""


class InputDataPolicyConfig(BaseModel):
    # TODO: Remove this once the model is extended to support everything
    model_config = ConfigDict(
        extra="ignore",
        frozen=True,
        use_attribute_docstrings=True,
        populate_by_name=True,
    )

    default: str = Field(
        "Default = DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol",
        alias="Default",
    )
    """Default input data access policy. This is the fallback policy when no specific protocol is matched."""
    download: str = Field(
        "DIRAC.WorkloadManagementSystem.Client.DownloadInputData", alias="Download"
    )
    """Policy for downloading input data files to the local worker node before job execution."""
    protocol: str = Field(
        "DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol", alias="Protocol"
    )
    """Policy for accessing input data directly via supported protocols without downloading."""
    all_replicas: bool = Field(True, alias="AllReplicas")
    """Whether to consider all available replicas when resolving input data locations."""
    protocols: InputDataPolicyProtocolsConfig = Field(
        InputDataPolicyProtocolsConfig(), alias="Protocols"
    )
    """Protocol-specific configuration defining which protocols are available for remote and local access."""
    input_data_module: str = Field(
        "DIRAC.Core.Utilities.InputDataResolution", alias="InputDataModule"
    )
    """Module responsible for resolving input data locations and determining access methods."""


class OperationsConfig(BaseModel):
    enable_security_logging: bool = Field(False, alias="EnableSecurityLogging")
    """Flag for globally disabling the use of the SecurityLogging service.

    This is False by default, as should be migrated to use centralized logging.
    """
    input_data_policy: InputDataPolicyConfig = Field(
        InputDataPolicyConfig(), alias="InputDataPolicy"
    )
    """Specify how jobs access their data. See InputDataResolution documentation for details."""
    job_description: JobDescriptionConfig = Field(
        JobDescriptionConfig(), alias="JobDescription"
    )
    """Configuration for job description defaults and limits."""
    services: ServicesConfig = Field(ServicesConfig(), alias="Services")
    """Configuration for various DIRAC services."""
    software_dist_module: str = Field("LocalSoftwareDist", alias="SoftwareDistModule")
    """Module used for software distribution."""

    cloud: MutableMapping[str, Any] | None = Field(None, alias="Cloud")
    """Cloud computing configuration."""
    data_consistency: MutableMapping[str, Any] | None = Field(
        None, alias="DataConsistency"
    )
    """Data consistency checking configuration."""
    data_management: MutableMapping[str, Any] | None = Field(
        None, alias="DataManagement"
    )
    """Data management operations configuration."""
    email: MutableMapping[str, Any] | None = Field(None, alias="EMail")
    """Email notification configuration."""
    gaudi_execution: MutableMapping[str, Any] | None = Field(
        None, alias="GaudiExecution"
    )
    """Gaudi framework execution configuration."""
    hospital: MutableMapping[str, Any] | None = Field(None, alias="Hospital")
    """Job recovery and hospital configuration."""
    job_scheduling: MutableMapping[str, Any] | None = Field(None, alias="JobScheduling")
    """Advanced job scheduling configuration."""
    job_type_mapping: MutableMapping[str, Any] | None = Field(
        None, alias="JobTypeMapping"
    )
    """Mapping of job types to execution environments."""
    log_files: MutableMapping[str, Any] | None = Field(None, alias="LogFiles")
    """Log file management configuration."""
    log_storage: MutableMapping[str, Any] | None = Field(None, alias="LogStorage")
    """Log storage backend configuration."""
    logging: MutableMapping[str, Any] | None = Field(None, alias="Logging")
    """General logging configuration."""
    matching: MutableMapping[str, Any] | None = Field(None, alias="Matching")
    """Job matching configuration."""
    monitoring_backends: MutableMapping[str, Any] | None = Field(
        None, alias="MonitoringBackends"
    )
    """Monitoring backend configuration."""
    nagios_connector: MutableMapping[str, Any] | None = Field(
        None, alias="NagiosConnector"
    )
    """Nagios monitoring integration configuration."""
    pilot: MutableMapping[str, Any] | None = Field(None, alias="Pilot")
    """Pilot job configuration."""
    productions: MutableMapping[str, Any] | None = Field(None, alias="Productions")
    """Production management configuration."""
    shares: MutableMapping[str, Any] | None = Field(None, alias="Shares")
    """Resource sharing configuration."""
    shifter: MutableMapping[str, Any] | None = Field(None, alias="Shifter")
    """Shifter proxy configuration."""
    site_se_mapping_by_protocol: MutableMapping[str, Any] | None = Field(
        None, alias="SiteSEMappingByProtocol"
    )
    """Site storage element mapping by protocol."""
    transformation_plugins: MutableMapping[str, Any] | None = Field(
        None, alias="TransformationPlugins"
    )
    """Data transformation plugin configuration."""
    transformations: MutableMapping[str, Any] | None = Field(
        None, alias="Transformations"
    )
    """Data transformation system configuration."""
    resource_status: MutableMapping[str, Any] | None = Field(
        None, alias="ResourceStatus"
    )
    """Resource status monitoring configuration."""


class ResourcesComputingConfig(BaseModel):
    # TODO: Remove this once the model is extended to support everything
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    # TODO: Figure out how to remove this in LHCbDIRAC and then consider
    # constraining there to be at least one entry
    os_compatibility: MutableMapping[str, set[str]] = Field({}, alias="OSCompatibility")
    """Compatibility matrix between DIRAC platforms and OS versions.

    Used by SiteDirector to match TaskQueues to Computing Element capabilities.
    """

    @field_validator("os_compatibility", mode="before")
    @classmethod
    def legacy_adaptor_os_compatibility(cls, v: Any) -> Any:
        """Apply transformations to interpret the legacy DIRAC CFG format."""
        if not os.environ.get("DIRAC_COMPAT_ENABLE_CS_CONVERSION"):
            return v
        os_compatibility = v.get("OSCompatibility", {})
        for k, v in os_compatibility.items():
            os_compatibility[k] = set(v.replace(" ", "").split(","))
        return os_compatibility

    @field_validator("os_compatibility")
    @classmethod
    def ensure_self_compatibility(cls, v: dict[str, set[str]]) -> dict[str, set[str]]:
        """Ensure platforms are compatible with themselves."""
        for platform, compatible_platforms in v.items():
            compatible_platforms.add(platform)
        return v


class ResourcesConfig(BaseModel):
    # TODO: Remove this once the model is extended to support everything
    model_config = ConfigDict(
        extra="ignore",
        frozen=True,
        use_attribute_docstrings=True,
        populate_by_name=True,
    )

    computing: ResourcesComputingConfig = Field(
        ResourcesComputingConfig(), alias="Computing"
    )
    """Computing resource configuration."""


class Config(BaseModel):
    dirac: DIRACConfig = Field(alias="DIRAC")
    """The DIRAC section contains general parameters needed in most installation types."""
    operations: MutableMapping[str, OperationsConfig] = Field(alias="Operations")
    """Operations configuration per VO. The Defaults entry is automatically merged into each VO-specific config."""
    registry: MutableMapping[str, RegistryConfig] = Field(alias="Registry")
    """Registry sections to register VOs, groups, users and hosts. See UserManagement documentation for details."""
    resources: ResourcesConfig = Field(ResourcesConfig(), alias="Resources")
    """Resources configuration including computing elements, storage elements, and sites."""

    local_site: Any = Field(None, alias="LocalSite")
    """Local site-specific configuration parameters."""
    log_level: Any = Field(None, alias="LogLevel")
    """Global logging level configuration."""
    mc_testing_destination: Any = Field(None, alias="MCTestingDestination")
    """Monte Carlo testing destination configuration."""
    systems: Any | None = Field(None, alias="Systems")
    """Systems configuration."""
    web_app: Any = Field(None, alias="WebApp")
    """Web application configuration parameters."""

    @model_validator(mode="before")
    @classmethod
    def ensure_operations_defaults(cls, v: dict[str, Any]):
        """Merge the Defaults entry into the VO-specific config under Operations."""
        for field_name, field_info in cls.model_fields.items():
            if field_info.alias and field_name in v:
                v[field_info.alias] = v.pop(field_name)

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
