from __future__ import annotations

from diracx.core.config.schema import Config
from diracx.core.models import GroupInfo, Metadata, OpenIDConfiguration
from diracx.core.settings import AuthSettings


async def get_openid_configuration(
    token_endpoint: str,
    userinfo_endpoint: str,
    authorization_endpoint: str,
    device_authorization_endpoint: str,
    config: Config,
    settings: AuthSettings,
) -> OpenIDConfiguration:
    """OpenID Connect discovery endpoint."""
    scopes_supported = []
    for vo in config.Registry:
        scopes_supported.append(f"vo:{vo}")
        scopes_supported += [f"group:{vo}" for vo in config.Registry[vo].Groups]
    scopes_supported += [f"property:{p}" for p in settings.available_properties]

    return {
        "issuer": settings.token_issuer,
        "token_endpoint": token_endpoint,
        "userinfo_endpoint": userinfo_endpoint,
        "authorization_endpoint": authorization_endpoint,
        "device_authorization_endpoint": device_authorization_endpoint,
        "grant_types_supported": [
            "authorization_code",
            "urn:ietf:params:oauth:grant-type:device_code",
        ],
        "scopes_supported": scopes_supported,
        "response_types_supported": ["code"],
        "token_endpoint_auth_signing_alg_values_supported": [settings.token_algorithm],
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
    }


async def get_installation_metadata(
    config: Config,
) -> Metadata:
    """Get metadata about the dirac installation."""
    metadata: Metadata = {
        "virtual_organizations": {},
    }
    for vo, vo_info in config.Registry.items():
        groups: dict[str, GroupInfo] = {
            group: {"properties": sorted(group_info.Properties)}
            for group, group_info in vo_info.Groups.items()
        }
        metadata["virtual_organizations"][vo] = {
            "groups": groups,
            "support": {
                "message": vo_info.Support.Message,
                "webpage": vo_info.Support.Webpage,
                "email": vo_info.Support.Email,
            },
            "default_group": vo_info.DefaultGroup,
        }

    return metadata
