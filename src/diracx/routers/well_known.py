from __future__ import annotations

from typing import TypedDict

from fastapi import Request

from diracx.routers.auth import AuthSettings

from .dependencies import Config
from .fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False, path_root="")


@router.get("/openid-configuration")
async def openid_configuration(
    request: Request,
    config: Config,
    settings: AuthSettings,
):
    scopes_supported = []
    for vo in config.Registry:
        scopes_supported.append(f"vo:{vo}")
        scopes_supported += [f"group:{vo}" for vo in config.Registry[vo].Groups]
    scopes_supported += [f"property:{p}" for p in settings.available_properties]

    return {
        "issuer": settings.token_issuer,
        "token_endpoint": str(request.url_for("token")),
        "userinfo_endpoint:": str(request.url_for("userinfo")),
        "authorization_endpoint": str(request.url_for("authorization_flow")),
        "device_authorization_endpoint": str(request.url_for("initiate_device_flow")),
        # "introspection_endpoint":"",
        # "userinfo_endpoint":"",
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


@router.get("/dirac-metadata")
async def installation_metadata(config: Config) -> Metadata:
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
