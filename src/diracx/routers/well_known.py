from __future__ import annotations

from typing import Annotated

# from cachetools import TTLCache
from fastapi import Depends, Request

from diracx.core.config import Config
from diracx.core.properties import SecurityProperty
from diracx.routers.auth import AuthSettings

from .configuration import get_config
from .fastapi_classes import DiracRouter, ServiceSettingsBase


class WellKnownSettings(ServiceSettingsBase, env_prefix="DIRACX_SERVICE_WELL_KNOWN_"):
    pass


router = DiracRouter(
    tags=[".well-known"], prefix="/.well-known", settings_class=WellKnownSettings
)


@router.get("/openid-configuration")
async def openid_configuration(
    request: Request,
    config: Annotated[Config, Depends(get_config)],
    settings: Annotated[AuthSettings, Depends(AuthSettings.create)],
):
    scopes_supported = []
    for vo in config.Registry:
        scopes_supported.append(f"vo:{vo}")
        scopes_supported += [f"group:{vo}" for vo in config.Registry[vo].Groups]
    scopes_supported += [f"property:{p.value}" for p in SecurityProperty]

    return {
        "issuer": settings.token_issuer,
        "token_endpoint": str(request.url_for("token")),
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
