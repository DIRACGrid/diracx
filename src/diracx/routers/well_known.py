from __future__ import annotations

from fastapi import Request

from diracx.routers.auth import AuthSettings

from .dependencies import Config
from .fastapi_classes import DiracxRouter

router = DiracxRouter(require_auth=False)


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
