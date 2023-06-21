from __future__ import annotations

from typing import Annotated

# from cachetools import TTLCache
from fastapi import APIRouter, Depends, Request

from diracx.core.config import Config, get_config
from diracx.core.properties import SecurityProperty
from diracx.core.secrets import DiracxSecrets, get_secrets

router = APIRouter(tags=["well-known"])


@router.get("/.well-known/openid-configuration")
async def openid_configuration(
    request: Request,
    config: Annotated[Config, Depends(get_config)],
    secrets: Annotated[DiracxSecrets, Depends(get_secrets)],
):
    scopes_supported = []
    for vo in config.Registry:
        scopes_supported.append(f"vo:{vo}")
        scopes_supported += [f"group:{vo}" for vo in config.Registry[vo].Groups]
    scopes_supported += [f"property:{p.value}" for p in SecurityProperty]

    assert secrets.auth

    return {
        "issuer": secrets.auth.token_issuer,
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
        "token_endpoint_auth_signing_alg_values_supported": [
            secrets.auth.token_algorithm
        ],
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
    }
