import re
from typing import Annotated, Any
from uuid import UUID

from authlib.jose import JoseError, JsonWebToken
from fastapi import Depends, HTTPException, status
from fastapi.security import OpenIdConnect
from pydantic import BaseModel, Field
from pydantic_settings import SettingsConfigDict

from diracx.core.models import UserInfo
from diracx.core.properties import SecurityProperty
from diracx.core.settings import FernetKey, ServiceSettingsBase, TokenSigningKey
from diracx.routers.dependencies import Config, add_settings_annotation

# auto_error=False is used to avoid raising the wrong exception when the token is missing
# The error is handled in the verify_dirac_access_token function
# More info:
# - https://github.com/tiangolo/fastapi/issues/10177
# - https://datatracker.ietf.org/doc/html/rfc6750#section-3.1
oidc_scheme = OpenIdConnect(
    openIdConnectUrl="/.well-known/openid-configuration", auto_error=False
)


class AuthInfo(BaseModel):
    # raw token for propagation
    bearer_token: str

    # token ID in the DB for Component
    # unique jwt identifier for user
    token_id: UUID

    # list of DIRAC properties
    properties: list[SecurityProperty]

    policies: dict[str, Any] = {}


class AuthorizedUserInfo(AuthInfo, UserInfo):
    pass


@add_settings_annotation
class AuthSettings(ServiceSettingsBase):
    """Settings for the authentication service."""

    model_config = SettingsConfigDict(env_prefix="DIRACX_SERVICE_AUTH_")

    dirac_client_id: str = "myDIRACClientID"
    # TODO: This should be taken dynamically
    # ["http://pclhcb211:8000/docs/oauth2-redirect"]
    allowed_redirects: list[str] = []
    device_flow_expiration_seconds: int = 600
    authorization_flow_expiration_seconds: int = 300

    # State key is used to encrypt/decrypt the state dict passed to the IAM
    state_key: FernetKey

    # TODO: this should probably be something mandatory
    # to set by the user
    token_issuer: str = "http://lhcbdirac.cern.ch/"
    token_key: TokenSigningKey
    token_algorithm: str = "RS256"
    access_token_expire_minutes: int = 20
    refresh_token_expire_minutes: int = 60

    available_properties: set[SecurityProperty] = Field(
        default_factory=SecurityProperty.available_properties
    )


async def verify_dirac_access_token(
    authorization: Annotated[str, Depends(oidc_scheme)],
    settings: AuthSettings,
) -> AuthorizedUserInfo:
    """Verify dirac user token and return a UserInfo class
    Used for each API endpoint.
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header is missing",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if match := re.fullmatch(r"Bearer (.+)", authorization):
        raw_token = match.group(1)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid authorization header",
        )

    try:
        jwt = JsonWebToken(settings.token_algorithm)
        token = jwt.decode(
            raw_token,
            key=settings.token_key.jwk,
            claims_options={
                "iss": {"values": [settings.token_issuer]},
            },
        )
        token.validate()
    except JoseError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid JWT",
        ) from None

    return AuthorizedUserInfo(
        bearer_token=raw_token,
        token_id=token["jti"],
        properties=token["dirac_properties"],
        sub=token["sub"],
        preferred_username=token["preferred_username"],
        dirac_group=token["dirac_group"],
        vo=token["vo"],
        policies=token.get("dirac_policies", {}),
    )


def get_allowed_user_properties(config: Config, sub, vo: str) -> set[SecurityProperty]:
    """Retrieve all properties of groups a user is registered in."""
    allowed_user_properties = set()
    for group in config.Registry[vo].Groups:
        if sub in config.Registry[vo].Groups[group].Users:
            allowed_user_properties.update(config.Registry[vo].Groups[group].Properties)
    return allowed_user_properties
