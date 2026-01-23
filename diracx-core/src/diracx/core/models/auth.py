from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel
from typing_extensions import TypedDict


class UserInfo(BaseModel):
    sub: str  # dirac generated vo:sub
    preferred_username: str
    dirac_group: str
    vo: str


class TokenTypeHint(StrEnum):
    """Token type hints for RFC7009 revocation endpoint."""

    access_token = "access_token"  # noqa: S105
    refresh_token = "refresh_token"  # noqa: S105


class GrantType(StrEnum):
    """Grant types for OAuth2."""

    authorization_code = "authorization_code"
    device_code = "urn:ietf:params:oauth:grant-type:device_code"
    refresh_token = "refresh_token"  # noqa: S105   # False positive of Bandit about hard coded password


class InitiateDeviceFlowResponse(TypedDict):
    """Response for the device flow initiation."""

    user_code: str
    device_code: str
    verification_uri_complete: str
    verification_uri: str
    expires_in: int


class OpenIDConfiguration(TypedDict):
    issuer: str
    token_endpoint: str
    userinfo_endpoint: str
    authorization_endpoint: str
    device_authorization_endpoint: str
    revocation_endpoint: str
    jwks_uri: str
    grant_types_supported: list[str]
    scopes_supported: list[str]
    response_types_supported: list[str]
    token_endpoint_auth_signing_alg_values_supported: list[str]
    token_endpoint_auth_methods_supported: list[str]
    code_challenge_methods_supported: list[str]


class TokenPayload(TypedDict):
    jti: str
    exp: datetime
    dirac_policies: dict


class TokenResponse(BaseModel):
    # Based on RFC 6749
    access_token: str
    expires_in: int
    token_type: str = "Bearer"  # noqa: S105
    refresh_token: str | None = None


class AccessTokenPayload(TokenPayload):
    sub: str
    vo: str
    iss: str
    dirac_properties: list[str]
    preferred_username: str
    dirac_group: str


class RefreshTokenPayload(TokenPayload):
    legacy_exchange: bool


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
