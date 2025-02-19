from __future__ import annotations

import base64
import hashlib
import json
import secrets
from uuid import UUID

import httpx
from authlib.integrations.starlette_client import OAuthError
from authlib.jose import JsonWebKey, JsonWebToken
from authlib.oidc.core import IDToken
from cachetools import TTLCache
from cryptography.fernet import Fernet
from typing_extensions import TypedDict

from diracx.core.config.schema import Config
from diracx.core.exceptions import AuthorizationError, IAMClientError, IAMServerError
from diracx.core.models import GrantType
from diracx.core.properties import SecurityProperty
from diracx.core.settings import AuthSettings


class ScopeInfoDict(TypedDict):
    group: str
    properties: set[str]
    vo: str


_server_metadata_cache: TTLCache = TTLCache(maxsize=1024, ttl=3600)


async def get_server_metadata(url: str):
    """Get the server metadata from the IAM."""
    server_metadata = _server_metadata_cache.get(url)
    if server_metadata is None:
        async with httpx.AsyncClient() as c:
            res = await c.get(url)
            if res.status_code != 200:
                # TODO: Better error handling
                raise NotImplementedError(res)
            server_metadata = res.json()
            _server_metadata_cache[url] = server_metadata
    return server_metadata


def encrypt_state(state_dict: dict[str, str], cipher_suite: Fernet) -> str:
    """Encrypt the state dict and return it as a string."""
    return cipher_suite.encrypt(
        base64.urlsafe_b64encode(json.dumps(state_dict).encode())
    ).decode()


def decrypt_state(state: str, cipher_suite: Fernet) -> dict[str, str]:
    """Decrypt the state string and return it as a dict."""
    try:
        return json.loads(
            base64.urlsafe_b64decode(cipher_suite.decrypt(state.encode())).decode()
        )
    except Exception as e:
        raise AuthorizationError("Invalid state") from e


async def fetch_jwk_set(url: str):
    """Fetch the JWK set from the IAM."""
    server_metadata = await get_server_metadata(url)

    jwks_uri = server_metadata.get("jwks_uri")
    if not jwks_uri:
        raise RuntimeError('Missing "jwks_uri" in metadata')

    async with httpx.AsyncClient() as c:
        res = await c.get(jwks_uri)
        if res.status_code != 200:
            # TODO: Better error handling
            raise NotImplementedError(res)
        jwk_set = res.json()

    return JsonWebKey.import_key_set(jwk_set)


async def parse_id_token(config, vo, raw_id_token: str):
    """Parse and validate the ID token from IAM."""
    server_metadata = await get_server_metadata(
        config.Registry[vo].IdP.server_metadata_url
    )
    alg_values = server_metadata.get("id_token_signing_alg_values_supported", ["RS256"])
    jwk_set = await fetch_jwk_set(config.Registry[vo].IdP.server_metadata_url)

    token = JsonWebToken(alg_values).decode(
        raw_id_token,
        key=jwk_set,
        claims_cls=IDToken,
        claims_options={
            "iss": {"values": [server_metadata["issuer"]]},
            # The audience is a required parameter and is the client ID of the application
            # https://openid.net/specs/openid-connect-core-1_0.html#IDToken
            "aud": {"values": [config.Registry[vo].IdP.ClientID]},
        },
    )
    token.validate()
    return token


async def initiate_authorization_flow_with_iam(
    config, vo: str, redirect_uri: str, state: dict[str, str], cipher_suite: Fernet
):
    """Initiate the authorization flow with the IAM. Return the URL to redirect the user to.

    The state dict is encrypted and passed to the IAM.
    It is then decrypted when the user is redirected back to the redirect_uri.
    """
    # code_verifier: https://www.rfc-editor.org/rfc/rfc7636#section-4.1
    code_verifier = secrets.token_hex()

    # code_challenge: https://www.rfc-editor.org/rfc/rfc7636#section-4.2
    code_challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
        .decode()
        .replace("=", "")
    )

    server_metadata = await get_server_metadata(
        config.Registry[vo].IdP.server_metadata_url
    )

    # Take these two from CS/.well-known
    authorization_endpoint = server_metadata["authorization_endpoint"]

    # Encrypt the state and pass it to the IAM
    # Needed to retrieve the original flow details when the user is redirected back to the redirect_uri
    encrypted_state = encrypt_state(
        state | {"vo": vo, "code_verifier": code_verifier}, cipher_suite
    )

    url_params = [
        "response_type=code",
        f"code_challenge={code_challenge}",
        "code_challenge_method=S256",
        f"client_id={config.Registry[vo].IdP.ClientID}",
        f"redirect_uri={redirect_uri}",
        "scope=openid%20profile",
        f"state={encrypted_state}",
    ]
    authorization_flow_url = f"{authorization_endpoint}?{'&'.join(url_params)}"
    return authorization_flow_url


async def get_token_from_iam(
    config, vo: str, code: str, state: dict[str, str], redirect_uri: str
) -> dict[str, str]:
    """Get the token from the IAM using the code and state. Return the ID token."""
    server_metadata = await get_server_metadata(
        config.Registry[vo].IdP.server_metadata_url
    )

    # Take these two from CS/.well-known
    token_endpoint = server_metadata["token_endpoint"]

    data = {
        "grant_type": GrantType.authorization_code.value,
        "client_id": config.Registry[vo].IdP.ClientID,
        "code": code,
        "code_verifier": state["code_verifier"],
        "redirect_uri": redirect_uri,
    }

    async with httpx.AsyncClient() as c:
        res = await c.post(
            token_endpoint,
            data=data,
        )
        if res.status_code >= 500:
            raise IAMServerError("Failed to contact IAM server")
        elif res.status_code >= 400:
            raise IAMClientError("Failed to contact IAM server")

    raw_id_token = res.json()["id_token"]
    # Extract the payload and verify it
    try:
        id_token = await parse_id_token(
            config=config,
            vo=vo,
            raw_id_token=raw_id_token,
        )
    except OAuthError:
        raise

    return id_token


async def verify_dirac_refresh_token(
    refresh_token: str,
    settings: AuthSettings,
) -> tuple[UUID, float, bool]:
    """Verify dirac user token and return a UserInfo class
    Used for each API endpoint.
    """
    jwt = JsonWebToken(settings.token_algorithm)
    token = jwt.decode(
        refresh_token,
        key=settings.token_key.jwk,
    )
    token.validate()

    return (
        UUID(token["jti"], version=4),
        float(token["exp"]),
        token["legacy_exchange"],
    )


def get_allowed_user_properties(config: Config, sub, vo: str) -> set[SecurityProperty]:
    """Retrieve all properties of groups a user is registered in."""
    allowed_user_properties = set()
    for group in config.Registry[vo].Groups:
        if sub in config.Registry[vo].Groups[group].Users:
            allowed_user_properties.update(config.Registry[vo].Groups[group].Properties)
    return allowed_user_properties


def parse_and_validate_scope(
    scope: str, config: Config, available_properties: set[SecurityProperty]
) -> ScopeInfoDict:
    """Check:
        * At most one VO
        * At most one group
        * group belongs to VO
        * properties are known
    return dict with group and properties.

    :raises:
        * ValueError in case the scope isn't valide
    """
    scopes = set(scope.split(" "))

    groups = []
    properties = []
    vos = []
    unrecognised = []
    for scope in scopes:
        if scope.startswith("group:"):
            groups.append(scope.split(":", 1)[1])
        elif scope.startswith("property:"):
            properties.append(scope.split(":", 1)[1])
        elif scope.startswith("vo:"):
            vos.append(scope.split(":", 1)[1])
        else:
            unrecognised.append(scope)
    if unrecognised:
        raise ValueError(f"Unrecognised scopes: {unrecognised}")

    if not vos:
        available_vo_scopes = [repr(f"vo:{vo}") for vo in config.Registry]
        raise ValueError(
            "No vo scope requested, available values: "
            f"{' '.join(available_vo_scopes)}"
        )
    elif len(vos) > 1:
        raise ValueError(f"Only one vo is allowed but got {vos}")
    else:
        vo = vos[0]
        if vo not in config.Registry:
            raise ValueError(f"VO {vo} is not known to this installation")

    if not groups:
        # TODO: Handle multiple groups correctly
        group = config.Registry[vo].DefaultGroup
    elif len(groups) > 1:
        raise ValueError(f"Only one DIRAC group allowed but got {groups}")
    else:
        group = groups[0]
        if group not in config.Registry[vo].Groups:
            raise ValueError(f"{group} not in {vo} groups")

    allowed_properties = config.Registry[vo].Groups[group].Properties
    properties.extend([str(p) for p in allowed_properties])

    if not set(properties).issubset(available_properties):
        raise ValueError(
            f"{set(properties)-set(available_properties)} are not valid properties"
        )

    return {
        "group": group,
        "properties": set(sorted(properties)),
        "vo": vo,
    }
