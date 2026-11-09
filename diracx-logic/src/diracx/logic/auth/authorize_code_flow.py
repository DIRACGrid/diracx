"""Authorization code flow."""

from __future__ import annotations

from typing import Literal

from diracx.core.config import Config
from diracx.core.models import GrantType
from diracx.core.properties import SecurityProperty
from diracx.core.settings import AuthSettings
from diracx.db.sql import AuthDB

from .utils import (
    decrypt_state,
    get_token_from_iam,
    initiate_authorization_flow_with_iam,
    parse_and_validate_scope,
)


async def initiate_authorization_flow(
    request_url: str,
    code_challenge: str,
    code_challenge_method: Literal["S256"],
    client_id: str,
    redirect_uri: str,
    scope: str,
    state: str,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
    available_properties: set[SecurityProperty],
) -> str:
    """Initiate an authorization flow and return the IAM redirect URL.

    Args:
        request_url: Base URL of the DiracX request handling the flow.
        code_challenge: PKCE challenge supplied by the client.
        code_challenge_method: PKCE method; only ``S256`` is supported.
        client_id: OAuth client identifier.
        redirect_uri: Client redirect URI registered in settings.
        scope: Requested VO, group, and property scopes.
        state: Client state value to return after authorization.
        auth_db: Authentication database.
        config: Installation configuration used to validate scope.
        settings: Authentication service settings.
        available_properties: Security properties supported by the installation.

    Returns:
        URL to which the client should be redirected at the IAM.

    Raises:
        ValueError: If the client ID or redirect URI is not allowed.
    """
    if settings.dirac_client_id != client_id:
        raise ValueError("Unrecognised client_id")
    if redirect_uri not in settings.allowed_redirects:
        raise ValueError("Unrecognised redirect_uri")

    # Parse and validate the scope
    parsed_scope = parse_and_validate_scope(scope, config, available_properties)

    # Store the authorization flow details
    uuid = await auth_db.insert_authorization_flow(
        client_id,
        scope,
        code_challenge,
        code_challenge_method,
        redirect_uri,
    )

    # Initiate the authorization flow with the IAM
    state_for_iam = {
        "external_state": state,
        "uuid": uuid,
        "grant_type": GrantType.authorization_code.value,
    }

    authorization_flow_url = await initiate_authorization_flow_with_iam(
        config,
        parsed_scope["vo"],
        f"{request_url}/complete",
        state_for_iam,
        settings.state_key.fernet,
    )

    return authorization_flow_url


async def complete_authorization_flow(
    code: str,
    state: str,
    request_url: str,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
) -> str:
    """Complete an authorization-code flow and build the client redirect URL.

    Args:
        code: Authorization code returned by the IAM.
        state: Encrypted flow state returned by the IAM.
        request_url: Base URL of the callback request.
        auth_db: Authentication database.
        config: Installation configuration used to validate the flow.
        settings: Authentication service settings.

    Returns:
        Client redirect URL containing the issued code and original state.

    Raises:
        AuthorizationError: If the state cannot be decrypted.
        AssertionError: If the state does not belong to an authorization-code flow.
        IAMServerError: If the IAM returns a server error.
        IAMClientError: If the IAM rejects the token request.
        JoseError: If the IAM ID token fails validation.
    """
    # Decrypt the state to access user details
    decrypted_state = decrypt_state(state, settings.state_key.fernet)
    assert decrypted_state["grant_type"] == GrantType.authorization_code

    # Get the ID token from the IAM
    id_token = await get_token_from_iam(
        config,
        decrypted_state["vo"],
        code,
        decrypted_state,
        request_url,
    )

    # Store the ID token and redirect the user to the client's redirect URI
    code, redirect_uri = await auth_db.authorization_flow_insert_id_token(
        decrypted_state["uuid"],
        id_token,
        settings.authorization_flow_expiration_seconds,
    )

    return f"{redirect_uri}?code={code}&state={decrypted_state['external_state']}"
