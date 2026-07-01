"""Business logic for the OAuth2 authorization-code flow.

This module contains the logic-layer operations used by the routers to
initiate and complete the authorization code grant with the IAM. The helpers
validate client and redirect URI values, persist transient authorization
flow state, and exchange authorization codes for ID tokens.
"""

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
    """Initiate the authorization code flow.

    This validates the client and redirect URI, resolves the requested
    scope, persists the transient authorization flow data, and constructs the
    IAM authorization URL used for redirecting the user agent.

    Args:
        request_url (str): Base URL of the current request.
        code_challenge (str): PKCE code challenge.
        code_challenge_method (Literal["S256"]): PKCE method.
        client_id (str): Client identifier from the request.
        redirect_uri (str): Client redirect URI.
        scope (str): OAuth2 scope string.
        state (str): Opaque state value to preserve user session.
        auth_db (AuthDB): DB helper storing transient authorization flow data.
        config (Config): Application configuration registry.
        settings (AuthSettings): Authentication settings.
        available_properties (set[SecurityProperty]): Properties available for scope resolution.

    Returns:
        str: Redirect URL for the IAM authorization endpoint.
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
    """Complete the authorization code flow.

    This decrypts the state returned from the IAM, exchanges the authorization
    code for an ID token, persists the token in the database, and returns a
    client redirect URL containing the new authorization code and original
    state.

    Args:
        code (str): Authorization code returned by the IAM.
        state (str): Encrypted state from the IAM redirect.
        request_url (str): Base URL of the current request.
        auth_db (AuthDB): DB helper storing authorization data.
        config (Config): Application configuration registry.
        settings (AuthSettings): Authentication settings.

    Returns:
        str: Redirect URI to send the user back to the client.
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
