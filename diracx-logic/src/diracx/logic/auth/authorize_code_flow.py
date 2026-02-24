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
    """Initiate the authorization flow."""
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
    """Complete the authorization flow."""
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
