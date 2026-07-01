"""Business logic for OAuth2 device flow operations.

This module handles the DiracX-specific device authorization flow, including
initial device flow creation, verification URI redirection, and completion of
the interactive user authorization step.
"""

from __future__ import annotations

from diracx.core.config import Config
from diracx.core.models import GrantType, InitiateDeviceFlowResponse
from diracx.core.properties import SecurityProperty
from diracx.core.settings import AuthSettings
from diracx.db.sql import AuthDB

from .utils import (
    decrypt_state,
    get_token_from_iam,
    initiate_authorization_flow_with_iam,
    parse_and_validate_scope,
)


async def initiate_device_flow(
    client_id: str,
    scope: str,
    verification_uri: str,
    auth_db: AuthDB,
    config: Config,
    available_properties: set[SecurityProperty],
    settings: AuthSettings,
) -> InitiateDeviceFlowResponse:
    """Initiate the device authorization flow.

    This validates the client identifier and requested scope, creates a new
    device flow record in the database, and returns the response required by
    the device to complete user authorization.

    Args:
        client_id (str): OAuth2 client identifier.
        scope (str): Requested OAuth2 scope string.
        verification_uri (str): URI the user should visit to enter the code.
        auth_db (AuthDB): Database helper for device flow state.
        config (Config): Application configuration registry.
        available_properties (set[SecurityProperty]): Security properties available for scope resolution.
        settings (AuthSettings): Authentication-related settings.

    Returns:
        InitiateDeviceFlowResponse: Verification and expiry data for the device.
    """
    if settings.dirac_client_id != client_id:
        raise ValueError("Unrecognised client ID")

    parse_and_validate_scope(scope, config, available_properties)

    user_code, device_code = await auth_db.insert_device_flow(client_id, scope)

    return {
        "user_code": user_code,
        "device_code": device_code,
        "verification_uri_complete": f"{verification_uri}?user_code={user_code}",
        "verification_uri": verification_uri,
        "expires_in": settings.device_flow_expiration_seconds,
    }


async def do_device_flow(
    request_url: str,
    auth_db: AuthDB,
    user_code: str,
    config: Config,
    available_properties: set[SecurityProperty],
    settings: AuthSettings,
) -> str:
    """Verify the device user code and initiate the interactive authorization flow.

    This validates the submitted user code, resolves the requested scope, and
    constructs the IAM authorization URL for the browser-based portion of the
    device flow.

    Args:
        request_url (str): Base URL of the current request.
        auth_db (AuthDB): Database helper for device flow state.
        user_code (str): User code issued to the device.
        config (Config): Application configuration registry.
        available_properties (set[SecurityProperty]): Available security properties for scope resolution.
        settings (AuthSettings): Authentication-related settings.

    Returns:
        str: Redirect URL for the IAM authorization endpoint.
    """
    # Here we make sure the user_code actually exists
    scope = await auth_db.device_flow_validate_user_code(
        user_code, settings.device_flow_expiration_seconds
    )
    parsed_scope = parse_and_validate_scope(scope, config, available_properties)

    redirect_uri = f"{request_url}/complete"

    state_for_iam = {
        "grant_type": GrantType.device_code.value,
        "user_code": user_code,
    }

    authorization_flow_url = await initiate_authorization_flow_with_iam(
        config,
        parsed_scope["vo"],
        redirect_uri,
        state_for_iam,
        settings.state_key.fernet,
    )
    return authorization_flow_url


async def finish_device_flow(
    request_url: str,
    code: str,
    state: str,
    auth_db: AuthDB,
    config: Config,
    settings: AuthSettings,
):
    """Complete the device flow by exchanging the IAM authorization code.

    This decrypts the returned state, exchanges the authorization code for an
    ID token, and stores the token in the database under the device flow record.

    Args:
        request_url (str): Base URL of the current request.
        code (str): Authorization code returned by the IAM.
        state (str): Encrypted state from the IAM redirect.
        auth_db (AuthDB): Database helper for device flow state.
        config (Config): Application configuration registry.
        settings (AuthSettings): Authentication-related settings.
    """
    decrypted_state = decrypt_state(state, settings.state_key.fernet)
    assert decrypted_state["grant_type"] == GrantType.device_code

    id_token = await get_token_from_iam(
        config,
        decrypted_state["vo"],
        code,
        decrypted_state,
        request_url,
    )
    await auth_db.device_flow_insert_id_token(
        decrypted_state["user_code"], id_token, settings.device_flow_expiration_seconds
    )
