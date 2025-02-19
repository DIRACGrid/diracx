"""Device flow."""

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
    """Initiate the device flow against DIRAC authorization Server."""
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
    """This is called as the verification URI for the device flow."""
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
    """This the url callbacked by IAM/Checkin after the authorization
    flow was granted.
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
