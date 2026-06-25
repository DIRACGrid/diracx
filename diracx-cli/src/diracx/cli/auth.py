from __future__ import annotations

__all__ = ["app"]

import asyncio
import json
import os
from asyncio import sleep
from datetime import datetime, timedelta, timezone
from typing import Annotated, Optional

import typer

from diracx.client.aio import AsyncDiracClient

# See https://github.com/DIRACGrid/diracx/issues/578
from diracx.client.models import DeviceFlowErrorResponse  # type: ignore [attr-defined]
from diracx.core.preferences import get_diracx_preferences
from diracx.core.utils import read_credentials, write_credentials

from .utils import AsyncTyper

app = AsyncTyper()


async def installation_metadata():
    """Fetch installation metadata from the server's well-known endpoint.

    This helper uses an `AsyncDiracClient` to request the DIRAC installation
    metadata. It is intended for use from synchronous callback contexts
    (e.g. `vo_callback`) where `asyncio.run` may be used to synchronously
    obtain the result.

    Returns:
        Metadata: Installation metadata retrieved from the server.
    """
    async with AsyncDiracClient() as api:
        return await api.well_known.get_installation_metadata()


def vo_callback(vo: str | None) -> str:
    """Validate the provided VO against installation metadata.

    This callback is used by `typer` to validate the `vo` argument passed by
    the user. It synchronously fetches installation metadata and verifies
    that the supplied VO exists. On failure it raises a `typer.BadParameter`.

    Args:
        vo (Optional[str]): The VO name provided by the user.

    Returns:
        str: The validated VO string.

    Raises:
        typer.BadParameter: If no VO was provided or the VO is not known.
    """
    metadata = asyncio.run(installation_metadata())
    vos = list(metadata.virtual_organizations)
    if not vo:
        raise typer.BadParameter(
            f"VO must be specified, available options are: {' '.join(vos)}"
        )
    if vo not in vos:
        raise typer.BadParameter(
            f"Unknown VO {vo}, available options are: {' '.join(vos)}"
        )
    return vo


@app.async_command()
async def login(
    vo: Annotated[
        Optional[str],
        typer.Argument(callback=vo_callback, help="Virtual Organization name"),
    ] = None,
    group: Annotated[
        Optional[str],
        typer.Option(
            help="Group name within the VO. If not provided, the default group for the VO will be used."
        ),
    ] = None,
    property: Annotated[
        Optional[list[str]],
        typer.Option(
            help=(
                "List of properties to add to the default properties of the group. "
                "If not provided, default properties of the group will be used."
            )
        ),
    ] = None,
):
    """Login to DIRAC using the OAuth2 device flow.

    The command initiates a device authorization flow, instructs the user to
    open a verification URL and enter a user code, polls the token endpoint
    until the user completes authorization, and saves received credentials to
    the local preferences file.

    Scope resolution behavior:
    - If only VO is provided: uses the VO's default group and its properties.
    - If VO and group are provided: uses the specified group and its properties.
    - If VO and properties are provided: uses the default group and merges its
        properties with the provided properties.
    - If VO, group, and properties are provided: uses the specified group and
        merges its properties with the provided properties.

    Args:
            vo (Optional[str]): Virtual Organization name (validated by `vo_callback`).
            group (Optional[str]): Group name within the VO.
            property (Optional[list[str]]): Additional properties to request.

    Raises:
            RuntimeError: If the device flow fails or expires before completion.
    """
    scopes = [f"vo:{vo}"]
    if group:
        scopes.append(f"group:{group}")
    if property:
        scopes += [f"property:{p}" for p in property]

    print(f"Logging in with scopes: {scopes}")
    async with AsyncDiracClient() as api:
        data = await api.auth.initiate_device_flow(
            client_id=api.client_id,
            scope=" ".join(scopes),
        )
        print("Now go to:", data.verification_uri_complete)
        expires = datetime.now(tz=timezone.utc) + timedelta(
            seconds=data.expires_in - 30
        )
        while expires > datetime.now(tz=timezone.utc):
            print(".", end="", flush=True)
            response = await api.auth.get_oidc_token(
                device_code=data.device_code, client_id=api.client_id
            )  # type: ignore
            if isinstance(response, DeviceFlowErrorResponse):
                if response.error == "authorization_pending":
                    # TODO: Setting more than 5 seconds results in an error
                    # Related to keep-alive disconnects from uvicon (--timeout-keep-alive)
                    await sleep(2)
                    continue
                raise RuntimeError(f"Device flow failed with {response}")
            break
        else:
            raise RuntimeError("Device authorization flow expired")

        # Save credentials
        write_credentials(response)
        credentials_path = get_diracx_preferences().credentials_path
        print(f"Saved credentials to {credentials_path}")
    print("\nLogin successful!")


@app.async_command()
async def whoami():
    """Print authenticated user's identity information.

    Queries the `userinfo` endpoint and prints a JSON representation of the
    returned identity attributes. Intended for interactive inspection.
    """
    async with AsyncDiracClient() as api:
        user_info = await api.auth.userinfo()
        # TODO: Add a RICH output format
        print(json.dumps(user_info.as_dict(), indent=2))


@app.async_command()
async def logout():
    """Logout by revoking refresh token and removing stored credentials.

    If stored credentials are present, the command attempts to revoke the
    refresh token at the server and then deletes the local credentials file.
    Any errors during revocation are printed but do not prevent credential
    file removal.
    """
    async with AsyncDiracClient() as api:
        credentials_path = get_diracx_preferences().credentials_path
        if credentials_path.exists():
            credentials = read_credentials(credentials_path)

            # Revoke refresh token
            try:
                await api.auth.revoke_refresh_token_by_refresh_token(
                    client_id=api.client_id, token=credentials.refresh_token
                )
            except Exception as e:
                print(f"Error revoking the refresh token {e!r}")
                pass

            # Remove credentials
            credentials_path.unlink(missing_ok=True)
            print(f"Removed credentials from {credentials_path}")
        else:
            print("You are not connected to DiracX, or your credentials are missing.")
            return
    print("\nLogout successful!")


@app.callback()
def callback(output_format: Optional[str] = None):
    """Typer callback to set the output format for CLI commands.

    When provided, this callback sets the `DIRACX_OUTPUT_FORMAT` environment
    variable so subsequent commands can adapt their output formatting.

    Args:
        output_format (Optional[str]): Output format identifier (e.g. "json").
    """
    if output_format is not None:
        os.environ["DIRACX_OUTPUT_FORMAT"] = output_format
