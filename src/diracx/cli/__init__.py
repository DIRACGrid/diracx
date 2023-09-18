from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta
from typing import Optional

from typer import Option

from diracx.client.aio import DiracClient
from diracx.client.models import DeviceFlowErrorResponse
from diracx.core.preferences import get_diracx_preferences
from diracx.core.utils import write_credentials

from . import internal, jobs
from .utils import AsyncTyper

app = AsyncTyper()


@app.async_command()
async def login(
    vo: str,
    group: Optional[str] = None,
    property: Optional[list[str]] = Option(
        None, help="Override the default(s) with one or more properties"
    ),
):
    scopes = [f"vo:{vo}"]
    if group:
        scopes.append(f"group:{group}")
    if property:
        scopes += [f"property:{p}" for p in property]

    print(f"Logging in with scopes: {scopes}")
    async with DiracClient() as api:
        data = await api.auth.initiate_device_flow(
            client_id=api.client_id,
            audience="Dirac server",
            scope=" ".join(scopes),
        )
        print("Now go to:", data.verification_uri_complete)
        expires = datetime.now() + timedelta(seconds=data.expires_in - 30)
        while expires > datetime.now():
            print(".", end="", flush=True)
            response = await api.auth.token(device_code=data.device_code, client_id=api.client_id)  # type: ignore
            if isinstance(response, DeviceFlowErrorResponse):
                if response.error == "authorization_pending":
                    # TODO: Setting more than 5 seconds results in an error
                    # Related to keep-alive disconnects from uvicon (--timeout-keep-alive)
                    await asyncio.sleep(2)
                    continue
                raise RuntimeError(f"Device flow failed with {response}")
            break
        else:
            raise RuntimeError("Device authorization flow expired")

        # Save credentials
        credentials_path = get_diracx_preferences().credentials_path
        credentials_path.parent.mkdir(parents=True, exist_ok=True)
        write_credentials(response)
        print(f"Saved credentials to {credentials_path}")
    print("\nLogin successful!")


@app.async_command()
async def logout():
    async with DiracClient() as api:
        credentials_path = get_diracx_preferences().credentials_path
        if credentials_path:
            credentials = json.loads(credentials_path.read_text())

            # Revoke refresh token
            try:
                await api.auth.revoke_refresh_token(credentials["refresh_token"])
            except Exception:
                pass

            # Remove credentials
            credentials_path.unlink(missing_ok=True)
            print(f"Removed credentials from {credentials_path}")
    print("\nLogout successful!")


@app.callback()
def callback(output_format: Optional[str] = None):
    if "DIRACX_OUTPUT_FORMAT" not in os.environ:
        output_format = output_format or "RICH"
    if output_format is not None:
        os.environ["DIRACX_OUTPUT_FORMAT"] = output_format


app.add_typer(jobs.app, name="jobs")
app.add_typer(internal.app, name="internal", hidden=True)


if __name__ == "__main__":
    app()
