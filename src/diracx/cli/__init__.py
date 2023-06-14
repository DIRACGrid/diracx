from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from typer import Option

from diracx.client.aio import Dirac
from diracx.client.models import DeviceFlowErrorResponse
from diracx.routers.auth import DIRAC_CLIENT_ID

from . import internal, jobs
from .utils import CREDENTIALS_PATH, AsyncTyper

app = AsyncTyper()

EXPIRES_GRACE_SECONDS = 15


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
    # TODO set endpoint URL from preferences
    async with Dirac(endpoint="http://localhost:8000") as api:
        data = await api.auth.initiate_device_flow(
            client_id=DIRAC_CLIENT_ID,
            audience="Dirac server",
            scope=" ".join(scopes),
        )
        print("Now go to:", data.verification_uri_complete)
        expires = datetime.now() + timedelta(seconds=data.expires_in - 30)
        while expires > datetime.now():
            print(".", end="", flush=True)
            response = await api.auth.token(  # type: ignore
                vo, device_code=data.device_code, client_id=DIRAC_CLIENT_ID
            )
            if isinstance(response, DeviceFlowErrorResponse):
                if response.error == "authorization_pending":
                    # TODO: Setting more than 5 seconds results in an error
                    # Related to keep-alive disconnects from uvicon (--timeout-keep-alive)
                    await asyncio.sleep(2)
                    continue
                raise RuntimeError(f"Device flow failed with {response}")
            print("\nLogin successful!")
            break
        else:
            raise RuntimeError("Device authorization flow expired")

    CREDENTIALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    expires = datetime.now(tz=timezone.utc) + timedelta(
        seconds=response.expires_in - EXPIRES_GRACE_SECONDS
    )
    credential_data = {
        "access_token": response.access_token,
        # TODO: "refresh_token":
        # TODO: "refresh_token_expires":
        "expires": expires.isoformat(),
    }
    CREDENTIALS_PATH.write_text(json.dumps(credential_data))
    print(f"Saved credentials to {CREDENTIALS_PATH}")


@app.async_command()
async def logout():
    CREDENTIALS_PATH.unlink(missing_ok=True)
    # TODO: This should also revoke the refresh token
    print(f"Removed credentials from {CREDENTIALS_PATH}")


@app.callback()
def callback(output_format: Optional[str] = None):
    if "DIRACX_OUTPUT_FORMAT" not in os.environ:
        output_format = output_format or "rich"
    if output_format is not None:
        os.environ["DIRACX_OUTPUT_FORMAT"] = output_format


app.add_typer(jobs.app, name="jobs")
app.add_typer(internal.app, name="internal", hidden=True)


if __name__ == "__main__":
    app()
