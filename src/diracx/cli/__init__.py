from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Optional

from typer import Option

from diracx.client.aio import Dirac
from diracx.client.models import DeviceFlowErrorResponse
from diracx.routers.auth import DIRAC_CLIENT_ID

from . import jobs
from .utils import AsyncTyper

app = AsyncTyper()


@app.async_command()
async def login(
    vo: Optional[str] = None,
    group: Optional[str] = None,
    property: Optional[list[str]] = Option(
        None, help="Override the default(s) with one or more properties"
    ),
):
    # TODO: The default should probably be server side
    # TODO: vo should probably be a scope
    scopes = []
    if vo is None:
        vo = "lhcb"
    if group is None:
        group = "lhcb_user"
    scopes.append(f"group:{group}")
    scopes += [
        f"property:{p}" for p in property or ["FileCatalogManagement", "NormalUser"]
    ]

    print(f"Logging in to {vo}")
    async with Dirac(endpoint="http://localhost:8000") as api:
        data = await api.auth.initiate_device_flow(
            vo=vo,
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

    print(f"Got token {response}")


app.add_typer(jobs.app, name="jobs")


if __name__ == "__main__":
    app()
