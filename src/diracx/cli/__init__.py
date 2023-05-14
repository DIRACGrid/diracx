import asyncio
from datetime import datetime, timedelta

from diracx.client.aio import Dirac
from diracx.client.models import DeviceFlowErrorResponse
from diracx.routers.auth import DIRAC_CLIENT_ID

from . import jobs
from .utils import AsyncTyper

app = AsyncTyper()


@app.async_command()
async def login(vo: str):
    print(f"Logging in to {vo}")

    async with Dirac(endpoint="http://localhost:8000") as api:
        data = await api.auth.initiate_device_flow(
            vo=vo,
            client_id=DIRAC_CLIENT_ID,
            audience="Dirac server",
            scope="group:lhcb_user property:FileCatalogManagement property:NormalUser",
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
                    await asyncio.sleep(5)
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
