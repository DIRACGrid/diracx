from __future__ import annotations

__all__ = ("app",)

import asyncio
import json
from typing import Annotated, Optional

import typer

from diracx.client.aio import AsyncDiracClient

from .utils import AsyncTyper

app = AsyncTyper()


async def installation_metadata():
    async with AsyncDiracClient() as api:
        return await api.well_known.get_installation_metadata()


def vo_callback(vo: str | None) -> str:
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
async def generate_pilot_secrets(
    vo: Annotated[
        str,
        typer.Argument(callback=vo_callback, help="Virtual Organization name"),
    ],
    n: Annotated[
        int,
        typer.Argument(help="Number of secrets to generate."),
    ],
    expiration_minutes: Optional[int] = typer.Option(
        60,
        help="Expiration in minutes of the secrets.",
    ),
    max_use: Optional[int] = typer.Option(
        60,
        help="Number of uses max for a secret.",
    ),
):
    async with AsyncDiracClient() as api:
        secrets = await api.pilots.create_pilot_secrets(
            n=n,
            expiration_minutes=expiration_minutes,
            pilot_secret_use_count_max=max_use,
            vo=vo,
        )
        # Convert each model to dict
        secrets_dict = [secret.as_dict() for secret in secrets]

        print(json.dumps(secrets_dict, indent=2))
