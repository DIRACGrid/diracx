from __future__ import annotations

import os
from typing import Optional

from typer import Option

from diracx.client.aio import DiracClient

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
        await api.login(scopes)
    print("\nLogin successful!")


@app.async_command()
async def logout():
    async with DiracClient() as api:
        await api.logout()
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
