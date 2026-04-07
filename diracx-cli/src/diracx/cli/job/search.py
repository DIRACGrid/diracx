from __future__ import annotations

__all__: list[str] = []

from typing import Annotated, cast

from typer import Option

from diracx.client.aio import AsyncDiracClient

from ..jobs import ContentRange, available_operators, display, parse_condition
from . import app


@app.async_command()
async def search(
    parameter: list[str] = [
        "JobID",
        "Status",
        "MinorStatus",
        "ApplicationStatus",
        "JobGroup",
        "Site",
        "JobName",
        "Owner",
        "LastUpdateTime",
    ],
    condition: Annotated[
        list[str], Option(help=f'Example: "JobID eq 1000". {available_operators}')
    ] = [],
    all: bool = False,
    page: int = 1,
    per_page: int = 10,
):
    search_specs = [parse_condition(cond) for cond in condition]
    async with AsyncDiracClient() as api:
        jobs, content_range = await api.jobs.search(
            parameters=None if all else parameter,
            search=search_specs if search_specs else None,
            page=page,
            per_page=per_page,
            cls=lambda _, jobs, headers: (
                jobs,
                ContentRange(headers.get("Content-Range", "jobs")),
            ),
        )

    display(jobs, cast(ContentRange, content_range))
