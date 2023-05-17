from __future__ import annotations

__all__ = ("app",)

from typing import List

from rich.console import Console
from rich.table import Table
from typer import FileText

from diracx.client.aio import Dirac
from diracx.client.models import JobSearchParams

from .utils import AsyncTyper

app = AsyncTyper()


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
    all: bool = False,
):
    async with Dirac(endpoint="http://localhost:8000") as api:
        jobs = await api.jobs.search(
            JobSearchParams(parameters=None if all else parameter)
        )
    display_rich(jobs, "jobs")


def display_rich(data, unit: str) -> None:
    if not data:
        print(f"No {unit} found")
        return

    console = Console()
    columns = [str(c) for c in data[0].keys()]
    if sum(map(len, columns)) > 0.75 * console.width:
        table = Table(
            "Parameter",
            "Value",
            caption=f"Showing {len(data)} of {len(data)} {unit}",
            caption_justify="right",
        )
        for job in data:
            for k, v in job.items():
                table.add_row(k, str(v))
            table.add_section()
    else:
        table = Table(
            *columns,
            caption=f"Showing {len(data)} of {len(data)} {unit}",
            caption_justify="right",
        )
        for job in data:
            table.add_row(*map(str, job.values()))
    console.print(table)


@app.async_command()
async def submit(jdl: List[FileText]):
    async with Dirac(endpoint="http://localhost:8000") as api:
        jobs = await api.jobs.submit_bulk_jobs(jdl)
    print(
        f"Inserted {len(jobs)} jobs with ids: {','.join(map(str, (job.job_id for job in jobs)))}"
    )
