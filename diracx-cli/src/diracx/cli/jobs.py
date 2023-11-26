# Can't using PEP-604 with typer: https://github.com/tiangolo/typer/issues/348
# from __future__ import annotations

__all__ = ("app",)

import json
from typing import Annotated

from rich.console import Console
from rich.table import Table
from typer import FileText, Option

from diracx.client.aio import DiracClient
from diracx.core.models import ScalarSearchOperator, SearchSpec, VectorSearchOperator
from diracx.core.preferences import OutputFormats, get_diracx_preferences

from .utils import AsyncTyper

app = AsyncTyper()


def parse_condition(value: str) -> SearchSpec:
    parameter, operator, rest = value.split(" ", 2)
    if operator in set(ScalarSearchOperator):
        return {
            "parameter": parameter,
            "operator": ScalarSearchOperator(operator),
            "value": rest,
        }
    elif operator in set(VectorSearchOperator):
        return {
            "parameter": parameter,
            "operator": VectorSearchOperator(operator),
            "values": json.loads(rest),
        }
    else:
        raise ValueError(f"Unknown operator {operator}")


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
    condition: Annotated[list[SearchSpec], Option(parser=parse_condition)] = [],
    all: bool = False,
):
    async with DiracClient() as api:
        jobs = await api.jobs.search(
            parameters=None if all else parameter,
            search=condition if condition else None,
        )
    display(jobs, "jobs")


def display(data, unit: str):
    match get_diracx_preferences().output_format:
        case OutputFormats.JSON:
            print(json.dumps(data, indent=2))
        case OutputFormats.RICH:
            display_rich(data, unit)
        case _:
            raise NotImplementedError(format)


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
async def submit(jdl: list[FileText]):
    async with DiracClient() as api:
        # api.valid(enforce_https=False)
        jobs = await api.jobs.submit_bulk_jobs([x.read() for x in jdl])
    print(
        f"Inserted {len(jobs)} jobs with ids: {','.join(map(str, (job.job_id for job in jobs)))}"
    )
