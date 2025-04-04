# Can't using PEP-604 with typer: https://github.com/tiangolo/typer/issues/348
# from __future__ import annotations
from __future__ import annotations

__all__ = ("app",)

import json
import re
from typing import Annotated, cast

from rich.console import Console
from rich.table import Table
from typer import FileText, Option

from diracx.client.aio import AsyncDiracClient
from diracx.core.models import ScalarSearchOperator, SearchSpec, VectorSearchOperator
from diracx.core.preferences import OutputFormats, get_diracx_preferences

from .utils import AsyncTyper

app = AsyncTyper()


available_operators = (
    f"Scalar operators: {', '.join([op.value for op in ScalarSearchOperator])}. "
    f"Vector operators: {', '.join([op.value for op in VectorSearchOperator])}."
)


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


class ContentRange:
    unit: str | None = None
    start: int | None = None
    end: int | None = None
    total: int | None = None

    def __init__(self, header: str):
        if match := re.fullmatch(r"(\w+) (\d+-\d+|\*)/(\d+|\*)", header):
            self.unit, range, total = match.groups()
            self.total = int(total)
            if range != "*":
                self.start, self.end = map(int, range.split("-"))
        elif match := re.fullmatch(r"\w+", header):
            self.unit = match.group()

    @property
    def caption(self):
        if self.start is None and self.end is None:
            range_str = "all"
        else:
            range_str = (
                f"{self.start if self.start is not None else 'unknown'}-"
                f"{self.end if self.end is not None else 'unknown'} "
                f"of {self.total or 'unknown'}"
            )
        return f"Showing {range_str} {self.unit}"


def display(data, content_range: ContentRange):
    output_format = get_diracx_preferences().output_format
    match output_format:
        case OutputFormats.JSON:
            print(json.dumps(data, indent=2))
        case OutputFormats.RICH:
            display_rich(data, content_range)
        case _:
            raise NotImplementedError(output_format)


def display_rich(data, content_range: ContentRange) -> None:
    if not data:
        print(f"No {content_range.unit} found")
        return

    console = Console()
    columns = [str(c) for c in data[0].keys()]
    if sum(map(len, columns)) > 0.75 * console.width:
        table = Table(
            "Parameter",
            "Value",
            caption=content_range.caption,
            caption_justify="right",
        )
        for job in data:
            for k, v in job.items():
                table.add_row(k, str(v))
            table.add_section()
    else:
        table = Table(
            *columns,
            caption=content_range.caption,
            caption_justify="right",
        )
        for job in data:
            table.add_row(*map(str, job.values()))
    console.print(table)


@app.async_command()
async def submit(jdl: list[FileText]):
    async with AsyncDiracClient() as api:
        jobs = await api.jobs.submit_jdl_jobs([x.read() for x in jdl])
    print(
        f"Inserted {len(jobs)} jobs with ids: {','.join(map(str, (job.job_id for job in jobs)))}"
    )
