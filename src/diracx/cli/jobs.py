# Can't using PEP-604 with typer: https://github.com/tiangolo/typer/issues/348
# from __future__ import annotations

__all__ = ("app",)

import json
import os
import re
from typing import Annotated, Any, cast

from rich.console import Console
from rich.table import Table
from typer import FileText, Option

from diracx.client.aio import Dirac
from diracx.core.models import ScalarSearchOperator, SearchSpec, VectorSearchOperator

from .utils import AsyncTyper, get_auth_headers

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
    page: int = 1,
    per_page: int = 10,
):
    async with Dirac(endpoint="http://localhost:8000") as api:
        content_range, jobs = cast(
            tuple[ContentRange, dict[str, Any]],
            await api.jobs.search(
                parameters=None if all else parameter,
                search=condition if condition else None,
                headers=get_auth_headers(),
                page=page,
                per_page=per_page,
                cls=lambda a, b, c: (
                    ContentRange(a.http_response.headers["Content-Range"]),
                    b,
                ),
            ),
        )
    display(jobs, content_range)


def display(data, content_range: "ContentRange"):
    format = os.environ["DIRACX_OUTPUT_FORMAT"]
    if format == "json":
        print(json.dumps(data, indent=2))
    elif format == "rich":
        display_rich(data, content_range)
    else:
        raise NotImplementedError(format)


def display_rich(data, content_range: "ContentRange") -> None:
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
    async with Dirac(endpoint="http://localhost:8000") as api:
        jobs = await api.jobs.submit_bulk_jobs(
            [x.read() for x in jdl], headers=get_auth_headers()
        )
    print(
        f"Inserted {len(jobs)} jobs with ids: {','.join(map(str, (job.job_id for job in jobs)))}"
    )


class ContentRange:
    unit: str | None = None
    start: int | None = None
    end: int | None = None
    total: int | None = None

    def __init__(self, header):
        if match := re.fullmatch(r"(\w+) (\d+-\d+|\*)/(\d+|\*)", header):
            self.unit, range, total = match.groups()
            self.total = int(total)
            if range != "*":
                self.start, self.end = map(int, range.split("-"))

    @property
    def caption(self):
        if self.start is None or self.end is None:
            range_str = "unknown"
        else:
            range_str = f"{self.start}-{self.end}"
        return f"Showing {range_str} of {self.total or 'unknown'} {self.unit}"
