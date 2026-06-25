# Can't using PEP-604 with typer: https://github.com/tiangolo/typer/issues/348
# from __future__ import annotations
from __future__ import annotations

__all__ = ["app"]

import json
import re
from typing import Annotated, cast

from rich.console import Console
from rich.table import Table
from typer import FileText, Option

from diracx.client.aio import AsyncDiracClient
from diracx.core.models import (
    ScalarSearchOperator,
    SearchSpec,
    VectorSearchOperator,
)
from diracx.core.preferences import OutputFormats, get_diracx_preferences

from .utils import AsyncTyper

app = AsyncTyper()


available_operators = (
    f"Scalar operators: {', '.join([op.value for op in ScalarSearchOperator])}. "
    f"Vector operators: {', '.join([op.value for op in VectorSearchOperator])}."
)


def parse_condition(value: str) -> SearchSpec:
    """Parse a single search condition into a `SearchSpec`.

    The expected string format is ``"<parameter> <operator> <value>"``. For
    scalar operators the ``value`` is returned as ``value``; for vector
    operators the ``value`` is parsed as JSON and returned under ``values``.

    Args:
        value (str): Condition string, e.g. ``"JobID eq 1000"`` or
            ``"Embedding cos_sim [0.1, 0.2, 0.3]"``.

    Returns:
        SearchSpec: Dictionary describing the parsed condition in the
            shape expected by the API client.

    Raises:
        ValueError: If the operator is unknown or the input cannot be parsed.
    """
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
    """Search for jobs and display results.

    The command constructs a list of ``SearchSpec`` objects from the
    provided ``condition`` arguments and performs a paginated job search
    through the API client. Results are displayed using the user's
    configured output format.

    Args:
        parameter (list[str]): List of fields to return for each job. Use
            the special flag ``--all`` to return all available parameters.
        condition (list[str]): Search condition strings (see
            ``parse_condition``) that will be combined with AND semantics.
        all (bool): If true, ignore ``parameter`` and request all fields.
        page (int): Page number for pagination (1-based).
        per_page (int): Number of items per page.

    Returns:
        None: Results are printed to stdout using the configured display
            format (JSON or rich table).
    """
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
    """Parse and represent a `Content-Range` response header.

    The class understands headers of the form ``"unit start-end/total"``
    (e.g. ``"jobs 0-9/100"``) and exposes parsed attributes suitable for
    building human-readable captions for CLI output.
    """

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
    """Render search results using the configured output format.

    The helper consults the user's ``output_format`` preference and routes
    the data to the appropriate renderer. Supported formats are JSON and
    rich-table output.

    Args:
        data: JSON-serializable list of job records returned by the API.
        content_range (ContentRange): Parsed content-range metadata used for
            captions in the rich renderer.

    Raises:
        NotImplementedError: If the configured output format is unsupported.
    """
    output_format = get_diracx_preferences().output_format
    match output_format:
        case OutputFormats.JSON:
            print(json.dumps(data, indent=2))
        case OutputFormats.RICH:
            display_rich(data, content_range)
        case _:
            raise NotImplementedError(output_format)


def display_rich(data, content_range: ContentRange) -> None:
    """Render a rich table representation of the job results.

    Chooses between a two-column parameter/value layout (for wide or
    numerous columns) and a multi-column table. The table caption displays
    the parsed content-range information.

    Args:
        data: List of job records (each a mapping of parameter -> value).
        content_range (ContentRange): Parsed content-range metadata.
    """
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
    """Submit one or more JDL job descriptions and print the inserted IDs.

    The command accepts one or more files containing JDL job descriptions,
    submits them to the server via the client API, and prints a summary of
    the inserted job IDs.

    Args:
        jdl (list[FileText]): List of file-like objects pointing to JDL
            descriptions.

    Returns:
        None
    """
    async with AsyncDiracClient() as api:
        jobs = await api.jobs.submit_jdl_jobs([x.read() for x in jdl])
    print(
        f"Inserted {len(jobs)} jobs with ids: {','.join(map(str, (job.job_id for job in jobs)))}"
    )
