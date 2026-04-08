from __future__ import annotations

__all__: list[str] = []

from pathlib import Path
from typing import Annotated

import typer

from ..._submission.pipeline import submit_cwl
from . import app


@app.async_command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
    help="""Submit a CWL workflow to the grid.

WORKFLOW is a CWL file (.cwl). INPUTS are optional YAML/JSON files
providing input values (one job per file, or one job per YAML document).

Workflow inputs can also be passed as CLI arguments after a -- separator.
These are parsed against the workflow's declared input parameters:

  dirac job submit cwl workflow.cwl -- --message "hello" --count 42

Combine file inputs with CLI overrides:

  dirac job submit cwl workflow.cwl base.yaml -- --message "override"
""",
)
async def cwl(
    ctx: typer.Context,
    workflow: Annotated[Path, typer.Argument(help="CWL workflow file (.cwl)")],
    range: Annotated[
        str | None,
        typer.Option(
            "--range",
            help="Parametric range: PARAM=END, PARAM=START:END, or PARAM=START:END:STEP",
        ),
    ] = None,
    yes: Annotated[
        bool, typer.Option("-y", "--yes", help="Skip confirmation prompt")
    ] = False,
):
    """Submit a CWL workflow to the grid."""
    # ctx.args contains: extra positional paths (input files) and
    # unknown options passed after -- (cli args for the workflow).
    # Positional file args appear before any --option args.
    input_files: list[Path] = []
    cli_args: list[str] = []
    in_cli = False
    for arg in ctx.args:
        if arg.startswith("-"):
            in_cli = True
        if in_cli:
            cli_args.append(arg)
        else:
            input_files.append(Path(arg))

    results = await submit_cwl(
        workflow=workflow,
        input_files=input_files,
        cli_args=cli_args,
        range_spec=range,
        yes=yes,
    )
    job_ids = [str(r.job_id) for r in results]
    print(f"Submitted {len(results)} job(s): {', '.join(job_ids)}")
