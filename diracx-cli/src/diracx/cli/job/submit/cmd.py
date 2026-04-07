from __future__ import annotations

__all__: list[str] = []

import tempfile
from pathlib import Path
from typing import Annotated

import typer
import yaml

from ..._submission.pipeline import submit_cwl
from ..._submission.simple import detect_sandbox_files, generate_cwl
from . import app


@app.async_command(
    help="""Submit a simple command to the grid.

Runs COMMAND on a worker node. Local files referenced in the command
are automatically detected and shipped as input sandboxes.

Use --sandbox for additional files not mentioned in the command.

Examples:
  dirac job submit cmd "python my_script.py"
  dirac job submit cmd "python my_script.py" --sandbox config.json
""",
)
async def cmd(
    command: Annotated[str, typer.Argument(help="Shell command to run on the grid")],
    sandbox: Annotated[
        list[Path],
        typer.Option("--sandbox", help="Additional local files to ship"),
    ] = [],
    yes: Annotated[
        bool, typer.Option("-y", "--yes", help="Skip confirmation prompt")
    ] = False,
):
    """Submit a simple command to the grid."""
    # Auto-detect files from command
    auto_files = detect_sandbox_files(command)
    all_sandbox = list(set(auto_files + sandbox))

    # Generate CWL
    cwl = generate_cwl(command=command, sandbox_files=all_sandbox)

    # Write CWL to temp file (pipeline expects a Path)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cwl", delete=False) as f:
        yaml.dump(cwl, f)
        cwl_path = Path(f.name)

    try:
        # Build sandbox inputs if files exist
        input_files: list[Path] = []
        if all_sandbox:
            sandbox_input = {
                "sandbox_files": [
                    {"class": "File", "path": str(p)} for p in all_sandbox
                ]
            }
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False
            ) as inp_f:
                yaml.dump(sandbox_input, inp_f)
                input_files = [Path(inp_f.name)]

        results = await submit_cwl(
            workflow=cwl_path,
            input_files=input_files,
            cli_args=[],
            range_spec=None,
            yes=yes,
        )
        job_ids = [str(r.job_id) for r in results]
        print(f"Submitted {len(results)} job(s): {', '.join(job_ids)}")
    finally:
        cwl_path.unlink(missing_ok=True)
