"""CLI tool for running CWL workflows with the DIRAC executor.

Invoked as ``dirac-cwl-runner`` on the worker node. Handles mypyc
compatibility, replica map management, and cwltool integration.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import typer

logger = logging.getLogger("dirac-cwl-runner")

app = typer.Typer(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)


@app.command()
def main(  # noqa: B008
    ctx: typer.Context,
    workflow: Path = typer.Argument(  # noqa: B008
        ..., help="Path to CWL workflow file", exists=True
    ),
    inputs: Path | None = typer.Argument(  # noqa: B008
        None, help="Path to inputs YAML file (optional)"
    ),
    outdir: Path | None = typer.Option(None, help="Output directory"),  # noqa: B008
    tmpdir_prefix: Path | None = typer.Option(None, help="Temporary directory prefix"),  # noqa: B008
    leave_tmpdir: bool = typer.Option(False, help="Keep temporary directories"),  # noqa: B008
    replica_map: Path | None = typer.Option(  # noqa: B008
        None, help="Path to global replica map JSON file"
    ),
    preserve_environment: list[str] = typer.Option(  # noqa: B008
        [],
        "--preserve-environment",
        help="Preserve specific environment variable when running CommandLineTools.",
    ),
    preserve_entire_environment: bool = typer.Option(  # noqa: B008
        False,
        "--preserve-entire-environment",
        help="Preserve entire host environment when running CommandLineTools.",
    ),
    debug: bool = typer.Option(False, help="Enable debug logging"),  # noqa: B008
):
    """Run a CWL workflow with the DIRAC executor."""
    # UTC logging
    logging.Formatter.converter = lambda *_: datetime.now(timezone.utc).timetuple()

    start_time = datetime.now(timezone.utc)
    workflow_params = ctx.args or []

    # Build cwltool arguments
    cwltool_args = [
        "--outdir",
        str(outdir) if outdir else ".",
        "--disable-color",
    ]

    if tmpdir_prefix:
        cwltool_args.extend(["--tmpdir-prefix", str(tmpdir_prefix)])
    if leave_tmpdir:
        cwltool_args.append("--leave-tmpdir")
    if debug:
        cwltool_args.append("--debug")
    if preserve_entire_environment:
        cwltool_args.append("--preserve-entire-environment")
    else:
        for envvar in preserve_environment:
            cwltool_args.extend(["--preserve-environment", envvar])

    cwltool_args.append(str(workflow))
    if inputs:
        cwltool_args.append(str(inputs))
    if workflow_params:
        cwltool_args.extend(workflow_params)

    try:
        # mypyc patch is applied by __init__.py before these imports
        from cwltool.context import LoadingContext
        from cwltool.main import main as cwltool_main

        from . import DiracExecutor
        from .tool import dirac_make_tool

        dirac_executor = DiracExecutor(global_map_path=replica_map)

        logger.info("Workflow: %s", workflow.resolve())
        if inputs:
            logger.info("Inputs: %s", inputs.resolve())
        if replica_map:
            logger.info("Replica map: %s", replica_map)

        # Prevent our logger from duplicating cwltool output.
        # Use stderr so that only the cwltool JSON output goes to stdout,
        # allowing the caller to reliably parse it with json.loads().
        logger.propagate = False
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        loading_context = LoadingContext()
        loading_context.construct_tool_object = dirac_make_tool  # type: ignore[assignment]

        exit_code = cwltool_main(
            argsl=cwltool_args,
            executor=dirac_executor,
            loadingContext=loading_context,  # type: ignore[arg-type]
        )

        end_time = datetime.now(timezone.utc)
        duration = end_time - start_time

        # Write final replica map
        if exit_code == 0 and dirac_executor.global_map:
            output_dir = Path(outdir).resolve() if outdir else Path.cwd()
            final_map_path = output_dir / "replica_map.json"
            try:
                final_map_path.write_text(
                    dirac_executor.global_map.model_dump_json(indent=2)
                )
                logger.info(
                    "Replica map written: %s (%d entries)",
                    final_map_path,
                    len(dirac_executor.global_map.root),
                )
            except Exception as e:
                logger.warning("Could not write final replica map: %s", e)

        if exit_code == 0:
            logger.info(
                "Workflow completed successfully in %s", str(duration).split(".")[0]
            )
        else:
            logger.error(
                "Workflow failed with exit code %d after %s",
                exit_code,
                str(duration).split(".")[0],
            )

        sys.exit(exit_code)

    except SystemExit:
        raise
    except Exception as e:
        logger.exception("Error executing workflow: %s", e)
        sys.exit(1)


def cli():
    """Entry point for the CLI when installed as a script."""
    app()


if __name__ == "__main__":
    cli()
