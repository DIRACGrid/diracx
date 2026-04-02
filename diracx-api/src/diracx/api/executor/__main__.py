"""CLI tool for running CWL workflows with DIRAC executor.

This command-line tool runs CWL workflows using the DiracExecutor, which handles
replica map management for input and output files.
"""
# ruff: noqa: B008, N803
# mypy: disable-error-code="truthy-bool"

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from importlib.metadata import version as get_version
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def _get_package_version(package: str) -> str:
    """Get version of a package, returning 'unknown' if not installed."""
    try:
        return get_version(package)
    except Exception:
        return "unknown"


# Create Typer app with context settings to allow extra arguments
app = typer.Typer(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)


# Configure logging to use UTC
def configure_utc_logging():
    """Configure logging to use UTC timestamps."""
    logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()


def version_callback(value: bool):
    """Handle --version flag."""
    if value:
        console.print("[cyan]dirac-cwl executor[/cyan]")
        console.print(f"diracx: [green]{_get_package_version('diracx-api')}[/green]")
        raise typer.Exit()


def check_and_generate_inputs(
    workflow_path: Path,
    inputs_path: Path | None,
    replica_map_path: Path | None,
    **kwargs,
) -> tuple[Path | None, Path | None]:
    """Return provided inputs and replica map paths as-is.

    Production-level auto-generation of inputs from dataset plugins
    is not available in diracx. Inputs and replica maps must be
    provided explicitly.
    """
    return inputs_path, replica_map_path


@app.command()
def main(
    ctx: typer.Context,
    workflow: Path = typer.Argument(..., help="Path to CWL workflow file", exists=True),
    inputs: Path | None = typer.Argument(
        None, help="Path to inputs YAML file (optional)"
    ),
    outdir: Path = typer.Option(
        None, help="Output directory (default: current directory)"
    ),
    tmpdir_prefix: Path = typer.Option(None, help="Temporary directory prefix"),
    leave_tmpdir: bool = typer.Option(False, help="Keep temporary directories"),
    replica_map: Path = typer.Option(None, help="Path to global replica map JSON file"),
    n_lfns: int = typer.Option(
        None,
        "--n-lfns",
        help="Number of LFNs to retrieve when auto-generating inputs (default: all available LFNs)",
    ),
    pick_smallest_lfn: bool = typer.Option(
        False,
        "--pick-smallest-lfn",
        help="Pick the smallest file(s) for faster testing (requires --n-lfns)",
    ),
    force_regenerate: bool = typer.Option(
        False,
        "--force-regenerate",
        help="Force regeneration of inputs/replica map without confirmation",
    ),
    print_workflow: bool = typer.Option(
        False, "--print-workflow", help="Print the workflow structure before execution"
    ),
    preserve_environment: list[str] = typer.Option(
        [],
        "--preserve-environment",
        help="Preserve specific environment variable when running CommandLineTools. May be provided multiple times.",
    ),
    preserve_entire_environment: bool = typer.Option(
        False,
        "--preserve-entire-environment",
        help="Preserve entire host environment when running CommandLineTools.",
    ),
    debug: bool = typer.Option(False, help="Enable debug logging"),
    verbose: bool = typer.Option(False, help="Enable verbose logging"),
    parallel: bool = typer.Option(False, help="Run jobs in parallel"),
    version: bool = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show version information",
    ),
):
    r"""Run CWL workflows with DIRAC executor.

    \b
    Workflow-specific parameters can be passed directly and will be forwarded to the workflow:
        dirac-cwl-run workflow.cwl --event-type 27165175 --run-number 12345

    \b
    Parameters recognized by dirac-cwl-run (like --outdir, --debug) must come before workflow parameters.
    If there's ambiguity, use -- to separate:
        dirac-cwl-run workflow.cwl --outdir myout -- --event-type 27165175
    """
    # Configure logging to use UTC
    configure_utc_logging()

    # Record start time
    start_time = datetime.now(timezone.utc)

    # Extract workflow parameters from context (passed after known options)
    workflow_params = ctx.args if ctx.args else []

    # Check and auto-generate inputs and catalog if needed
    actual_inputs, actual_replica_map = check_and_generate_inputs(
        workflow_path=workflow,
        inputs_path=inputs,
        replica_map_path=replica_map,
        n_lfns=n_lfns,
        pick_smallest=pick_smallest_lfn,
        force=force_regenerate,
    )

    # Build cwltool arguments
    cwltool_args = [
        "--outdir",
        str(outdir) if outdir else ".",
        "--disable-color",  # Disable ANSI color codes in logs
    ]

    if tmpdir_prefix:
        cwltool_args.extend(["--tmpdir-prefix", str(tmpdir_prefix)])

    if leave_tmpdir:
        cwltool_args.append("--leave-tmpdir")

    if debug:
        cwltool_args.append("--debug")
    elif verbose:
        cwltool_args.append("--verbose")

    if parallel:
        cwltool_args.append("--parallel")

    if preserve_entire_environment:
        cwltool_args.append("--preserve-entire-environment")
    else:
        for envvar in preserve_environment:
            cwltool_args.extend(["--preserve-environment", envvar])

    # Workflow visualization removed — not needed for worker-side execution

    # Add workflow and inputs
    cwltool_args.append(str(workflow))
    if actual_inputs:
        cwltool_args.append(str(actual_inputs))

    # Add any extra workflow parameters passed by the user
    if workflow_params:
        cwltool_args.extend(workflow_params)

    try:
        # Import hook is installed by __init__.py before any cwltool import.
        from cwltool.context import LoadingContext
        from cwltool.main import main as cwltool_main

        from . import DiracExecutor
        from .tool import dirac_make_tool

        dirac_executor = DiracExecutor(global_map_path=actual_replica_map)

        # Display execution info
        console.print()
        console.print(
            Panel.fit(
                "[bold cyan]DIRAC CWL Workflow Executor[/bold cyan]",
                border_style="cyan",
            )
        )

        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("Key", style="bold")
        table.add_column("Value")

        table.add_row(
            "Start time (UTC):",
            f"[cyan]{start_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
        )
        table.add_row("CWL Workflow:", f"[cyan]{workflow.resolve()}[/cyan]")
        if actual_inputs:
            table.add_row(
                "Input Parameter File:", f"[cyan]{actual_inputs.resolve()}[/cyan]"
            )

        table.add_row("Current working directory:", f"[cyan]{Path.cwd()}[/cyan]")
        table.add_row(
            "Temporary dir prefix:",
            f"[cyan]{tmpdir_prefix if tmpdir_prefix else 'system default'}[/cyan]",
        )
        table.add_row(
            "Output directory:",
            f"[cyan]{Path(outdir).resolve() if outdir else '.'}[/cyan]",
        )

        console.print(table)
        console.print()
        console.print(
            "[green]✓[/green] Using DIRAC executor with replica map management"
        )

        if actual_replica_map:
            console.print(
                f"[green]✓[/green] Global replica map: [cyan]{actual_replica_map}[/cyan]"
            )
        else:
            console.print(
                "[yellow]⚠[/yellow] No replica map provided - will create empty replica map"
            )

        # Show workflow parameters if provided
        if workflow_params:
            console.print(
                f"[green]✓[/green] Workflow parameters: [cyan]{' '.join(workflow_params)}[/cyan]"
            )

        console.print()

        # Show execution start message
        console.print(
            Panel.fit(
                "[bold green]▶[/bold green] Starting workflow execution with cwltool...",
                border_style="green",
                padding=(0, 2),
            )
        )
        console.print()

        # Let cwltool manage its own logging (coloredlogs to stderr).
        # Only set up a handler for dirac-cwl-run so our executor messages
        # go to stdout without duplicating cwltool output.
        _dcr = logging.getLogger("dirac-cwl-run")
        _dcr.propagate = False
        _dcr_handler = logging.StreamHandler(sys.stdout)
        _dcr_handler.setFormatter(logging.Formatter("%(message)s"))
        _dcr.addHandler(_dcr_handler)
        _dcr.setLevel(logging.INFO)

        # Create LoadingContext with our custom tool factory so cwltool
        # uses DiracCommandLineTool (which supports custom path mappers)
        # instead of the default CommandLineTool.
        loading_context = LoadingContext()
        loading_context.construct_tool_object = dirac_make_tool

        exit_code = cwltool_main(
            argsl=cwltool_args,
            executor=dirac_executor,
            loadingContext=loading_context,
        )

        # Record end time and calculate duration
        end_time = datetime.now(timezone.utc)
        duration = end_time - start_time

        if exit_code == 0:
            console.print()
            console.print(
                Panel.fit(
                    "[bold green]✅ Workflow Execution Complete[/bold green]",
                    border_style="green",
                )
            )

            # Build results table
            results_table = Table(show_header=False, box=None, padding=(0, 2))
            results_table.add_column("Key", style="bold")
            results_table.add_column("Value")

            results_table.add_row("Status:", "[green]Success[/green]")
            results_table.add_row(
                "Start time (UTC):",
                f"[cyan]{start_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
            )
            results_table.add_row(
                "End time (UTC):",
                f"[cyan]{end_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
            )
            results_table.add_row(
                "Duration:", f"[cyan]{str(duration).split('.')[0]}[/cyan]"
            )
            results_table.add_row(
                "Output directory:",
                f"[cyan]{Path(outdir).resolve() if outdir else '.'}[/cyan]",
            )

            # Write final global replica map to output directory
            output_dir_path = Path(outdir).resolve() if outdir else Path.cwd()
            final_replica_map_path = output_dir_path / "replica_map.json"

            if dirac_executor.global_map:
                try:
                    final_replica_map_path.write_text(
                        dirac_executor.global_map.model_dump_json(indent=2)
                    )
                    results_table.add_row(
                        "Final replica map:", f"[cyan]{final_replica_map_path}[/cyan]"
                    )
                    results_table.add_row(
                        "Replica map entries:",
                        f"[cyan]{len(dirac_executor.global_map.root)}[/cyan]",
                    )
                except Exception as e:
                    console.print(
                        f"[yellow]⚠ Warning:[/yellow] Could not write final replica map: {e}"
                    )

            # Show original replica map if it was different
            if (
                actual_replica_map
                and actual_replica_map.exists()
                and actual_replica_map != final_replica_map_path
            ):
                results_table.add_row(
                    "Input replica map:",
                    f"[dim][cyan]{actual_replica_map}[/cyan][/dim]",
                )

            console.print(results_table)
            console.print()

        else:
            console.print()
            console.print(
                Panel.fit(
                    f"[bold red]❌ Workflow Execution Failed[/bold red]\n[dim]Exit code: {exit_code}[/dim]",
                    border_style="red",
                )
            )

            # Build failure table
            failure_table = Table(show_header=False, box=None, padding=(0, 2))
            failure_table.add_column("Key", style="bold")
            failure_table.add_column("Value")

            failure_table.add_row("Status:", "[red]Failed[/red]")
            failure_table.add_row(
                "Start time (UTC):",
                f"[cyan]{start_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
            )
            failure_table.add_row(
                "End time (UTC):",
                f"[cyan]{end_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
            )
            failure_table.add_row(
                "Duration:", f"[cyan]{str(duration).split('.')[0]}[/cyan]"
            )
            failure_table.add_row("Exit code:", f"[red]{exit_code}[/red]")

            console.print(failure_table)
            console.print()

        sys.exit(exit_code)

    except SystemExit:
        raise
    except Exception as e:
        console.print(f"\n[red]❌ Error executing workflow:[/red] {e}")
        console.print_exception()
        sys.exit(1)


def cli():
    """Entry point for the CLI when installed as a script."""
    app()


if __name__ == "__main__":
    cli()
