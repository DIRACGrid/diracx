from __future__ import annotations

import argparse
import shlex
import subprocess
from importlib.resources import as_file, files
from pathlib import Path
from typing import NoReturn

SCRIPTS_BASE = files("diracx.testing").joinpath("scripts")


def parse_args() -> None:
    """Access to various utility scripts for testing DiracX and extensions."""
    parser = argparse.ArgumentParser(description="Utility for testing DiracX.")
    sp = parser.add_subparsers(dest="command", required=True)

    # Create the 'coverage' argument group.
    coverage_p = sp.add_parser("coverage", help="Coverage related commands")
    coverage_sp = coverage_p.add_subparsers(dest="subcommand", required=True)

    # Add the 'collect-demo' command under 'coverage'.
    collect_demo_p = coverage_sp.add_parser(
        "collect-demo", help="Collect demo coverage"
    )
    collect_demo_p.add_argument(
        "--demo-dir",
        required=True,
        type=Path,
        help="Path to the .demo dir of the diracx-charts repo.",
    )
    collect_demo_p.set_defaults(func=lambda a: coverage_collect_demo(a.demo_dir))

    args = parser.parse_args()
    args.func(args)


def coverage_collect_demo(demo_dir: Path) -> NoReturn:
    """Collect coverage data from a running instance of the demo.

    This script is primarily intended for use in CI/CD pipelines.
    """
    from diracx.core.extensions import extensions_by_priority

    client_extension_name = min(extensions_by_priority(), key=lambda x: x == "diracx")

    with as_file(SCRIPTS_BASE / "collect_demo_coverage.sh") as script_file:
        cmd = ["bash", str(script_file), "--demo-dir", str(demo_dir)]
        if client_extension_name in {"diracx", "gubbins"}:
            cmd += ["--diracx-repo", str(Path.cwd())]
            if client_extension_name == "gubbins":
                cmd += ["--extension-name", "gubbins"]
                cmd += ["--extension-repo", str(Path.cwd() / "extensions" / "gubbins")]
        else:
            cmd += ["--extension-name", client_extension_name]
            cmd += ["--extension-repo", str(Path.cwd())]
        print("Running:", shlex.join(cmd))
        proc = subprocess.run(cmd, check=False)
    if proc.returncode == 0:
        print("Process completed successfully.")
    else:
        print("Process failed with return code", proc.returncode)
    raise SystemExit(proc.returncode)


if __name__ == "__main__":
    parse_args()
