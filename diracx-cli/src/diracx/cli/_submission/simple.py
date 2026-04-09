from __future__ import annotations

__all__ = ("detect_sandbox_files", "generate_cwl")

import shlex
from pathlib import Path


def detect_sandbox_files(command: str) -> list[Path]:
    """Detect local files referenced in a command string.

    Tokenizes the command and checks each token against the filesystem.
    Only includes files that are:
    - Regular files (not directories)
    - Not symlinks
    - Relative paths (not absolute)
    """
    tokens = shlex.split(command)
    result: list[Path] = []
    for token in tokens:
        p = Path(token)
        if not p.is_absolute() and p.exists() and p.is_file() and not p.is_symlink():
            result.append(p)
    return result


def generate_cwl(
    command: str,
    sandbox_files: list[Path],
) -> dict:
    """Generate a minimal CWL CommandLineTool from a shell command."""
    tokens = shlex.split(command)
    label = tokens[0] if tokens else "command"
    if len(tokens) > 1 and not tokens[1].startswith("-"):
        label = Path(tokens[1]).stem

    cwl: dict = {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "label": label,
        "hints": [
            {
                "class": "dirac:Job",
                "schema_version": "1.0",
                "type": "User",
            }
        ],
        "baseCommand": ["bash", "-c", command],
        "stdout": "stdout.log",
        "stderr": "stderr.log",
        "inputs": [],
        "outputs": [
            {"id": "stdout_log", "type": "stdout"},
            {"id": "stderr_log", "type": "stderr"},
        ],
        "$namespaces": {"dirac": "https://diracgrid.org/cwl#"},
    }

    if sandbox_files:
        cwl["hints"][0]["input_sandbox"] = [{"source": "sandbox_files"}]
        cwl["inputs"].append(
            {
                "id": "sandbox_files",
                "type": {"type": "array", "items": "File"},
            }
        )

    return cwl
