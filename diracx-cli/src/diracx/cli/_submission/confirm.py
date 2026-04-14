# diracx-cli/src/diracx/cli/_submission/confirm.py
from __future__ import annotations

__all__ = ("needs_confirmation", "build_summary", "prompt_confirmation")


def _fmt_bytes(n: int) -> str:
    """Format bytes as human-readable string."""
    if n >= 1024**3:
        return f"{n / 1024**3:.1f} GB"
    elif n >= 1024**2:
        return f"{n / 1024**2:.1f} MB"
    elif n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


def needs_confirmation(num_jobs: int, yes: bool = False) -> bool:
    """Check if submission needs user confirmation."""
    if yes:
        return False
    return num_jobs > 100


def build_summary(
    *,
    workflow_name: str,
    workflow_path: str,
    num_jobs: int,
    source: str,
    num_unique_sandboxes: int,
    total_sandbox_bytes: int,
    num_lfn_inputs: int,
) -> str:
    """Build a human-readable submission summary string."""
    lines = [
        "Submission summary:",
        f"  Workflow:    {workflow_path} ({workflow_name})",
        f"  Jobs:        {num_jobs:,}",
        f"  Source:      {source}",
    ]

    if num_unique_sandboxes > 0:
        lines.append(
            f"  Sandboxes:   {num_unique_sandboxes} unique ({_fmt_bytes(total_sandbox_bytes)})"
        )

    if num_lfn_inputs > 0:
        lines.append(f"  LFN inputs:  {num_lfn_inputs:,}")

    return "\n".join(lines)


def prompt_confirmation(summary: str) -> bool:
    """Display summary and prompt for confirmation. Returns True if user confirms."""
    print(summary)
    response = input("\nProceed? [y/N] ").strip().lower()
    return response in ("y", "yes")
