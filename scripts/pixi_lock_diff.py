#!/usr/bin/env python3
"""Summarise how pixi.lock changes between two revisions.

The raw diff of pixi.lock is too large to review, so this lists the packages
which were added, removed, upgraded or downgraded instead.

Usage:
    pixi run lock-diff [--base REF] [--head REF]
        Print the summary as markdown. By default the working tree's pixi.lock
        is compared with the one on origin/main.

In CI the summary is also written as a pull request comment with --comment.
The comment falls back to listing only the downgrades and major version changes
when the full table doesn't fit, linking to --details-url for the rest. The
number of downgraded packages is written to $GITHUB_OUTPUT as ``downgrades``.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

from pixi_diff_to_markdown.diff import generate_output
from pixi_diff_to_markdown.models import ChangeType, Diff, UpdateSpec
from pixi_diff_to_markdown.settings import MergeDependencies, Settings
from rattler import Version

# Used to find the comment again when updating it
COMMENT_MARKER = "<!-- pixi-lock-diff -->"
# GitHub rejects comments longer than 65536 characters, leave room for the header
MAX_TABLE_LENGTH = 60_000
# Maximum number of entries in each list of the fallback comment
MAX_LIST_ENTRIES = 100
KINDS = ["added", "removed", "upgraded", "downgraded", "rebuilt"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--base", default="origin/main", help="Revision to compare against"
    )
    parser.add_argument(
        "--head", help="Revision to compare, defaults to the working tree"
    )
    parser.add_argument("--summary", type=Path, help="Write the summary to this file")
    parser.add_argument(
        "--comment", type=Path, help="Write a pull request comment to this file"
    )
    parser.add_argument("--details-url", help="Where to find the full summary")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        before = Path(tmpdir) / "before.lock"
        before.write_bytes(read_lock(args.base))
        if args.head:
            after = Path(tmpdir) / "after.lock"
            after.write_bytes(read_lock(args.head))
        else:
            after = Path("pixi.lock")
        diff_json = subprocess.run(  # noqa: S603
            ["pixi-diff", "--manifest-path", "pixi.toml", str(before), str(after)],  # noqa: S607
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout
    diff = Diff.model_validate_json(diff_json)
    changes = collect_changes(diff)
    table = ""
    if diff.environment.root:
        table = generate_output(
            diff.environment,
            Settings(**{"merge-dependencies": MergeDependencies.split_explicit}),
        )

    summary = f"{describe_counts(changes)}\n\n{table}".strip()
    print(summary)
    if args.summary:
        args.summary.write_text(summary)
    if args.comment:
        args.comment.write_text(make_comment(changes, table, args.details_url))

    downgrades = changes["downgraded"]
    n_downgraded = len({spec.name for spec in downgrades})
    if downgrades:
        print(f"\n{n_downgraded} downgraded package(s):", file=sys.stderr)
        print("\n".join(format_list(downgrades)), file=sys.stderr)
    if github_output := os.environ.get("GITHUB_OUTPUT"):
        with open(github_output, "a") as fh:
            fh.write(f"downgrades={n_downgraded}\n")


def read_lock(revision: str) -> bytes:
    """Read pixi.lock as it is at the given git revision."""
    result = subprocess.run(  # noqa: S603
        ["git", "show", f"{revision}:pixi.lock"],  # noqa: S607
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        sys.exit(f"Failed to read pixi.lock at {revision}: {result.stderr.decode()}")
    return result.stdout


def classify(spec: UpdateSpec) -> str:
    """Describe a change using one of KINDS."""
    if spec.before is None:
        return "added"
    if spec.after is None:
        return "removed"
    try:
        before = Version(spec.before.version)
        after = Version(spec.after.version)
    except Exception:
        # Packages without a version, such as git dependencies
        return "rebuilt"
    if after > before:
        return "upgraded"
    if after < before:
        return "downgraded"
    return "rebuilt"


def collect_changes(diff: Diff) -> dict[str, dict[UpdateSpec, int]]:
    """Group the changes by kind, counting how many environments each affects.

    The same change appears once per environment and platform, UpdateSpec's
    hash ignores the package hashes so these are merged together.
    """
    changes: dict[str, dict[UpdateSpec, set[str]]] = {
        kind: defaultdict(set) for kind in KINDS
    }
    for environment, platforms in diff.environment.root.items():
        for dependencies in platforms.root.values():
            for spec in dependencies.root:
                changes[classify(spec)][spec].add(environment)
    return {
        kind: {
            spec: len(specs[spec])
            for spec in sorted(specs, key=lambda s: (s.name, s.before_after_str()))
        }
        for kind, specs in changes.items()
    }


def describe_counts(changes: dict[str, dict[UpdateSpec, int]]) -> str:
    counts = {
        kind: len({spec.name for spec in specs}) for kind, specs in changes.items()
    }
    if not any(counts.values()):
        return "No packages changed."
    total = len({spec.name for specs in changes.values() for spec in specs})
    details = ", ".join(f"{n} {kind}" for kind, n in counts.items() if n)
    return f"**{total} package(s) changed:** {details}."


def format_list(specs: dict[UpdateSpec, int]) -> list[str]:
    lines = []
    for spec, n_envs in list(specs.items())[:MAX_LIST_ENTRIES]:
        before, after = spec.before_after_str()
        lines.append(f"- `{spec.name}` {before} → {after} ({n_envs} environment(s))")
    if len(specs) > MAX_LIST_ENTRIES:
        lines.append(f"- … and {len(specs) - MAX_LIST_ENTRIES} more")
    return lines


def make_comment(
    changes: dict[str, dict[UpdateSpec, int]], table: str, details_url: str | None
) -> str:
    lines = [COMMENT_MARKER, "### `pixi.lock` changes", "", describe_counts(changes)]

    if downgrades := changes["downgraded"]:
        lines += [
            "",
            "> [!WARNING]",
            "> This pull request downgrades packages. If that isn't intended, it "
            "usually comes from resolving a `pixi.lock` conflict by keeping this "
            "branch's version, which rolls back changes made on `main`. Take the "
            "lock file from `main` and re-lock instead:",
            ">",
            "> ```bash",
            "> git checkout origin/main -- pixi.lock",
            "> pixi lock",
            "> ```",
            ">",
            "> If the downgrade is intended, explain why in the description and ask "
            "a maintainer to add the `deps: intentional-lock-change` label.",
        ]

    if len(table) <= MAX_TABLE_LENGTH:
        if table:
            lines += ["", table]
        return "\n".join(lines)

    see_details = f"[the job summary]({details_url})" if details_url else "the CI logs"
    lines += ["", f"The full table is too long for a comment, see {see_details}."]
    if downgrades:
        lines += ["", "#### Downgrades", "", *format_list(downgrades)]
    majors = {
        spec: n_envs
        for specs in changes.values()
        for spec, n_envs in specs.items()
        if spec.change_type in (ChangeType.MAJOR_UP, ChangeType.MAJOR_DOWN)
    }
    if majors:
        lines += ["", "#### Major version changes", "", *format_list(majors)]
    return "\n".join(lines)


if __name__ == "__main__":
    main()
