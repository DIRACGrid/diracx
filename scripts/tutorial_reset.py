#!/usr/bin/env python3
"""Strip tutorial code from the repository.

Removes snippet-marked sections from shared files and deletes
files that are entirely tutorial code, giving the reader a clean
starting point to follow the advanced tutorial.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Files/directories that are entirely tutorial code — deleted wholesale
TUTORIAL_PATHS = [
    "extensions/gubbins/gubbins-db/src/gubbins/db/sql/my_pilot_db",
    "extensions/gubbins/gubbins-db/tests/test_my_pilot_db.py",
    "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilots.py",
    "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilot_lock_types.py",
    "extensions/gubbins/gubbins-tasks/tests/test_my_pilot_tasks.py",
    "extensions/gubbins/gubbins-routers/src/gubbins/routers/my_pilots.py",
    "extensions/gubbins/gubbins-routers/tests/test_my_pilots.py",
]

# Regex: match start marker through end marker (inclusive), for any
# section whose name begins with "my_pilots".
MARKER_PATTERN = re.compile(
    r"^[^\n]*--8<--\s*\[start:my_pilots[^\]]*\].*\n"
    r"(?:.*\n)*?"
    r"[^\n]*--8<--\s*\[end:my_pilots[^\]]*\].*\n",
    re.MULTILINE,
)


def strip_markers(path: Path) -> bool:
    """Remove snippet-marked tutorial sections from *path*.

    Returns True if the file was modified.
    """
    text = path.read_text()
    new_text = MARKER_PATTERN.sub("", text)
    if new_text != text:
        path.write_text(new_text)
        return True
    return False


def main() -> None:
    # 1. Delete tutorial-only files and directories
    for rel in TUTORIAL_PATHS:
        p = REPO_ROOT / rel
        if p.is_dir():
            shutil.rmtree(p)
            print(f"  deleted directory: {rel}")
        elif p.is_file():
            p.unlink()
            print(f"  deleted file: {rel}")

    # 2. Strip markers from shared files
    search_dirs = [
        REPO_ROOT / "extensions" / "gubbins",
        REPO_ROOT / "extensions" / "gubbins-charts",
    ]
    for ext in ("*.py", "*.toml", "*.yaml"):
        for search_dir in search_dirs:
            for path in search_dir.rglob(ext):
                if ".pixi" in str(path):
                    continue
                if strip_markers(path):
                    print(f"  stripped markers: {path.relative_to(REPO_ROOT)}")

    print("\nTutorial code removed.  Run 'pixi install' to refresh.")


if __name__ == "__main__":
    main()
