"""Pre-commit hook for checking __all__ dunder in __init__.py files."""

from __future__ import annotations

import re
from glob import glob


def main():
    """Check if the __all__ dunder exists and it's a list."""
    check_patterns = [
        "extensions/gubbins/gubbins-*/**/__init__.py",
        "diracx-*/**/__init__.py",
    ]
    exclude_patterns = ["_generated"]

    files = []

    for pattern in check_patterns:
        files.extend(glob(pattern, recursive=True))

    for pattern in exclude_patterns:
        for file in files[:]:
            if pattern in file:
                files.remove(file)

    files_without_all = []
    files_incorrect_format_all = []

    for file in files:
        with open(file, "r") as f:
            content = f.read()

        # __all__ dunder exists
        if not re.search("__all__ =", content):
            files_without_all.append(file)

        # If exists, make sure its a list
        elif not re.search("__all__ =[\S\s]*\[", content):
            files_incorrect_format_all.append(file)

    ret_val = 0

    if files_without_all:
        ret_val = 1
        print("> Files without __all__ defined")
        for filename in files_without_all:
            print(f"\t- {filename}")

    if files_incorrect_format_all:
        ret_val = 1
        print("> Files with __all__ not defined as a list")
        for filename in files_incorrect_format_all:
            print(f"\t- {filename}")

    return ret_val


if __name__ == "__main__":
    raise SystemExit(main())
