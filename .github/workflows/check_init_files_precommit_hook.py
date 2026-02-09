"""Pre-commit hook for checking __all__ dunder in __init__.py files."""

from __future__ import annotations

import ast
import sys


def main():
    files_without_all = []
    files_not_list_all = []

    for file in sys.argv[1:]:
        with open(file, "r", encoding="utf-8") as f:
            content = f.read()

        module_tree = ast.parse(content)
        found = False

        # Go through the module
        for node in module_tree.body:
            # Has to be __all__ = {something} or __all__ += {something}
            if isinstance(node, ast.Assign) or isinstance(node, ast.AugAssign):
                target = node.targets[0]
                # The target is the __all__ dunder
                if isinstance(target, ast.Name) and target.id == "__all__":
                    found = True
                    # __all__ is a list
                    if not isinstance(node.value, ast.List):
                        files_not_list_all.append(file)

                    break

        if not found:
            files_without_all.append(file)

    if files_without_all == [] and files_not_list_all == []:
        return 0

    for file in files_without_all:
        print(f"- {file}: __all__ not found")

    for file in files_not_list_all:
        print(f"- {file}: __all__ is not a list")

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
