#!/usr/bin/env python3
"""Enforce the diracx ``__all__`` convention.

Two modes are supported, configured from ``.pre-commit-config.yaml``:

- ``--mode=require-list`` — the file MUST declare ``__all__`` and it MUST be a
  list literal. Used for package and subpackage ``__init__.py`` files and for
  top-level modules of a (sub)package, where ``__all__`` defines the public API.

- ``--mode=forbid`` — the file MUST NOT declare ``__all__``. Used for submodules
  of a subpackage: per convention they expose their names through the
  subpackage ``__init__.py`` instead. See
  ``docs/dev/reference/coding-conventions.md``.

Only top-level statements are inspected (anything nested inside ``if
TYPE_CHECKING:``, ``try/except``, function bodies, etc. is ignored — keep
``__all__`` at module scope). A re-export form (``from foo import __all__``)
counts as a declaration for both modes.
"""

from __future__ import annotations

import argparse
import ast


def find_all_node(tree: ast.Module) -> ast.stmt | None:
    """Return the first top-level statement that declares or re-exports ``__all__``."""
    for node in tree.body:
        match node:
            case ast.Assign(targets=targets):
                if any(isinstance(t, ast.Name) and t.id == "__all__" for t in targets):
                    return node
            case ast.AugAssign(target=ast.Name(id="__all__")):
                return node
            case ast.AnnAssign(target=ast.Name(id="__all__")):
                return node
            case ast.ImportFrom(names=names):
                if any(alias.name == "__all__" for alias in names):
                    return node
    return None


def is_list_value(node: ast.stmt) -> bool:
    """Return True if ``node`` is an ``__all__`` declaration whose value is a list literal.

    The re-export form (``from foo import __all__``) carries no value of its
    own and is treated as satisfying the list requirement — the upstream module
    is responsible for using a list.
    """
    if isinstance(node, ast.ImportFrom):
        return True
    return isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)) and isinstance(
        node.value, ast.List
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("require-list", "forbid"),
        required=True,
    )
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()

    failures: list[str] = []
    for path in args.files:
        with open(path, encoding="utf-8") as f:
            tree = ast.parse(f.read())
        node = find_all_node(tree)

        if args.mode == "require-list":
            if node is None:
                failures.append(f"{path}: __all__ not found")
            elif not is_list_value(node):
                failures.append(f"{path}: __all__ is not a list")
        else:
            if node is not None:
                failures.append(
                    f"{path}: __all__ must not be defined in a subpackage "
                    "submodule — expose names from the parent __init__.py "
                    "instead (see docs/dev/reference/coding-conventions.md)"
                )

    for msg in failures:
        print(f"- {msg}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
