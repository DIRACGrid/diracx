#!/usr/bin/env python3
"""Check that all Python modules have corresponding API reference documentation pages.

This script ensures that the API reference documentation stays in sync with the codebase
by checking that every Python module has a corresponding .md file in docs/dev/reference/api/.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import NamedTuple

# Root directory of the diracx project
ROOT_DIR = Path(__file__).parent.parent.parent.parent.parent
DOCS_API_DIR = Path(__file__).parent

# Package directories to check
PACKAGES = {
    "diracx-core": ROOT_DIR / "diracx-core" / "src" / "diracx" / "core",
    "diracx-routers": ROOT_DIR / "diracx-routers" / "src" / "diracx" / "routers",
    "diracx-logic": ROOT_DIR / "diracx-logic" / "src" / "diracx" / "logic",
    "diracx-db": ROOT_DIR / "diracx-db" / "src" / "diracx" / "db",
    "diracx-cli": ROOT_DIR / "diracx-cli" / "src" / "diracx" / "cli",
}

# Modules to ignore (typically __pycache__, __init__.py, py.typed, etc.)
IGNORED_PATTERNS = {
    "__pycache__",
    "__init__.py",
    "__main__.py",
    "py.typed",
    ".pyc",
}


class ModuleInfo(NamedTuple):
    """Information about a Python module."""

    package: str
    module_path: str  # e.g., "routers.jobs.submission"
    file_path: Path


def find_python_modules(package_name: str, package_path: Path) -> list[ModuleInfo]:
    """Find all Python modules in a package.

    Args:
        package_name: Name of the package (e.g., "diracx-routers")
        package_path: Path to the package source directory

    Returns:
        List of ModuleInfo objects for each Python module

    """
    modules: list[ModuleInfo] = []

    if not package_path.exists():
        print(f"Warning: Package path does not exist: {package_path}")
        return modules

    # Find all .py files
    for py_file in package_path.rglob("*.py"):
        # Skip ignored files
        if any(pattern in str(py_file) for pattern in IGNORED_PATTERNS):
            continue

        # Get relative path from package root
        rel_path = py_file.relative_to(package_path)

        # Convert path to module notation (e.g., jobs/submission.py -> jobs.submission)
        module_parts = list(rel_path.parts[:-1]) + [rel_path.stem]
        module_path = ".".join(module_parts)

        modules.append(
            ModuleInfo(
                package=package_name,
                module_path=module_path,
                file_path=py_file,
            )
        )

    return sorted(modules, key=lambda m: m.module_path)


def get_expected_doc_path(module: ModuleInfo) -> Path | None:
    """Get the expected documentation file path for a module.

    Args:
        module: ModuleInfo object

    Returns:
        Expected path to the .md documentation file

    """
    # Map package names to doc sections
    section_map = {
        "diracx-core": "core",
        "diracx-routers": "routers",
        "diracx-logic": "logic",
        "diracx-db": "db",
        "diracx-cli": "cli",
    }

    section = section_map.get(module.package)
    if not section:
        return None

    # Convert module path to doc path
    # e.g., jobs.submission -> jobs.md (we document at package level, not individual files)
    # Or for deeper nesting: sql.job.db -> job.md
    parts = module.module_path.split(".")

    # For most cases, we document at the first or second level
    # This is a heuristic - adjust based on your documentation structure
    if section == "db":
        # Special handling for db: sql.job.db -> job.md, os.job_parameters -> opensearch.md
        if parts[0] == "sql":
            doc_name = parts[1] if len(parts) > 1 else "index"
        elif parts[0] == "os":
            doc_name = "opensearch"
        elif parts[0] == "exceptions":
            doc_name = "exceptions"
        else:
            doc_name = parts[0]
    elif section in ["routers", "logic"]:
        # routers.jobs.submission -> jobs.md
        # logic.jobs.submission -> jobs.md
        doc_name = parts[0] if parts else "index"
    elif section == "core":
        # core.models -> models.md
        doc_name = parts[0] if parts else "index"
    elif section == "cli":
        # cli.jobs -> index.md (CLI is all in one file currently)
        doc_name = "index"
    else:
        doc_name = parts[0] if parts else "index"

    doc_path = DOCS_API_DIR / section / f"{doc_name}.md"
    return doc_path


def check_module_documented(module: ModuleInfo) -> tuple[bool, Path | None]:
    """Check if a module is documented.

    Args:
        module: ModuleInfo object

    Returns:
        Tuple of (is_documented, expected_doc_path)

    """
    expected_path = get_expected_doc_path(module)
    if expected_path is None:
        return False, None

    # Check if the doc file exists
    exists = expected_path.exists()

    # If it exists, check if it references this module
    if exists:
        content = expected_path.read_text()
        # Check for the module reference in the doc (loose check)
        # This could be improved to parse the ::: directives more carefully
        referenced = (
            f"diracx.{module.package.split('-')[1]}.{module.module_path}" in content
        )
        return referenced, expected_path

    return False, expected_path


def main():
    """Main function to check documentation coverage."""
    print("Checking API reference documentation coverage...\n")

    all_modules = []
    undocumented = []
    documented = []

    # Collect all modules
    for package_name, package_path in PACKAGES.items():
        modules = find_python_modules(package_name, package_path)
        all_modules.extend(modules)
        print(f"Found {len(modules)} modules in {package_name}")

    print(f"\nTotal modules found: {len(all_modules)}\n")

    # Check documentation coverage
    for module in all_modules:
        is_documented, doc_path = check_module_documented(module)

        if is_documented:
            documented.append((module, doc_path))
        else:
            undocumented.append((module, doc_path))

    # Print results
    print(f"✓ Documented modules: {len(documented)}")
    print(f"✗ Undocumented modules: {len(undocumented)}\n")

    if undocumented:
        print("Undocumented modules:")
        print("-" * 80)

        # Group by package
        by_package = {}
        for module, doc_path in undocumented:
            by_package.setdefault(module.package, []).append((module, doc_path))

        for package_name, modules in sorted(by_package.items()):
            print(f"\n{package_name}:")
            for module, doc_path in modules:
                if doc_path and doc_path.exists():
                    status = f"Doc exists but missing reference: {doc_path}"
                elif doc_path:
                    status = f"Missing doc file: {doc_path}"
                else:
                    status = "No doc path determined"
                print(f"  - {module.module_path:40} → {status}")

        print("\n" + "=" * 80)
        print(
            f"Documentation coverage: {len(documented)}/{len(all_modules)} "
            f"({len(documented) * 100 / len(all_modules):.1f}%)"
        )
        print("=" * 80)

        # Return non-zero exit code if there are undocumented modules
        sys.exit(1)
    else:
        print("✓ All modules are documented!")
        print("=" * 80)
        print(f"Documentation coverage: 100% ({len(all_modules)} modules)")
        print("=" * 80)


if __name__ == "__main__":
    main()
