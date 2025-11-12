#!/usr/bin/env python
"""Generate documentation for all available DiracX entry points.

This script discovers all entry points defined across DiracX and its extensions,
providing comprehensive documentation for extension developers.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tomllib
from collections import defaultdict
from pathlib import Path
from typing import Any


def get_entry_points_from_toml(
    toml_file: Path,
) -> tuple[str, dict[str, dict[str, str]]]:
    """Parse entry points from pyproject.toml.

    Args:
        toml_file: Path to pyproject.toml file

    Returns:
        Tuple of (package_name, entry_points_dict)

    """
    with open(toml_file, "rb") as f:
        pyproject = tomllib.load(f)
    package_name = pyproject["project"]["name"]
    entry_points = pyproject.get("project", {}).get("entry-points", {})
    return package_name, entry_points


def discover_entry_points(repo_base: Path) -> dict[str, dict[str, dict[str, str]]]:
    """Discover all entry points in the repository.

    Args:
        repo_base: Base directory of the repository

    Returns:
        Nested dict: {entry_point_group: {package_name: {entry_name: entry_value}}}

    """
    all_entry_points: dict[str, dict[str, dict[str, str]]] = defaultdict(
        lambda: defaultdict(dict)
    )

    # Search for all pyproject.toml files in diracx-* and extensions/*/
    patterns = [
        "diracx-*/pyproject.toml",
        "extensions/*/pyproject.toml",
        "extensions/*/*/pyproject.toml",
    ]

    for pattern in patterns:
        for toml_file in repo_base.glob(pattern):
            try:
                package_name, entry_points = get_entry_points_from_toml(toml_file)

                # Only include diracx-related entry points
                for group, entries in entry_points.items():
                    if group.startswith("diracx"):
                        all_entry_points[group][package_name] = entries
            except Exception as e:
                print(f"Warning: Could not parse {toml_file}: {e}", file=sys.stderr)

    return dict(all_entry_points)


def get_entry_point_description(group: str) -> dict[str, Any]:
    """Get description and metadata for an entry point group.

    Args:
        group: Entry point group name

    Returns:
        Dict with title, description, usage_example, and notes

    """
    descriptions = {
        "diracx": {
            "title": "Core Extension Registration",
            "description": (
                "The base entry point group for registering DiracX extensions. "
                "Extensions MUST register themselves here."
            ),
            "keys": {
                "extension": "Extension name (required for all extensions)",
                "properties_module": "Module path to custom DIRAC properties",
                "config": "Path to extended configuration schema class",
            },
            "usage_example": """
```toml
[project.entry-points."diracx"]
extension = "myextension"
properties_module = "myextension.core.properties"
config = "myextension.core.config.schema:Config"
```
""",
            "notes": [
                "The `extension` key is **required** for all extensions",
                "Extensions are prioritized by name (alphabetically, with 'diracx' last)",
                "Only one extension can be installed alongside DiracX core",
            ],
        },
        "diracx.services": {
            "title": "FastAPI Router Registration",
            "description": (
                "Register FastAPI routers to create new API endpoints or override existing ones. "
                "Each entry creates a route under `/api/<system-name>/`."
            ),
            "keys": {
                "<system-name>": "Path to DiracxRouter instance (e.g., 'myext.routers.jobs:router')",
            },
            "usage_example": """
```toml
[project.entry-points."diracx.services"]
myjobs = "myextension.routers.jobs:router"
".well-known" = "myextension.routers.well_known:router"  # Special case: served at root
```
""",
            "notes": [
                "Routers can be disabled with `DIRACX_SERVICE_<SYSTEM_NAME>_ENABLED=false`",
                "Extensions can override core routers by using the same name",
                "All routes must have proper access policies or use `@open_access`",
                "The system name becomes the first tag in OpenAPI spec",
            ],
        },
        "diracx.dbs.sql": {
            "title": "SQL Database Registration",
            "description": (
                "Register SQL database classes using SQLAlchemy. "
                "Database URLs are configured via `DIRACX_DB_URL_<DB_NAME>` environment variables."
            ),
            "keys": {
                "<db-name>": "Path to BaseSQLDB subclass (e.g., 'myext.db.sql.jobs:JobDB')",
            },
            "usage_example": """
```toml
[project.entry-points."diracx.dbs.sql"]
JobDB = "myextension.db.sql.jobs:ExtendedJobDB"
MyCustomDB = "myextension.db.sql.custom:MyCustomDB"
```
""",
            "notes": [
                "Database classes must inherit from `BaseSQLDB`",
                "Use `@declared_attr` for tables to support extension inheritance",
                "Transactions are auto-managed: commit on success, rollback on errors",
                "Connection pooling is automatic via SQLAlchemy",
            ],
        },
        "diracx.dbs.os": {
            "title": "OpenSearch Database Registration",
            "description": (
                "Register OpenSearch/Elasticsearch database classes for log and parameter storage. "
                "Connection parameters configured via `DIRACX_OS_DB_<DB_NAME>_*` environment variables."
            ),
            "keys": {
                "<db-name>": "Path to BaseOSDB subclass (e.g., 'myext.db.os.jobs:JobParametersDB')",
            },
            "usage_example": """
```toml
[project.entry-points."diracx.dbs.os"]
JobParametersDB = "myextension.db.os.jobs:ExtendedJobParametersDB"
```
""",
            "notes": [
                "Database classes must inherit from `BaseOSDB`",
                "No automatic transaction management (unlike SQL databases)",
                "Connection pooling is handled by AsyncOpenSearch client",
            ],
        },
        "diracx.cli": {
            "title": "CLI Command Registration",
            "description": (
                "Register Typer applications as subcommands of the `dirac` CLI. "
                "Extensions can add new subcommands or extend existing ones."
            ),
            "keys": {
                "<command-name>": "Path to Typer app (e.g., 'myext.cli.jobs:app')",
            },
            "usage_example": """
```toml
[project.entry-points."diracx.cli"]
jobs = "myextension.cli.jobs:app"  # Override core 'dirac jobs' command
mycmd = "myextension.cli.custom:app"  # Add 'dirac mycmd' command
```
""",
            "notes": [
                "Commands are automatically integrated into the main `dirac` CLI",
                "Extensions can completely replace core commands by using the same name",
                "Use `@app.async_command()` for async operations",
                "Follows standard Typer patterns for argument/option parsing",
            ],
        },
        "diracx.cli.hidden": {
            "title": "Hidden CLI Commands",
            "description": (
                "Register CLI commands that should not appear in help text. "
                "Used for internal/debugging commands."
            ),
            "keys": {
                "<command-name>": "Path to Typer app for hidden command",
            },
            "usage_example": """
```toml
[project.entry-points."diracx.cli.hidden"]
internal = "myextension.cli.internal:app"
debug = "myextension.cli.debug:app"
```
""",
            "notes": [
                "Commands are functional but don't appear in `dirac --help`",
                "Useful for debugging tools and internal utilities",
            ],
        },
        "diracx.access_policies": {
            "title": "Access Policy Registration",
            "description": (
                "Register custom access policies for fine-grained authorization control. "
                "Policies can inject claims into tokens and check permissions at runtime."
            ),
            "keys": {
                "<PolicyName>": "Path to BaseAccessPolicy subclass",
            },
            "usage_example": """
```toml
[project.entry-points."diracx.access_policies"]
WMSAccessPolicy = "myextension.routers.jobs.access_policy:WMSAccessPolicy"
CustomPolicy = "myextension.routers.custom.policy:CustomAccessPolicy"
```
""",
            "notes": [
                "Policies must inherit from `BaseAccessPolicy`",
                "Each route must call its policy or use `@open_access` decorator",
                "Policies can inject data during token generation via `policy_name` claim",
                "CI test `test_all_routes_have_policy` enforces policy usage",
            ],
        },
        "diracx.min_client_version": {
            "title": "Minimum Client Version Declaration",
            "description": (
                "Declare the minimum compatible client version for the server. "
                "Used to prevent compatibility issues between client and server."
            ),
            "keys": {
                "diracx": "Variable name containing version string (e.g., 'myext.routers:MIN_VERSION')",
            },
            "usage_example": """
```toml
[project.entry-points."diracx.min_client_version"]
myextension = "myextension.routers:MYEXT_MIN_CLIENT_VERSION"
```
""",
            "notes": [
                "Extensions take priority over 'diracx' entry point",
                "Version string should follow semantic versioning",
                "Server rejects requests from clients below minimum version",
            ],
        },
        "diracx.resources": {
            "title": "Resource Management Functions",
            "description": (
                "Register functions that can be overridden by extensions to customize "
                "resource management behavior (e.g., platform compatibility)."
            ),
            "keys": {
                "find_compatible_platforms": "Function to determine platform compatibility",
            },
            "usage_example": """
```toml
[project.entry-points."diracx.resources"]
find_compatible_platforms = "myext.core.resources:find_compatible_platforms"
```
""",
            "notes": [
                "Uses `@supports_extending` decorator pattern",
                "Extension implementations automatically override core functions",
                "Useful for site-specific resource matching logic",
            ],
        },
    }

    return descriptions.get(
        group,
        {
            "title": f"Entry Point Group: {group}",
            "description": "Custom entry point group (not documented in core DiracX).",
            "keys": {},
            "usage_example": "",
            "notes": [],
        },
    )


def generate_markdown(entry_points: dict[str, dict[str, dict[str, str]]]) -> str:
    """Generate markdown documentation for entry points.

    Args:
        entry_points: Discovered entry points nested dict

    Returns:
        Markdown formatted documentation

    """
    output = ["# DiracX Entry Points Reference\n"]
    output.append(
        "This document catalogs all available entry points for creating DiracX extensions.\n"
    )
    output.append(
        "Entry points are defined in `pyproject.toml` files and discovered at runtime.\n"
    )

    # Generate table of contents
    output.append("## Table of Contents\n")
    sorted_groups = sorted(entry_points.keys())
    for group in sorted_groups:
        metadata = get_entry_point_description(group)
        anchor = group.replace(".", "").replace("_", "-").lower()
        output.append(f"- [{metadata['title']}](#{anchor})\n")

    # Generate detailed sections
    for group in sorted_groups:
        packages = entry_points[group]
        metadata = get_entry_point_description(group)

        output.append(f"\n## {metadata['title']}\n")
        output.append(f"**Entry Point Group**: `{group}`\n\n")
        output.append(f"{metadata['description']}\n")

        # Keys/entries documentation
        if metadata["keys"]:
            output.append("\n### Entry Point Keys\n\n")
            for key, desc in metadata["keys"].items():
                output.append(f"- **`{key}`**: {desc}\n")

        # Usage example
        if metadata["usage_example"]:
            output.append("\n### Usage Example\n")
            output.append(metadata["usage_example"])

        # Notes
        if metadata["notes"]:
            output.append("\n### Important Notes\n\n")
            for note in metadata["notes"]:
                output.append(f"- {note}\n")

        # Current implementations
        output.append("\n### Current Implementations\n\n")
        if not packages:
            output.append("*No implementations found in the repository.*\n")
        else:
            output.append("| Package | Entry Name | Entry Point |\n")
            output.append("|---------|------------|-------------|\n")
            for package in sorted(packages.keys()):
                entries = packages[package]
                for entry_name, entry_value in sorted(entries.items()):
                    # Escape pipe characters in entry values
                    safe_value = entry_value.replace("|", "\\|")
                    output.append(
                        f"| `{package}` | `{entry_name}` | `{safe_value}` |\n"
                    )

    # Footer
    output.append("\n---\n\n")
    output.append(
        "*This documentation is auto-generated. "
        "See `scripts/generate_entrypoints_docs.py` for details.*\n"
    )

    return "".join(output)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Generate documentation for DiracX entry points"
    )
    parser.add_argument(
        "--repo-base",
        type=Path,
        default=Path(__file__).parent.parent,
        help="Base directory of the DiracX repository",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file path (default: docs/dev/reference/entrypoints.md)",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print to stdout instead of writing to file",
    )

    args = parser.parse_args()

    # Discover entry points
    print(f"Discovering entry points in {args.repo_base}...", file=sys.stderr)
    entry_points = discover_entry_points(args.repo_base)

    print(f"Found {len(entry_points)} entry point groups:", file=sys.stderr)
    for group, packages in sorted(entry_points.items()):
        total_entries = sum(len(entries) for entries in packages.values())
        print(f"  - {group}: {total_entries} entries", file=sys.stderr)

    # Generate markdown
    markdown = generate_markdown(entry_points)

    # Output
    if args.stdout:
        print(markdown)
    else:
        output_path = (
            args.output
            or args.repo_base / "docs" / "dev" / "reference" / "entrypoints.md"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown)
        print(f"\nDocumentation written to: {output_path}", file=sys.stderr)

        # Format the generated markdown file with mdformat
        print("Formatting with mdformat...", file=sys.stderr)
        try:
            subprocess.run(  # noqa: S603
                ["mdformat", "--number", str(output_path.absolute())],  # noqa: S607
                check=True,
                capture_output=True,
                text=True,
            )
            print("✓ Markdown formatted successfully", file=sys.stderr)
        except subprocess.CalledProcessError as e:
            print(f"Warning: mdformat failed: {e.stderr}", file=sys.stderr)
        except FileNotFoundError:
            print(
                "Warning: mdformat not found.",
                file=sys.stderr,
            )


if __name__ == "__main__":
    main()
