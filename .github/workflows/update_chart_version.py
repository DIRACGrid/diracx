#!/usr/bin/env python3
"""Script to update chart versions based on DiracX releases."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def parse_version(version_str: str) -> tuple[int, int, int, str | None]:
    """Parse a semantic version string into components.

    Args:
        version_str: Version string like "1.0.0-alpha.1"

    Returns:
        Tuple of (major, minor, patch, pre_release_suffix)

    """
    # Remove leading 'v' if present
    version_str = version_str.lstrip("v")

    # Pattern for semver with optional pre-release
    pattern = r"^(\d+)\.(\d+)\.(\d+)(?:-(.+))?$"
    match = re.match(pattern, version_str)

    if not match:
        raise ValueError(f"Invalid version format: {version_str}")

    major, minor, patch, pre_release = match.groups()
    return int(major), int(minor), int(patch), pre_release


def bump_version(current_version: str) -> str:
    """Bump a version automatically - alpha if present, else patch.

    Args:
        current_version: Current version string

    Returns:
        New version string

    """
    major, minor, patch, pre_release = parse_version(current_version)

    if pre_release and pre_release.startswith("alpha."):
        # Increment alpha number
        alpha_match = re.match(r"alpha\.(\d+)", pre_release)
        if alpha_match:
            alpha_num = int(alpha_match.group(1)) + 1
            return f"{major}.{minor}.{patch}-alpha.{alpha_num}"
        else:
            # Invalid alpha format, start with alpha.1
            return f"{major}.{minor}.{patch}-alpha.1"
    else:
        # No alpha suffix, bump patch version
        return f"{major}.{minor}.{patch + 1}"


def update_chart_yaml(
    chart_path: Path, diracx_version: str, new_chart_version: str
) -> None:
    """Update Chart.yaml with new versions."""
    content = chart_path.read_text()

    # Update appVersion
    content = re.sub(
        r"^appVersion:\s*.*$",
        f"appVersion: {diracx_version}",
        content,
        flags=re.MULTILINE,
    )

    # Update version
    content = re.sub(
        r"^version:\s*.*$",
        f'version: "{new_chart_version}"',
        content,
        flags=re.MULTILINE,
    )

    chart_path.write_text(content)


def update_values_yaml(values_path: Path, diracx_version: str) -> None:
    """Update values.yaml with new image tag."""
    content = values_path.read_text()

    # Update only the main services image tag under global.images.tag
    # This pattern looks for the first occurrence of 'tag:' after 'images:' section
    content = re.sub(
        r"(global:\s*[\s\S]*?images:\s*[\s\S]*?tag:\s*).*?(\s)",
        f"\\1{diracx_version}\\2",
        content,
        count=1,
    )

    values_path.write_text(content)


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Update chart versions for DiracX release"
    )
    parser.add_argument(
        "--charts-dir", type=Path, default=".", help="Path to charts directory"
    )
    parser.add_argument(
        "--diracx-version", required=True, help="DiracX version (e.g., v0.0.1a49)"
    )

    args = parser.parse_args()

    # Define paths
    chart_yaml = args.charts_dir / "diracx" / "Chart.yaml"
    values_yaml = args.charts_dir / "diracx" / "values.yaml"

    if not chart_yaml.exists():
        print(f"Error: Chart.yaml not found at {chart_yaml}")
        sys.exit(1)

    if not values_yaml.exists():
        print(f"Error: values.yaml not found at {values_yaml}")
        sys.exit(1)

    # Get current chart version
    chart_content = chart_yaml.read_text()
    version_match = re.search(
        r'^version:\s*["\']?([^"\']+)["\']?$', chart_content, re.MULTILINE
    )

    if not version_match:
        print("Error: Could not find version in Chart.yaml")
        sys.exit(1)

    current_chart_version = version_match.group(1)
    print(f"Current chart version: {current_chart_version}")

    # Calculate new chart version (alpha if present, else patch)
    new_chart_version = bump_version(current_chart_version)
    print(f"New chart version: {new_chart_version}")

    # Update files
    print("Updating Chart.yaml...")
    update_chart_yaml(chart_yaml, args.diracx_version, new_chart_version)

    print("Updating values.yaml...")
    update_values_yaml(values_yaml, args.diracx_version)

    print(f"Successfully updated charts for DiracX {args.diracx_version}")
    print(f"Chart version: {current_chart_version} -> {new_chart_version}")


if __name__ == "__main__":
    main()
