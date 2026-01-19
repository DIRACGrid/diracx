#!/usr/bin/env python3
"""Script to update chart versions based on DiracX releases."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import requests
import yaml


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
    chart_path: Path,
    app_version: str,
    new_chart_version: str,
    dependency_name: str | None = None,
    dependency_version: str | None = None,
) -> None:
    """Update Chart.yaml with new versions.

    Args:
        chart_path: Path to Chart.yaml
        app_version: New application version
        new_chart_version: New chart version
        dependency_name: Optional name of dependency to update
        dependency_version: Optional version for the dependency

    """
    content = chart_path.read_text()

    # Update appVersion
    content = re.sub(
        r"^appVersion:\s*.*$",
        f"appVersion: {app_version}",
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

    # Update dependency version if specified
    if dependency_name and dependency_version:
        # Use YAML to properly update the dependency
        chart_data = yaml.safe_load(content)
        if "dependencies" in chart_data:
            for dep in chart_data["dependencies"]:
                if dep.get("name") == dependency_name:
                    dep["version"] = dependency_version
                    break
        # Use regex for dependency update to preserve formatting
        # Match the dependency block and update the version
        name_pat = re.escape(dependency_name)
        dep_pattern = (
            rf'(- name:\s*{name_pat}\s*\n(?:.*\n)*?\s*version:\s*)("[^"]*"|[^\n]*)'
        )
        content = re.sub(dep_pattern, rf'\g<1>"{dependency_version}"', content)

    chart_path.write_text(content)


def update_values_yaml(
    values_path: Path, app_version: str, image_tag_path: str
) -> None:
    """Update values.yaml with new image tag.

    Args:
        values_path: Path to values.yaml
        app_version: New application version to set as image tag
        image_tag_path: Dot-separated path to the image tag field (e.g., "global.images.tag")

    """
    content = values_path.read_text()
    data = yaml.safe_load(content)

    # Navigate to the parent of the tag field and update it
    path_parts = image_tag_path.split(".")
    current = data
    for part in path_parts[:-1]:
        if part not in current:
            print(f"Warning: Path '{part}' not found in values.yaml")
            return
        current = current[part]

    tag_key = path_parts[-1]
    if tag_key not in current:
        print(f"Warning: Key '{tag_key}' not found at path in values.yaml")
        return

    old_value = current[tag_key]
    current[tag_key] = app_version

    # Use regex to preserve formatting - find and replace the specific value
    # Build a regex pattern that matches the nested structure
    # This is more reliable than re-serializing the entire YAML
    def build_yaml_path_pattern(parts: list[str], value: str) -> tuple[str, str]:
        """Build a regex pattern to find and replace a nested YAML value."""
        # For simple cases, we use a pattern that finds the key and its value
        # at the appropriate nesting level
        key = parts[-1]
        # Match the key followed by its value, being careful about indentation
        pattern = rf"({re.escape(key)}:\s*)({re.escape(str(value))})"
        replacement = rf"\g<1>{app_version}"
        return pattern, replacement

    pattern, replacement = build_yaml_path_pattern(path_parts, str(old_value))
    new_content = re.sub(pattern, replacement, content, count=1)

    if new_content == content:
        # Fallback: if regex didn't match, dump the modified YAML
        print("Warning: Using YAML dump fallback for values.yaml update")
        new_content = yaml.dump(data, default_flow_style=False, sort_keys=False)

    values_path.write_text(new_content)


def lookup_dependency_chart_version(
    index_url: str, dependency_name: str, app_version: str
) -> str | None:
    """Look up the chart version for a dependency given its app version.

    Args:
        index_url: URL to the Helm chart index.yaml
        dependency_name: Name of the dependency chart
        app_version: Application version to match

    Returns:
        The chart version that matches the app version, or None if not found

    """
    print(f"Fetching chart index from {index_url}...")
    response = requests.get(index_url, timeout=30)
    response.raise_for_status()

    index_data = yaml.safe_load(response.text)

    if "entries" not in index_data:
        print("Error: No entries found in chart index")
        return None

    if dependency_name not in index_data["entries"]:
        print(f"Error: Dependency '{dependency_name}' not found in chart index")
        return None

    # Normalize the app version for comparison (remove 'v' prefix if present)
    normalized_app_version = app_version.lstrip("v")

    # Find the chart version with matching appVersion
    for entry in index_data["entries"][dependency_name]:
        entry_app_version = str(entry.get("appVersion", "")).lstrip("v")
        if entry_app_version == normalized_app_version:
            chart_version = entry.get("version")
            print(
                f"Found {dependency_name} chart version {chart_version} "
                f"for app version {app_version}"
            )
            return chart_version

    print(
        f"Warning: No chart found for {dependency_name} with app version {app_version}"
    )
    return None


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Update chart versions for DiracX release"
    )
    parser.add_argument(
        "--charts-dir", type=Path, default=".", help="Path to charts directory"
    )
    parser.add_argument(
        "--chart-subdir",
        type=str,
        default="diracx",
        help="Subdirectory containing chart files (use '.' for root)",
    )
    parser.add_argument(
        "--app-version",
        required=True,
        help="Application version (e.g., v0.0.1a49)",
    )
    parser.add_argument(
        "--image-tag-path",
        type=str,
        default="global.images.tag",
        help="Dot-separated YAML path to image tag in values.yaml",
    )
    parser.add_argument(
        "--update-dependency",
        type=str,
        help="Name of dependency to update in Chart.yaml",
    )
    parser.add_argument(
        "--dependency-app-version",
        type=str,
        help="App version to match for the dependency",
    )
    parser.add_argument(
        "--dependency-chart-index",
        type=str,
        default="https://charts.diracgrid.org/index.yaml",
        help="URL to the dependency's chart index.yaml",
    )

    args = parser.parse_args()

    # Determine chart subdirectory
    if args.chart_subdir == ".":
        chart_subdir = args.charts_dir
    else:
        chart_subdir = args.charts_dir / args.chart_subdir

    # Define paths
    chart_yaml = chart_subdir / "Chart.yaml"
    values_yaml = chart_subdir / "values.yaml"

    if not chart_yaml.exists():
        print(f"Error: Chart.yaml not found at {chart_yaml}")
        sys.exit(1)

    if not values_yaml.exists():
        print(f"Error: values.yaml not found at {values_yaml}")
        sys.exit(1)

    # Get current chart version using YAML parser to handle comments correctly
    chart_content = chart_yaml.read_text()
    chart_data = yaml.safe_load(chart_content)
    version_match = chart_data.get("version")

    if not version_match:
        print("Error: Could not find version in Chart.yaml")
        sys.exit(1)

    current_chart_version = str(version_match)
    print(f"Current chart version: {current_chart_version}")

    # Calculate new chart version (alpha if present, else patch)
    new_chart_version = bump_version(current_chart_version)
    print(f"New chart version: {new_chart_version}")

    # Look up dependency chart version if needed
    dependency_version = None
    if args.update_dependency:
        if not args.dependency_app_version:
            parser.error("--update-dependency requires --dependency-app-version")
        dependency_version = lookup_dependency_chart_version(
            args.dependency_chart_index,
            args.update_dependency,
            args.dependency_app_version,
        )
        if not dependency_version:
            print(
                f"Error: Could not find chart version for {args.update_dependency} "
                f"with app version {args.dependency_app_version}"
            )
            sys.exit(1)

    # Update files
    print("Updating Chart.yaml...")
    update_chart_yaml(
        chart_yaml,
        args.app_version,
        new_chart_version,
        args.update_dependency,
        dependency_version,
    )

    print("Updating values.yaml...")
    update_values_yaml(values_yaml, args.app_version, args.image_tag_path)

    print(f"Successfully updated charts for app version {args.app_version}")
    print(f"Chart version: {current_chart_version} -> {new_chart_version}")
    if dependency_version:
        print(f"Dependency {args.update_dependency} version: {dependency_version}")


if __name__ == "__main__":
    main()
