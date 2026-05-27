from __future__ import annotations

__all__ = ["verify_entry_points"]

import tomllib
from importlib.metadata import PackageNotFoundError, distribution, entry_points

import pytest

from diracx.core.extensions import DiracEntryPoint


def get_installed_entry_points():
    """Retrieve the installed entry points from the environment as a flat set."""
    entry_pts = entry_points()
    result = set()
    for group in entry_pts.groups:
        if DiracEntryPoint.CORE in group:
            for ep in entry_pts.select(group=group):
                result.add((group, ep.name, ep.value))
    return result


def get_entry_points_from_toml(toml_file):
    """Parse entry points from pyproject.toml."""
    with open(toml_file, "rb") as f:
        pyproject = tomllib.load(f)
    package_name = pyproject["project"]["name"]
    return package_name, pyproject.get("project", {}).get("entry-points", {})


def get_current_entry_points(repo_base) -> set:
    """Create current entry points set for comparison."""
    result = set()
    toml_globs = [
        repo_base.glob("diracx-*/pyproject.toml"),
        repo_base.glob("extensions/*/*/pyproject.toml"),
    ]
    for toml_file in (f for g in toml_globs for f in g):
        package_name, entry_pts = get_entry_points_from_toml(f"{toml_file}")
        # Ignore packages that are not installed
        try:
            distribution(package_name)
        except PackageNotFoundError:
            continue
        for group, eps in entry_pts.items():
            for name, value in eps.items():
                result.add((group, name, value))
    return result


def _format_entry_points(eps):
    return {f"{g}:{n}={v}" for g, n, v in eps}


@pytest.fixture(scope="session", autouse=True)
def verify_entry_points(request, pytestconfig):
    try:
        ini_toml_name = tomllib.loads(pytestconfig.inipath.read_text())["project"][
            "name"
        ]
    except tomllib.TOMLDecodeError:
        return
    if ini_toml_name == "diracx":
        repo_base = pytestconfig.inipath.parent
    elif ini_toml_name.startswith("diracx-"):
        repo_base = pytestconfig.inipath.parent.parent
    else:
        return

    installed_eps = get_installed_entry_points()
    current_eps = get_current_entry_points(repo_base)

    if installed_eps != current_eps:
        pytest.fail(
            "Project and installed entry-points are not consistent. "
            "You should run `pip install -r requirements-dev.txt` "
            f"installed-only={_format_entry_points(installed_eps - current_eps)} "
            f"project-only={_format_entry_points(current_eps - installed_eps)}",
        )
