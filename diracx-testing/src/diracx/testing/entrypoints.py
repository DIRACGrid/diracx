from __future__ import annotations

import tomllib
from collections import defaultdict
from importlib.metadata import PackageNotFoundError, distribution, entry_points

import pytest


def get_installed_entry_points():
    """Retrieve the installed entry points from the environment."""
    entry_pts = entry_points()
    diracx_eps = defaultdict(dict)
    for group in entry_pts.groups:
        if "diracx" in group:
            for ep in entry_pts.select(group=group):
                diracx_eps[group][ep.name] = ep.value
    return dict(diracx_eps)


def get_entry_points_from_toml(toml_file):
    """Parse entry points from pyproject.toml."""
    with open(toml_file, "rb") as f:
        pyproject = tomllib.load(f)
    package_name = pyproject["project"]["name"]
    return package_name, pyproject.get("project", {}).get("entry-points", {})


def get_current_entry_points(repo_base) -> bool:
    """Create current entry points dict for comparison."""
    current_eps = {}
    for toml_file in repo_base.glob("diracx-*/pyproject.toml"):
        package_name, entry_pts = get_entry_points_from_toml(f"{toml_file}")
        # Ignore packages that are not installed
        try:
            distribution(package_name)
        except PackageNotFoundError:
            continue
        # Merge the entry points
        for key, value in entry_pts.items():
            current_eps[key] = current_eps.get(key, {}) | value
    return current_eps


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

    installed_eps = set(get_installed_entry_points())
    current_eps = set(get_current_entry_points(repo_base))

    if installed_eps != current_eps:
        pytest.fail(
            "Project and installed entry-points are not consistent. "
            "You should run `pip install -r requirements-dev.txt`"
            f"{installed_eps-current_eps=}",
            f"{current_eps-installed_eps=}",
        )
