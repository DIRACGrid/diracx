from __future__ import annotations

from importlib.metadata import entry_points

import pytest

from diracx.core.extensions import DiracEntryPoint


def test_diracx_resources_entry_point():
    """Test that the diracx.resources entry point is properly configured."""
    resources_eps = entry_points().select(group=DiracEntryPoint.RESOURCES)

    find_platforms_ep = None
    for ep in resources_eps:
        if ep.name == "find_compatible_platforms":
            find_platforms_ep = ep
            break

    assert find_platforms_ep is not None, (
        "find_compatible_platforms entry point not found"
    )
    assert find_platforms_ep.value == "diracx.core.resources:find_compatible_platforms"

    loaded_func = find_platforms_ep.load()
    assert callable(loaded_func)


def _all_diracx_entry_points():
    """Yield (group, entry_point) for every diracx.* entry point."""
    for group in sorted(entry_points().groups):
        if not group.startswith("diracx"):
            continue
        for ep in entry_points().select(group=group):
            yield pytest.param(ep, id=f"{group}:{ep.name}")


@pytest.mark.parametrize("ep", _all_diracx_entry_points())
def test_all_entry_points_loadable(ep):
    """Every registered diracx.* entry point must be importable."""
    ep.load()
