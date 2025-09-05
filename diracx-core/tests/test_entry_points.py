from __future__ import annotations

from importlib.metadata import entry_points


def test_diracx_resources_entry_point():
    """Test that the diracx.resources entry point is properly configured."""
    # Get all entry points for the diracx.resources group
    resources_eps = entry_points().select(group="diracx.resources")

    # Check that find_compatible_platforms entry point exists
    find_platforms_ep = None
    for ep in resources_eps:
        if ep.name == "find_compatible_platforms":
            find_platforms_ep = ep
            break

    assert find_platforms_ep is not None, (
        "find_compatible_platforms entry point not found"
    )
    assert find_platforms_ep.value == "diracx.core.resources:find_compatible_platforms"

    # Test that the entry point can be loaded
    loaded_func = find_platforms_ep.load()
    # Check that it's callable
    assert callable(loaded_func)


def test_entry_point_functionality():
    """Test that the entry point points to the correct function."""
    # Get the entry point
    resources_eps = entry_points().select(group="diracx.resources")
    find_platforms_ep = None
    for ep in resources_eps:
        if ep.name == "find_compatible_platforms":
            find_platforms_ep = ep
            break

    assert find_platforms_ep is not None

    # Load the function from the entry point
    loaded_func = find_platforms_ep.load()

    # Verify it's callable
    assert callable(loaded_func)

    # Verify the function has the expected signature and behavior
    from diracx.core.config import Config

    test_config = Config.model_validate(
        {
            "DIRAC": {},
            "Registry": {},
            "Operations": {},
            "Resources": {
                "Computing": {
                    "OSCompatibility": {
                        "slc7": {"slc7", "centos7"},
                    }
                }
            },
        }
    )

    job_platforms = ["slc7"]

    # Test that the function can be called and returns a list
    result = loaded_func(job_platforms, test_config)

    # Verify it returns a list (the exact content may vary due to extensions)
    assert isinstance(result, list)
