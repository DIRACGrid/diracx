from __future__ import annotations

import pytest
from DIRACCommon.Core.Utilities.ReturnValues import SErrorException

from diracx.core.config import Config
from diracx.core.resources import find_compatible_platforms


def _make_test_config(os_compatibility: dict[str, set[str]]) -> Config:
    return Config.model_validate(
        {
            "DIRAC": {},
            "Registry": {},
            "Operations": {},
            "Resources": {"Computing": {"OSCompatibility": os_compatibility}},
        }
    )


def test_find_compatible_platforms_default_implementation():
    """Test the default implementation of find_compatible_platforms."""
    # Create a test config with OS compatibility mapping
    test_config = _make_test_config(
        {
            "slc7": {"slc7", "centos7"},
            "centos7": {"centos7"},
            "ubuntu20": {"ubuntu20", "ubuntu18"},
            "rhel8": {"rhel8", "centos8"},
            "debian10": {"ubuntu20"},
        }
    )
    result = find_compatible_platforms(["slc7", "ubuntu20"], test_config)
    assert result == ["ubuntu20", "slc7", "debian10"]


def test_find_compatible_platforms_empty_platforms():
    """Test find_compatible_platforms with empty job platforms list."""
    test_config = _make_test_config({"slc7": {"slc7", "centos7"}})
    with pytest.raises(SErrorException, match="No compatible DIRAC platform found"):
        find_compatible_platforms([], test_config)


def test_find_compatible_platforms_empty_compatibility():
    """Test find_compatible_platforms with empty OS compatibility mapping."""
    test_config = _make_test_config({})
    with pytest.raises(SErrorException, match="OS compatibility info not found"):
        find_compatible_platforms(["slc7", "ubuntu20"], test_config)


def test_find_compatible_platforms_with_extension(monkeypatch):
    """Test find_compatible_platforms when an extension is available."""

    def extension_function(job_platforms: list[str], config: Config) -> list[str]:
        return ["extension_result"]

    test_config = _make_test_config({"slc7": {"slc7", "centos7"}})

    job_platforms = ["slc7"]

    def mock_select_from_extension(group: str, name: str):
        # Mock to return extension first, then original
        class MockEntryPoint:
            def __init__(self, value, load_func):
                self.value = value
                self._load_func = load_func

            def load(self):
                return self._load_func

        mock_extension_ep = MockEntryPoint(
            "extension.module:extension_function", extension_function
        )
        mock_original_ep = MockEntryPoint(
            "diracx.core.resources:find_compatible_platforms", find_compatible_platforms
        )
        return [mock_extension_ep, mock_original_ep]

    monkeypatch.setattr(
        "diracx.core.extensions.select_from_extension", mock_select_from_extension
    )

    # Import the function again to get the decorated version
    from importlib import reload

    import diracx.core.resources

    reload(diracx.core.resources)

    result = diracx.core.resources.find_compatible_platforms(job_platforms, test_config)

    # Should use the extension function
    assert result == ["extension_result"]
