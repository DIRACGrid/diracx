"""
This test makes sure that we are getting the configuration as a GubbinsConfig
"""

import datetime
from urllib import request

import pytest
from diracx.core.config import ConfigSource, RemoteGitConfigSource

from gubbins.core.config.schema import Config

# The diracx-chart contains a CS example
TEST_REPO = "git+https://github.com/DIRACGrid/diracx-charts/"


def github_is_down():
    try:
        request.urlopen("https://github.com", timeout=1)
        return False
    except Exception:
        return True


@pytest.mark.skipif(github_is_down(), reason="Github unavailble")
def test_remote_git_config_source(monkeypatch):
    monkeypatch.setattr(
        "diracx.core.config.sources.DEFAULT_CONFIG_FILE",
        "k3s/examples/cs.yaml",
    )
    remote_conf = ConfigSource.create_from_url(backend_url=TEST_REPO)
    assert isinstance(remote_conf, RemoteGitConfigSource)

    hexsha, modified = remote_conf.latest_revision()
    assert isinstance(hexsha, str)
    assert isinstance(modified, datetime.datetime)
    result = remote_conf.read_raw(hexsha, modified)
    assert isinstance(result, Config)


def test_with_config_repo_uses_gubbins_config(with_config_repo):
    """Verify with_config_repo fixture uses gubbins Config, not base Config.

    The with_config_repo fixture must use select_from_extension() to discover
    the Config class, so extensions like gubbins can add fields to the schema.

    Before the fix (DIRACGrid/diracx#727), the fixture directly imported Config
    from diracx.core.config, bypassing the extension system. This caused the
    fixture to serialize config using base UserConfig (without GubbinsSpecificInfo),
    so the JSON file wouldn't contain gubbins-specific fields.

    This test reads the raw JSON to verify it contains GubbinsSpecificInfo,
    which proves the fixture used gubbins Config for serialization.
    """
    import json

    # Read the raw JSON file created by the fixture
    config_file = with_config_repo / "default.yml"
    raw_config = json.loads(config_file.read_text())

    # The JSON should contain GubbinsSpecificInfo in user configs
    # This field only exists if gubbins Config was used to serialize
    for vo_config in raw_config["Registry"].values():
        for user_config in vo_config["Users"].values():
            assert "GubbinsSpecificInfo" in user_config, (
                "GubbinsSpecificInfo not found in JSON - fixture may not be using "
                "extension-aware Config discovery"
            )
