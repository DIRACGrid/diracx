from __future__ import annotations

import datetime
from urllib import request

import pytest

from diracx.core.config import ConfigSource, RemoteGitConfigSource
from diracx.core.config.schema import Config

# The diracx-chart contains a CS example
TEST_REPO = "git+https://github.com/DIRACGrid/diracx-charts.git"
TEST_REPO_SPECIFIC_BRANCH = TEST_REPO + "?branch=master"


def github_is_down():
    try:

        request.urlopen("https://github.com", timeout=1)
        return False
    except Exception:
        return True


@pytest.mark.skipif(github_is_down(), reason="Github unavailble")
@pytest.mark.parametrize("repo_url", [TEST_REPO, TEST_REPO_SPECIFIC_BRANCH])
def test_remote_git_config_source(monkeypatch, repo_url):

    monkeypatch.setattr(
        "diracx.core.config.sources.DEFAULT_CONFIG_FILE",
        "k3s/examples/cs.yaml",
    )
    remote_conf = ConfigSource.create_from_url(backend_url=repo_url)
    assert isinstance(remote_conf, RemoteGitConfigSource)

    hexsha, modified = remote_conf.latest_revision()
    assert isinstance(hexsha, str)
    assert isinstance(modified, datetime.datetime)
    result = remote_conf.read_raw(hexsha, modified)
    assert isinstance(result, Config)
