from __future__ import annotations

import datetime
from urllib import request

import pytest

from diracx.core.config import ConfigSource, RemoteGitConfigSource
from diracx.core.config.schema import Config

# The diracx-chart contains a CS example
TEST_REPO = "git+https://github.com/DIRACGrid/diracx-charts.git"
TEST_REPO_SPECIFIC_BRANCH = TEST_REPO + "?revision=master"
COMMIT_HASH = "03c5a890d1af4a0a0fb934acea8f538ba08ec68c"
TEST_REPO_SPECIFIC_COMMIT_HASH = TEST_REPO + f"?revision={COMMIT_HASH}"


def github_is_down():
    try:
        request.urlopen("https://github.com", timeout=1)
        return False
    except Exception:
        return True


def default_remote_conf_assertions(monkeypatch, repo_url):
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

    return hexsha


@pytest.mark.skipif(github_is_down(), reason="Github unavailble")
def test_remote_git_config_source_default(monkeypatch):
    default_remote_conf_assertions(monkeypatch, TEST_REPO)


@pytest.mark.skipif(github_is_down(), reason="Github unavailble")
def test_remote_git_config_source_branch(monkeypatch):
    monkeypatch.setattr(
        "diracx.core.config.sources.DEFAULT_GIT_BRANCH",
        "non_existing_branch",
    )
    default_remote_conf_assertions(monkeypatch, TEST_REPO_SPECIFIC_BRANCH)


@pytest.mark.skipif(github_is_down(), reason="Github unavailble")
def test_remote_git_config_source_commit(monkeypatch):
    hexsha = default_remote_conf_assertions(monkeypatch, TEST_REPO_SPECIFIC_COMMIT_HASH)
    assert hexsha == COMMIT_HASH
