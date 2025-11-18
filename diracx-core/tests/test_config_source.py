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


def test_file_url_config_source(tmp_path):
    """Test that file URLs work with RemoteGitConfigSource."""
    from git import Repo

    # Create a test git repository
    repo = Repo.init(tmp_path, initial_branch="master")
    cs_file = tmp_path / "default.yml"
    example_cs = Config.model_validate(
        {
            "DIRAC": {},
            "Registry": {},
            "Operations": {},
        }
    )
    cs_file.write_text(example_cs.model_dump_json())
    repo.index.add([cs_file])
    repo.index.commit("Initial commit")

    # Test with git+file:// URL
    file_url = f"git+file://{tmp_path}"
    config_source = ConfigSource.create_from_url(backend_url=file_url)
    assert isinstance(config_source, RemoteGitConfigSource)

    hexsha, modified = config_source.latest_revision()
    assert isinstance(hexsha, str)
    assert isinstance(modified, datetime.datetime)
    result = config_source.read_raw(hexsha, modified)
    assert isinstance(result, Config)
