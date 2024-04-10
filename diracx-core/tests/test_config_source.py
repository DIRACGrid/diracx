import datetime

import pytest

from diracx.core.config import RemoteGitConfigSource
from diracx.core.config.schema import Config

DIRACX_URL = "https://github.com/DIRACGrid/diracx-charts/"


@pytest.fixture
def change_default_branch_and_file(monkeypatch):
    monkeypatch.setattr("diracx.core.config.DEFAULT_GIT_BRANCH", "master")
    monkeypatch.setattr(
        "diracx.core.config.DEFAULT_CONFIG_FILE",
        "k3s/examples/cs.yaml",
    )


def test_remote_git_config_source(change_default_branch_and_file):
    RemoteConf = RemoteGitConfigSource(backend_url=DIRACX_URL)
    hexsha, modified = RemoteConf.latest_revision()
    assert isinstance(hexsha, str)
    assert isinstance(modified, datetime.datetime)
    result = RemoteConf.read_raw(hexsha, modified)
    assert isinstance(result, Config)
    RemoteConf.clear_temp()
