from __future__ import annotations

import datetime
from pathlib import Path
from urllib import request

import pytest

from diracx.core.config import Config, ConfigSource, RemoteGitConfigSource

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


@pytest.mark.skipif(github_is_down(), reason="Github unavailable")
def test_remote_git_config_source_default(monkeypatch):
    default_remote_conf_assertions(monkeypatch, TEST_REPO)


@pytest.mark.skipif(github_is_down(), reason="Github unavailable")
def test_remote_git_config_source_branch(monkeypatch):
    monkeypatch.setattr(
        "diracx.core.config.sources.DEFAULT_GIT_BRANCH",
        "non_existing_branch",
    )
    default_remote_conf_assertions(monkeypatch, TEST_REPO_SPECIFIC_BRANCH)


@pytest.mark.skipif(github_is_down(), reason="Github unavailable")
def test_remote_git_config_source_commit(monkeypatch):
    hexsha = default_remote_conf_assertions(monkeypatch, TEST_REPO_SPECIFIC_COMMIT_HASH)
    assert hexsha == COMMIT_HASH


@pytest.fixture
def local_repo(tmp_path):
    """A local config repository on master, with an ``other`` branch."""
    from git import Repo

    repo = Repo.init(tmp_path / "repo", initial_branch="master")
    with repo.config_writer() as config:
        config.set_value("user", "name", "test")
        config.set_value("user", "email", "test@example.invalid")
        config.set_value("commit", "gpgsign", "false")
    (tmp_path / "repo" / "default.yml").write_text("master: true\n")
    repo.index.add(["default.yml"])
    repo.index.commit("master")
    repo.create_head("other")
    repo.heads.other.checkout()
    (tmp_path / "repo" / "default.yml").write_text("other: true\n")
    repo.index.add(["default.yml"])
    other = repo.index.commit("other")
    repo.heads.master.checkout()
    return repo, other


def test_local_git_config_source_does_not_modify_the_repository(local_repo):
    """The repository is shared by all the processes: it must not be modified.

    A ``git checkout`` at startup made the processes race on ``index.lock``.
    """
    repo, other = local_repo

    source = ConfigSource.create_from_url(
        backend_url=f"git+file://{repo.working_dir}?revision=other"
    )

    # Neither HEAD nor the working tree changed
    assert repo.head.ref.name == "master"
    assert not repo.is_dirty()
    assert (Path(repo.working_dir) / "default.yml").read_text() == "master: true\n"
    # But the requested revision is read
    hexsha, _ = source.latest_revision()
    assert hexsha == other.hexsha


def test_local_git_config_source_unknown_revision(local_repo):
    repo, _ = local_repo
    with pytest.raises(ValueError, match="Revision 'nope' not found"):
        ConfigSource.create_from_url(
            backend_url=f"git+file://{repo.working_dir}?revision=nope"
        )
