from __future__ import annotations

import shutil

import pytest
from typer.testing import CliRunner

from diracx.cli import app
from diracx.core.config import ConfigSource

runner = CliRunner()

TEST_VO = "testvo"
TEST_USER_GROUP = "user"
TEST_ADMIN_GROUP = "admin"


@pytest.fixture(scope="session")
def reference_cs_repo(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("reference_cs_repo")
    cs_repo = f"git+file://{tmp_path}"

    result = runner.invoke(app, ["internal", "generate-cs", cs_repo])
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        app,
        [
            "internal",
            "add-vo",
            cs_repo,
            f"--vo={TEST_VO}",
            "--idp-url=https://idp.invalid",
            "--idp-client-id=idp-client-id",
        ],
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        app,
        [
            "internal",
            "add-group",
            cs_repo,
            f"--vo={TEST_VO}",
            f"--group={TEST_ADMIN_GROUP}",
        ],
    )
    assert result.exit_code == 0, result.output

    yield tmp_path


@pytest.fixture
def cs_repo(reference_cs_repo, tmp_path):
    shutil.copytree(reference_cs_repo, tmp_path / "cs")
    yield f"git+file://{tmp_path}/cs"


@pytest.mark.parametrize("protocol", [None, "git+file://"])
def test_generate_cs(tmp_path, protocol):
    cs_repo = f"{tmp_path}"
    if protocol is None:
        cs_repo = f"git+file://{cs_repo}"

    result = runner.invoke(app, ["internal", "generate-cs", cs_repo])
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".git").is_dir()
    assert (tmp_path / "default.yml").is_file()

    # Running a second time should fail
    result = runner.invoke(app, ["internal", "generate-cs", cs_repo])
    assert result.exit_code != 0


def test_add_vo(cs_repo):
    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()

    assert TEST_VO in config.Registry
    assert config.Registry[TEST_VO].DefaultGroup == "user"
    assert config.Registry[TEST_VO].IdP.URL == "https://idp.invalid"
    assert config.Registry[TEST_VO].IdP.ClientID == "idp-client-id"

    # Add a second VO to it
    vo2 = "lhcb"
    result = runner.invoke(
        app,
        [
            "internal",
            "add-vo",
            cs_repo,
            f"--vo={vo2}",
            "--idp-url=https://idp.example.invalid",
            "--idp-client-id=idp-client-id2",
            "--default-group",
            "admin",
        ],
    )

    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()
    assert result.exit_code == 0, result.output

    assert vo2 in config.Registry
    assert config.Registry[vo2].DefaultGroup == "admin"
    assert config.Registry[vo2].IdP.URL == "https://idp.example.invalid"
    assert config.Registry[vo2].IdP.ClientID == "idp-client-id2"

    # Try to insert a VO that already exists
    result = runner.invoke(
        app,
        [
            "internal",
            "add-vo",
            cs_repo,
            f"--vo={TEST_VO}",
            "--idp-url=https://idp.invalid",
            "--idp-client-id=idp-client-id",
        ],
    )
    assert result.exit_code != 0, result.output


def test_add_group(cs_repo):
    new_group = "testgroup2"

    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()

    assert TEST_USER_GROUP in config.Registry[TEST_VO].Groups
    assert config.Registry[TEST_VO].Groups[TEST_USER_GROUP].JobShare == 1000
    assert config.Registry[TEST_VO].Groups[TEST_USER_GROUP].Properties == {"NormalUser"}
    assert config.Registry[TEST_VO].Groups[TEST_USER_GROUP].Users == set()

    # Add a second group to it
    result = runner.invoke(
        app,
        [
            "internal",
            "add-group",
            cs_repo,
            f"--vo={TEST_VO}",
            f"--group={new_group}",
            "--properties",
            "NormalUser",
            "--properties",
            "AdminUser",
        ],
    )
    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()
    assert result.exit_code == 0, result.output

    assert new_group in config.Registry[TEST_VO].Groups
    assert config.Registry[TEST_VO].Groups[new_group].JobShare == 1000
    assert config.Registry[TEST_VO].Groups[new_group].Properties == {
        "AdminUser",
        "NormalUser",
    }
    assert config.Registry[TEST_VO].Groups[new_group].Users == set()

    # Try to insert a group that already exists
    result = runner.invoke(
        app,
        [
            "internal",
            "add-group",
            cs_repo,
            f"--vo={TEST_VO}",
            f"--group={TEST_USER_GROUP}",
        ],
    )
    assert result.exit_code != 0, result.output

    # Try to insert a group with a non-existing VO
    result = runner.invoke(
        app,
        [
            "internal",
            "add-group",
            cs_repo,
            "--vo=nonexistingvo",
            f"--group={TEST_USER_GROUP}",
        ],
    )
    assert result.exit_code != 0, result.output


@pytest.mark.parametrize("vo", ["nonexistingvo", TEST_VO])
@pytest.mark.parametrize(
    "user_group",
    [["nonexisting_group"], [TEST_USER_GROUP], [TEST_USER_GROUP, TEST_ADMIN_GROUP], []],
)
def test_add_user(cs_repo, vo, user_group):
    sub = "lhcb:chaen"
    preferred_username = "dontCallMeShirley"

    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()

    # Check the user isn't in it
    if vo in config.Registry:
        assert sub not in config.Registry[vo].Users

    # Add a user to it
    result = runner.invoke(
        app,
        [
            "internal",
            "add-user",
            cs_repo,
            f"--vo={vo}",
            f"--sub={sub}",
            f"--preferred-username={preferred_username}",
        ]
        + [f"--group={x}" for x in user_group],
    )

    if "nonexistingvo" in vo or "nonexisting_group" in user_group:
        assert result.exit_code != 0
        return

    assert result.exit_code == 0, result.output

    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()
    # check the user is defined
    assert vo in config.Registry
    assert sub in config.Registry[vo].Users
    for group in user_group or [TEST_USER_GROUP]:
        assert config.Registry[vo].Groups[group].Users == {sub}
