from __future__ import annotations

import pytest
from typer.testing import CliRunner

from diracx.cli import app
from diracx.core.config import ConfigSource

runner = CliRunner()


@pytest.mark.parametrize("protocol", [None, "git+file://"])
def test_generate_cs(tmp_path, protocol):
    cs_repo = f"{tmp_path}"
    if protocol is None:
        cs_repo = f"git+file://{cs_repo}"

    result = runner.invoke(app, ["internal", "generate-cs", cs_repo])
    assert result.exit_code == 0
    assert (tmp_path / ".git").is_dir()
    assert (tmp_path / "default.yml").is_file()

    # Running a second time should fail
    result = runner.invoke(app, ["internal", "generate-cs", cs_repo])
    assert result.exit_code != 0


def test_add_vo(tmp_path):
    cs_repo = f"git+file://{tmp_path}"

    # Create the CS
    runner.invoke(app, ["internal", "generate-cs", cs_repo])

    # Add a VO to it
    vo1 = "testvo"
    result = runner.invoke(
        app,
        [
            "internal",
            "add-vo",
            cs_repo,
            f"--vo={vo1}",
            "--idp-url=https://idp.invalid",
            "--idp-client-id=idp-client-id",
        ],
    )
    assert result.exit_code == 0, result.output

    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()

    assert vo1 in config.Registry
    assert config.Registry[vo1].DefaultGroup == "user"
    assert config.Registry[vo1].IdP.URL == "https://idp.invalid"
    assert config.Registry[vo1].IdP.ClientID == "idp-client-id"

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
            f"--vo={vo1}",
            "--idp-url=https://idp.invalid",
            "--idp-client-id=idp-client-id",
        ],
    )
    assert result.exit_code != 0, result.output


def test_add_group(tmp_path):
    cs_repo = f"git+file://{tmp_path}"
    vo = "testvo"
    group1 = "testgroup1"
    group2 = "testgroup2"

    # Create the CS
    runner.invoke(app, ["internal", "generate-cs", cs_repo])
    runner.invoke(
        app,
        [
            "internal",
            "add-vo",
            cs_repo,
            f"--vo={vo}",
            "--idp-url=https://idp.invalid",
            "--idp-client-id=idp-client-id",
        ],
    )

    # Add a group to it
    result = runner.invoke(
        app, ["internal", "add-group", cs_repo, f"--vo={vo}", f"--group={group1}"]
    )
    assert result.exit_code == 0, result.output

    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()

    assert group1 in config.Registry[vo].Groups
    assert config.Registry[vo].Groups[group1].JobShare == 1000
    assert config.Registry[vo].Groups[group1].Properties == {"NormalUser"}
    assert config.Registry[vo].Groups[group1].Users == set()

    # Add a second group to it
    result = runner.invoke(
        app,
        [
            "internal",
            "add-group",
            cs_repo,
            f"--vo={vo}",
            f"--group={group2}",
            "--properties",
            "NormalUser",
            "--properties",
            "AdminUser",
        ],
    )
    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()
    assert result.exit_code == 0, result.output

    assert group2 in config.Registry[vo].Groups
    assert config.Registry[vo].Groups[group2].JobShare == 1000
    assert config.Registry[vo].Groups[group2].Properties == {"AdminUser", "NormalUser"}
    assert config.Registry[vo].Groups[group2].Users == set()

    # Try to insert a group that already exists
    result = runner.invoke(
        app, ["internal", "add-group", cs_repo, f"--vo={vo}", f"--group={group1}"]
    )
    assert result.exit_code != 0, result.output

    # Try to insert a group with a non-existing VO
    result = runner.invoke(
        app,
        ["internal", "add-group", cs_repo, "--vo=nonexistingvo", f"--group={group1}"],
    )
    assert result.exit_code != 0, result.output


@pytest.mark.parametrize("vo", ["nonexistingvo", "testvo"])
@pytest.mark.parametrize(
    "user_group", [["nonexisting_group"], ["user"], ["user", "admin"], []]
)
def test_add_user(tmp_path, vo, user_group):
    cs_repo = f"git+file://{tmp_path}"

    sub = "lhcb:chaen"
    preferred_username = "dontCallMeShirley"

    # Create the CS
    runner.invoke(app, ["internal", "generate-cs", cs_repo])
    runner.invoke(
        app,
        [
            "internal",
            "add-vo",
            cs_repo,
            "--vo=testvo",
            "--idp-url=https://idp.invalid",
            "--idp-client-id=idp-client-id",
        ],
    )
    runner.invoke(
        app, ["internal", "add-group", cs_repo, "--vo=testvo", "--group=user"]
    )
    runner.invoke(
        app, ["internal", "add-group", cs_repo, "--vo=testvo", "--group=admin"]
    )

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
    for group in user_group or ["user"]:
        assert config.Registry[vo].Groups[group].Users == {sub}
