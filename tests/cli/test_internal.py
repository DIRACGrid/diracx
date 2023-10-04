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


@pytest.mark.parametrize("vo", ["nonexistingvo", "testvo"])
@pytest.mark.parametrize("user_group", ["nonexisting_group", "user"])
def test_add_user(tmp_path, vo, user_group):
    cs_repo = f"git+file://{tmp_path}"

    sub = "lhcb:chaen"
    preferred_username = "dontCallMeShirley"

    # Create the CS
    runner.invoke(app, ["internal", "generate-cs", cs_repo])

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
            "--vo",
            vo,
            "--user-group",
            user_group,
            "--sub",
            sub,
            "--preferred-username",
            preferred_username,
        ],
    )

    if "nonexisting" in vo or "nonexisting" in user_group:
        assert result.exit_code != 0
        return

    assert result.exit_code == 0, result.output

    config = ConfigSource.create_from_url(backend_url=cs_repo).read_config()
    # check the user is defined
    assert vo in config.Registry
    assert sub in config.Registry[vo].Users
