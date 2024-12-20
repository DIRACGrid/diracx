from __future__ import annotations

from pathlib import Path

from diracx import cli


async def test_login(monkeypatch, capfd, cli_env):
    from diracx.testing import test_login

    return await test_login(monkeypatch, capfd, cli_env)


async def test_logout(monkeypatch, capfd, cli_env, with_cli_login):
    expected_credentials_path = expected_credentials_path = Path(
        cli_env["HOME"], ".cache", "diracx", "credentials.json"
    )
    # Ensure the credentials file does exist
    assert expected_credentials_path.exists()

    # Run the logout command
    await cli.auth.logout()
    captured = capfd.readouterr()
    assert "Removed credentials from" in captured.out
    assert "Logout successful!" in captured.out
    assert captured.err == ""

    # Ensure the credentials file does not exist after logging out
    assert not expected_credentials_path.exists()

    # Rerun the logout command, it should not fail
    await cli.auth.logout()
    captured = capfd.readouterr()
    assert "Removed credentials from" not in captured.out
    assert "Logout successful!" in captured.out
    assert captured.err == ""

    # Ensure the credentials file still does not exist
    assert not expected_credentials_path.exists()
