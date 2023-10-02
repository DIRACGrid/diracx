from __future__ import annotations

import asyncio
import base64
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin

import pytest
import requests

from diracx import cli


async def test_login(monkeypatch, capfd, cli_env):
    """Test that the CLI can login successfully"""
    expected_credentials_path = Path(
        cli_env["HOME"], ".cache", "diracx", "credentials.json"
    )

    # Ensure the credentials file does not exist before logging in
    assert not expected_credentials_path.exists()

    # Do the actual login
    await do_successful_login(monkeypatch, capfd, cli_env)

    # Ensure the credentials file exists after logging in
    assert expected_credentials_path.exists()


async def test_invalid_credentials_file(monkeypatch, capfd, cli_env):
    """Test that the CLI can handle an invalid credentials file"""
    expected_credentials_path = Path(
        cli_env["HOME"], ".cache", "diracx", "credentials.json"
    )
    expected_credentials_path.parent.mkdir(parents=True, exist_ok=True)
    expected_credentials_path.write_text("invalid json")

    # Do the actual login
    await do_successful_login(monkeypatch, capfd, cli_env)


async def test_invalid_access_token(cli_env, monkeypatch, capfd, with_cli_login):
    """Test that the CLI can handle an invalid access token

    We expect the CLI to detect the invalid access token and use the refresh
    token to get a new access token without prompting the user to login again.
    """
    expected_credentials_path = Path(
        cli_env["HOME"], ".cache", "diracx", "credentials.json"
    )

    credentials = json.loads(expected_credentials_path.read_text())
    bad_credentials = credentials | {
        "access_token": make_invalid_jwt(credentials["access_token"]),
        "expires_on": credentials["expires_on"] - 3600,
    }
    expected_credentials_path.write_text(json.dumps(bad_credentials))

    # See if the credentials still work
    await cli.whoami()
    cap = capfd.readouterr()
    assert cap.err == ""
    assert json.loads(cap.out)["vo"] == "diracAdmin"


@pytest.mark.xfail(reason="TODO: Implement nicer error handling in the CLI")
async def test_invalid_refresh_token(cli_env, monkeypatch, capfd, with_cli_login):
    """Test that the CLI can handle an invalid refresh token

    We expect the CLI to detect the invalid refresh token and prompt the user
    to login again.
    """
    expected_credentials_path = Path(
        cli_env["HOME"], ".cache", "diracx", "credentials.json"
    )

    credentials = json.loads(expected_credentials_path.read_text())
    bad_credentials = credentials | {
        "refresh_token": make_invalid_jwt(credentials["refresh_token"]),
        "expires_on": credentials["expires_on"] - 3600,
    }
    expected_credentials_path.write_text(json.dumps(bad_credentials))

    with pytest.raises(SystemExit):
        await cli.whoami()
    cap = capfd.readouterr()
    assert cap.out == ""
    assert "dirac login" in cap.err

    # Having invalid credentials should prompt the user to login again
    await do_successful_login(monkeypatch, capfd, cli_env)

    # See if the credentials work
    await cli.whoami()
    cap = capfd.readouterr()
    assert cap.err == ""
    assert json.loads(cap.out)["vo"] == "diracAdmin"


# ###############################################
# The rest of this file contains helper functions
# ###############################################


async def do_successful_login(monkeypatch, capfd, cli_env):
    """Do a successful login using the CLI"""
    poll_attempts = 0

    def fake_sleep(*args, **kwargs):
        nonlocal poll_attempts

        # Keep track of the number of times this is called
        poll_attempts += 1

        # After polling 5 times, do the actual login
        if poll_attempts == 5:
            # The login URL should have been printed to stdout
            captured = capfd.readouterr()
            match = re.search(rf"{cli_env['DIRACX_URL']}[^\n]+", captured.out)
            assert match, captured

            do_device_flow_with_dex(match.group())

        # Ensure we don't poll forever
        assert poll_attempts <= 10

        # Reduce the sleep duration to zero to speed up the test
        return unpatched_sleep(0)

    # We monkeypatch asyncio.sleep to provide a hook to run the actions that
    # would normally be done by a user. This includes capturing the login URL
    # and doing the actual device flow with dex.
    unpatched_sleep = asyncio.sleep
    with monkeypatch.context() as m:
        m.setattr("asyncio.sleep", fake_sleep)

        # Run the login command
        await cli.login(vo="diracAdmin", group=None, property=None)

    captured = capfd.readouterr()
    assert "Login successful!" in captured.out
    assert captured.err == ""


def do_device_flow_with_dex(url: str) -> None:
    """Do the device flow with dex"""

    class DexLoginFormParser(HTMLParser):
        def handle_starttag(self, tag, attrs):
            nonlocal action_url
            if "form" in str(tag):
                assert action_url is None
                action_url = urljoin(login_page_url, dict(attrs)["action"])

    # Get the login page
    r = requests.get(url)
    r.raise_for_status()
    login_page_url = r.url  # This is not the same as URL as we redirect to dex
    login_page_body = r.text

    # Search the page for the login form so we know where to post the credentials
    action_url = None
    DexLoginFormParser().feed(login_page_body)
    assert action_url is not None, login_page_body

    # Do the actual login
    r = requests.post(
        action_url, data={"login": "admin@example.com", "password": "password"}
    )
    r.raise_for_status()
    # This should have redirected to the DiracX page that shows the login is complete
    assert "Please close the window" in r.text


def make_invalid_jwt(jwt: str) -> str:
    """Make an invalid JWT by reversing the signature"""
    header, payload, signature = jwt.split(".")
    # JWT's don't have padding but base64.b64decode expects it
    raw_signature = base64.urlsafe_b64decode(pad_base64(signature))
    bad_signature = base64.urlsafe_b64encode(raw_signature[::-1])
    return ".".join([header, payload, bad_signature.decode("ascii").rstrip("=")])


def pad_base64(data):
    """Add padding to base64 data"""
    missing_padding = len(data) % 4
    if missing_padding != 0:
        data += "=" * (4 - missing_padding)
    return data
