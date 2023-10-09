from __future__ import annotations

import asyncio
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin

import requests

from diracx import cli


def do_device_flow_with_dex(url: str, ca_path: str) -> None:
    """Do the device flow with dex"""

    class DexLoginFormParser(HTMLParser):
        def handle_starttag(self, tag, attrs):
            nonlocal action_url
            if "form" in str(tag):
                assert action_url is None
                action_url = urljoin(login_page_url, dict(attrs)["action"])

    # Get the login page
    r = requests.get(url, verify=ca_path)
    r.raise_for_status()
    login_page_url = r.url  # This is not the same as URL as we redirect to dex
    login_page_body = r.text

    # Search the page for the login form so we know where to post the credentials
    action_url = None
    DexLoginFormParser().feed(login_page_body)
    assert action_url is not None, login_page_body

    # Do the actual login
    r = requests.post(
        action_url,
        data={"login": "admin@example.com", "password": "password"},
        verify=ca_path,
    )
    r.raise_for_status()
    # This should have redirected to the DiracX page that shows the login is complete
    assert "Please close the window" in r.text


async def test_login(monkeypatch, capfd, cli_env):
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

            do_device_flow_with_dex(match.group(), cli_env["DIRACX_CA_PATH"])

        # Ensure we don't poll forever
        assert poll_attempts <= 100

        # Reduce the sleep duration to zero to speed up the test
        return unpatched_sleep(0)

    # We monkeypatch asyncio.sleep to provide a hook to run the actions that
    # would normally be done by a user. This includes capturing the login URL
    # and doing the actual device flow with dex.
    unpatched_sleep = asyncio.sleep

    expected_credentials_path = Path(
        cli_env["HOME"], ".cache", "diracx", "credentials.json"
    )

    # Ensure the credentials file does not exist before logging in
    assert not expected_credentials_path.exists()

    # Run the login command
    with monkeypatch.context() as m:
        m.setattr("asyncio.sleep", fake_sleep)
        await cli.login(vo="diracAdmin", group=None, property=None)
    captured = capfd.readouterr()
    assert "Login successful!" in captured.out
    assert captured.err == ""

    # Ensure the credentials file exists after logging in
    assert expected_credentials_path.exists()

    # Return the credentials so this test can also be used by the
    # "with_cli_login" fixture
    return expected_credentials_path.read_text()


async def test_logout(monkeypatch, capfd, cli_env, with_cli_login):
    expected_credentials_path = expected_credentials_path = Path(
        cli_env["HOME"], ".cache", "diracx", "credentials.json"
    )
    # Ensure the credentials file does exist
    assert expected_credentials_path.exists()

    # Run the logout command
    await cli.logout()
    captured = capfd.readouterr()
    assert "Removed credentials from" in captured.out
    assert "Logout successful!" in captured.out
    assert captured.err == ""

    # Ensure the credentials file does not exist after logging out
    assert not expected_credentials_path.exists()

    # Rerun the logout command, it should not fail
    await cli.logout()
    captured = capfd.readouterr()
    assert "Removed credentials from" not in captured.out
    assert "Logout successful!" in captured.out
    assert captured.err == ""

    # Ensure the credentials file still does not exist
    assert not expected_credentials_path.exists()
