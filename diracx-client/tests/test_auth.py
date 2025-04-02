from __future__ import annotations

import fcntl
import json
from datetime import datetime, timedelta, timezone

import jwt
import pytest
from azure.core.credentials import AccessToken

from diracx.client.patches.utils import get_token
from diracx.core.models import TokenResponse
from diracx.core.utils import serialize_credentials

# Create a fake jwt dictionary
REFRESH_CONTENT = {
    "jti": "f0706e0a-af1e-4538-9f1f-7b9620783cba",
    "exp": int((datetime.now(tz=timezone.utc) + timedelta(days=1)).timestamp()),
    "legacy_exchange": False,
    "dirac_policies": {},
}

TOKEN_RESPONSE_DICT = {
    "access_token": "test_token",
    "expires_in": 3600,
    "token_type": "Bearer",
    "refresh_token": jwt.encode(REFRESH_CONTENT, "secret_key"),
}
CREDENTIALS_CONTENT: str = serialize_credentials(TokenResponse(**TOKEN_RESPONSE_DICT))


def test_get_token_accessing_lock_file(monkeypatch, tmp_path):
    """Test get_token is waiting to read token from locked file."""
    token_location = tmp_path / "credentials.json"
    token_location.write_text(CREDENTIALS_CONTENT)

    # Patch 'fcntl.flock' within the 'diracx.client.patches.utils' module
    flock_calls = []

    def mock_flock(file, operation):
        flock_calls.append((file, operation))
        if operation == fcntl.LOCK_EX:
            raise BlockingIOError("File is locked")

    monkeypatch.setattr("diracx.client.patches.utils.fcntl.flock", mock_flock)

    # Attempt to get a token, expecting a BlockingIOError due to the lock
    with pytest.raises(BlockingIOError) as exc_info:
        get_token(
            location=token_location,
            token=None,
            token_endpoint="/endpoint",
            client_id="ID",
            verify=False,
        )

    # Verify that flock was called with LOCK_EX
    assert len(flock_calls) == 1, "fcntl.flock was not called"
    assert (
        flock_calls[-1][1] == fcntl.LOCK_EX
    ), f"Expected LOCK_SH, got {flock_calls[-1][1]}"
    assert "File is locked" in str(exc_info.value)


def test_get_token_valid_input_token(tmp_path):
    """Test that get_token return the valid provided token."""
    token_location = tmp_path / "credentials.json"
    # Create a valid access token
    token_response = TokenResponse(**TOKEN_RESPONSE_DICT)
    access_token = AccessToken(
        token_response.access_token,
        int(
            (
                datetime.now(tz=timezone.utc)
                + timedelta(seconds=token_response.expires_in)
            ).timestamp()
        ),
    )

    # Call get_token
    result = get_token(
        location=token_location,
        token=access_token,
        token_endpoint="",
        client_id="ID",
        verify=False,
    )

    assert result == access_token


def test_get_token_valid_input_credential(tmp_path):
    """Test that get_token return the valid token given in the credential file."""
    token_location = tmp_path / "credentials.json"
    token_location.write_text(CREDENTIALS_CONTENT)

    # Call get_token
    result = get_token(
        location=token_location,
        token=None,
        token_endpoint="",
        client_id="ID",
        verify=False,
    )

    # Verify that the returned token is the expected token
    assert isinstance(result, AccessToken)
    assert result.token == TOKEN_RESPONSE_DICT["access_token"]
    assert result.expires_on > datetime.now(tz=timezone.utc).timestamp()


def test_get_token_input_token_not_exists(tmp_path):
    """Test that get_token return an empty token when the provided token does not exist."""
    token_location = tmp_path / "credentials.json"

    # Call get_token
    result = get_token(
        location=token_location,
        token=None,
        token_endpoint="",
        client_id="ID",
        verify=False,
    )
    assert isinstance(result, AccessToken)
    assert result.token == ""
    assert result.expires_on == 0


def test_get_token_invalid_input(tmp_path):
    """Test that get_token manages invalid input token."""
    # Test wrong key in credential
    wrong_credential_content = {"wrong_key": False}

    token_location = tmp_path / "credentials.json"
    token_location.write_text(json.dumps(wrong_credential_content))

    # Call get_token
    result = get_token(
        location=token_location,
        token=None,
        token_endpoint="",
        client_id="ID",
        verify=False,
    )
    # Verify that the returned token is empty
    assert isinstance(result, AccessToken)
    assert result.token == ""
    assert result.expires_on == 0


def test_get_token_refresh_valid(monkeypatch, tmp_path):
    """Test that get_token refresh a valid outdated token."""
    token_response = TOKEN_RESPONSE_DICT.copy()
    # Expected future content of the refreshed token
    expected_token_response = TokenResponse(**token_response)

    # Create expired access token
    token_response["expires_on"] = int(
        (datetime.now(tz=timezone.utc) - timedelta(seconds=10)).timestamp()
    )
    token_response.pop("expires_in")

    # Write expired credentials to a file
    token_location = tmp_path / "credentials.json"
    token_location.write_text(json.dumps(token_response))

    # Mock the refresh_token function
    was_refresh_called = False

    def mock_refresh(token_endpoint, client_id, refresh_token, verify):
        nonlocal was_refresh_called
        was_refresh_called = True
        return TokenResponse(**TOKEN_RESPONSE_DICT)

    monkeypatch.setattr("diracx.client.patches.utils.refresh_token", mock_refresh)

    # Call get_token
    result = get_token(
        location=token_location,
        token=None,
        token_endpoint="",
        client_id="ID",
        verify=False,
    )

    # Verify that the credential file has been refreshed:
    with open(token_location, "r") as f:
        content = f.read()
        assert content == serialize_credentials(expected_token_response)

    # Verify that the returned token is the expected refreshed token
    assert result is not None
    assert isinstance(result, AccessToken)
    assert result.token == expected_token_response.access_token
    assert result.expires_on > token_response["expires_on"]
    assert was_refresh_called


def test_get_token_refresh_expired(tmp_path):
    """Test that get_token manages an expired refresh token: should return an empty token."""
    # Create expired access token and refresh token
    token_response = TOKEN_RESPONSE_DICT.copy()
    refresh_token = REFRESH_CONTENT.copy()

    refresh_token["exp"] = int(
        (datetime.now(tz=timezone.utc) - timedelta(seconds=10)).timestamp()
    )

    token_response["expires_on"] = int(
        (datetime.now(tz=timezone.utc) - timedelta(seconds=10)).timestamp()
    )
    token_response.pop("expires_in")
    token_response["refresh_token"] = jwt.encode(refresh_token, "secret_key")

    # Write expired credentials to a file
    token_location = tmp_path / "credentials.json"
    token_location.write_text(json.dumps(token_response))

    # Call get_token
    result = get_token(
        location=token_location,
        token=None,
        token_endpoint="",
        client_id="ID",
        verify=False,
    )

    # Verify that the returned token is empty
    assert isinstance(result, AccessToken)
    assert result.token == ""
    assert result.expires_on == 0
