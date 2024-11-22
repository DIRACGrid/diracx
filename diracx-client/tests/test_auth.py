import fcntl
import json
from datetime import datetime, time, timedelta, timezone
from multiprocessing import Pool
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pytest
from azure.core.credentials import AccessToken

from diracx.client.patches.utils import get_token
from diracx.core.models import TokenResponse
from diracx.core.utils import serialize_credentials

TOKEN_RESPONSE_DICT = {
    "access_token": "test_token",
    "expires_in": int((datetime.now(tz=timezone.utc) + timedelta(days=1)).timestamp()),
    "token_type": "Bearer",
    "refresh_token": "test_refresh",
}
CREDENTIALS_CONTENT: str = serialize_credentials(TokenResponse(**TOKEN_RESPONSE_DICT))


def lock_and_read_file(file_path):
    """Lock and read file."""
    with open(file_path, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        f.read()
        time.sleep(2)
        fcntl.flock(f, fcntl.LOCK_UN)


def lock_and_write_file(file_path: Path):
    """Lock and write file."""
    with open(file_path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(CREDENTIALS_CONTENT)
        time.sleep(2)
        fcntl.flock(f, fcntl.LOCK_UN)


@pytest.fixture
def concurrent_access_to_lock_file():

    def run_processes(proc_to_test, *, read=True):
        """Run the process to be tested and attempt to read or write concurrently."""
        location = proc_to_test[1]["location"]
        error_dict = dict()
        with Pool(2) as pool:
            if read:
                # Creating the file before reading it
                with open(location, "w") as f:
                    f.write(CREDENTIALS_CONTENT)
                pool.apply_async(
                    lock_and_read_file,
                    args=(location,),
                    error_callback=lambda e: error_callback(
                        e, error_dict, "lock_and_read_file"
                    ),
                )
            else:
                pool.apply_async(
                    lock_and_write_file,
                    args=(location,),
                    error_callback=lambda e: error_callback(
                        e, error_dict, "lock_and_write_file"
                    ),
                )
            time.sleep(1)
            result = pool.apply_async(
                proc_to_test[0],
                kwds=proc_to_test[1],
                error_callback=lambda e: error_callback(
                    e, error_dict, f"{proc_to_test[0].__name__}"
                ),
            )
            pool.close()
            pool.join()
            res = result.get(timeout=1)
        return res, error_dict

    return run_processes


def error_callback(error, error_dict, process_name):
    """Called if the process fails."""
    error_dict[process_name] = error


@pytest.fixture
def token_setup() -> tuple[TokenResponse, Path, AccessToken]:
    """Setup token response and location."""
    with NamedTemporaryFile(delete=False) as tmp:
        token_location = Path(tmp.name)
    token_response = TokenResponse(**TOKEN_RESPONSE_DICT)
    access_token = AccessToken(token_response.access_token, token_response.expires_in)

    yield token_response, token_location, access_token

    if token_location.exists():
        token_location.unlink()


def test_get_token_accessing_lock_file(token_setup, concurrent_access_to_lock_file):
    """Test get_token is waiting to read token from locked file."""
    token_response, token_location, _ = token_setup
    process_to_test = (
        get_token,
        {
            "location": token_location,
            "token": None,
            "token_endpoint": "/endpoint",
            "client_id": "ID",
            "verify": False,
        },
    )
    result, error_dict = concurrent_access_to_lock_file(process_to_test, read=False)
    assert not error_dict
    assert isinstance(result, AccessToken)
    assert result.token == token_response.access_token


def test_get_token_valid_input_token(token_setup):
    """Test that get_token return the valid token."""
    _, token_location, access_token = token_setup
    result = get_token(
        location=token_location,
        token=access_token,
        token_endpoint="",
        client_id="ID",
        verify=False,
    )

    assert result == access_token


def test_get_token_valid_input_credential():
    """Test that get_token return the valid token given in the credential file."""
    with NamedTemporaryFile(delete=False) as tmp:
        tmp.write(CREDENTIALS_CONTENT.encode())
        temp_file = Path(tmp.name)
    result = get_token(
        location=temp_file, token=None, token_endpoint="", client_id="ID", verify=False
    )
    temp_file.unlink()
    assert isinstance(result, AccessToken)


def test_get_token_input_token_not_exists(token_setup):
    _, token_location, _ = token_setup
    result = get_token(
        location=token_location,
        token=None,
        token_endpoint="",
        client_id="ID",
        verify=False,
    )
    assert result is None


def test_get_token_invalid_input():
    """Test that get_token manage invalid input token."""
    # Test wrong key in credential
    wrong_credential_content = "'{\"wrong_key\": False}'"
    with NamedTemporaryFile(delete=False) as tmp:
        tmp.write(json.dumps(wrong_credential_content).encode())
        temp_file = Path(tmp.name)
    result = get_token(
        location=temp_file, token=None, token_endpoint="", client_id="ID", verify=False
    )
    temp_file.unlink()
    assert result is None

    # Test with invalid token date
    token_response = TOKEN_RESPONSE_DICT.copy()
    token_response["expires_in"] = int(datetime.now(tz=timezone.utc).timestamp())
    with NamedTemporaryFile(delete=False) as tmp:
        tmp.write(json.dumps(token_response).encode())
        temp_file = Path(tmp.name)

    result = get_token(
        location=temp_file, token=None, token_endpoint="", client_id="ID", verify=False
    )
    temp_file.unlink()
    assert result is None


def test_get_token_refresh_valid():
    """Test that get_token refresh a valid outdated token."""
    token_response = TOKEN_RESPONSE_DICT.copy()
    # the future content of the refreshed token
    refresh_token = TokenResponse(**token_response)
    # Create expired credential file
    token_response["expires_on"] = int(
        (datetime.now(tz=timezone.utc) - timedelta(seconds=10)).timestamp()
    )
    token_response.pop("expires_in")
    with NamedTemporaryFile(delete=False) as tmp:
        tmp.write(json.dumps(token_response).encode())
        temp_file = Path(tmp.name)

    with (
        patch(
            "diracx.client.patches.utils.is_refresh_token_valid", return_value=True
        ) as mock_is_refresh_valid,
        patch(
            "diracx.client.patches.utils.refresh_token", return_value=refresh_token
        ) as mock_refresh_token,
    ):
        result = get_token(
            location=temp_file,
            token=None,
            token_endpoint="",
            client_id="ID",
            verify=False,
        )

    # Verify that the credential fil has been refreshed:
    with open(temp_file, "r") as f:
        content = f.read()
        assert content == serialize_credentials(refresh_token)

    temp_file.unlink()

    assert result is not None
    assert isinstance(result, AccessToken)
    assert result.token == refresh_token.access_token
    assert result.expires_on > refresh_token.expires_in
    mock_is_refresh_valid.assert_called_once_with(refresh_token.refresh_token)
    mock_refresh_token.assert_called_once_with(
        "", "ID", refresh_token.refresh_token, verify=False
    )


def test_get_token_refresh_invalid():
    """Test that get_token manages an invalid refresh token."""
    token_response = TOKEN_RESPONSE_DICT.copy()
    refresh_token = TokenResponse(**token_response)
    token_response["expires_on"] = int(
        (datetime.now(tz=timezone.utc) - timedelta(seconds=10)).timestamp()
    )
    token_response.pop("expires_in")
    with NamedTemporaryFile(delete=False) as tmp:
        tmp.write(json.dumps(token_response).encode())
        temp_file = Path(tmp.name)

    with (
        patch(
            "diracx.client.patches.utils.is_refresh_token_valid", return_value=False
        ) as mock_is_refresh_valid,
    ):
        result = get_token(
            location=temp_file,
            token=None,
            token_endpoint="",
            client_id="ID",
            verify=False,
        )

    temp_file.unlink()
    assert result is None
    mock_is_refresh_valid.assert_called_once_with(refresh_token.refresh_token)
