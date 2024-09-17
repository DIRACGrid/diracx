from __future__ import annotations

import fcntl
import time
from datetime import datetime, timedelta, timezone
from multiprocessing import Pool
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from diracx.core.models import TokenResponse
from diracx.core.utils import (
    dotenv_files_from_environment,
    read_credentials,
    serialize_credentials,
    write_credentials,
)


def test_dotenv_files_from_environment(monkeypatch):
    monkeypatch.setattr("os.environ", {})
    assert dotenv_files_from_environment("TEST_PREFIX") == []

    monkeypatch.setattr("os.environ", {"TEST_PREFIX": "/a"})
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a"]

    monkeypatch.setattr("os.environ", {"TEST_PREFIX": "/a", "TEST_PREFIX_1": "/b"})
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a", "/b"]

    monkeypatch.setattr(
        "os.environ",
        {"TEST_PREFIX_2": "/c", "TEST_PREFIX": "/a", "TEST_PREFIX_1": "/b"},
    )
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a", "/b", "/c"]

    monkeypatch.setattr(
        "os.environ",
        {"TEST_PREFIX_2a": "/c", "TEST_PREFIX": "/a", "TEST_PREFIX_1": "/b"},
    )
    assert dotenv_files_from_environment("TEST_PREFIX") == ["/a", "/b"]


TOKEN_RESPONSE_DICT = {
    "access_token": "test_token",
    "expires_in": int((datetime.now(tz=timezone.utc) + timedelta(days=1)).timestamp()),
    "token_type": "Bearer",
    "refresh_token": "test_refresh",
}
CREDENTIALS_CONTENT = serialize_credentials(TokenResponse(**TOKEN_RESPONSE_DICT))


def lock_and_read_file(file_path):
    """Lock and read file."""
    with open(file_path, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
        f.read()
        time.sleep(2)
        fcntl.flock(f, fcntl.LOCK_UN)


def lock_and_write_file(file_path: Path):
    """Lock and write file."""
    with open(file_path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.write(CREDENTIALS_CONTENT)
        time.sleep(2)
        fcntl.flock(f, fcntl.LOCK_UN)


@pytest.fixture
def token_setup() -> tuple[TokenResponse, Path]:
    """Setup token response and location."""
    token_location = Path(NamedTemporaryFile().name)
    token_response = TokenResponse(**TOKEN_RESPONSE_DICT)
    return token_response, token_location


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
            pool.apply_async(
                proc_to_test[0],
                kwds=proc_to_test[1],
                error_callback=lambda e: error_callback(
                    e, error_dict, f"{proc_to_test[0].__name__}"
                ),
            )
            pool.close()
            pool.join()
        return error_dict

    return run_processes


def error_callback(error, error_dict, process_name):
    """Called if the process fails."""
    error_dict[process_name] = error


def assert_read_credentials_error_message(exc_info):
    assert "Error reading credentials:" in exc_info.value.args[0]


def test_read_credentials_reading_locked_file(
    token_setup, concurrent_access_to_lock_file
):
    """Test that read_credentials reading a locked file end in error."""
    _, token_location = token_setup
    process_to_test = (read_credentials, {"location": token_location})
    error_dict = concurrent_access_to_lock_file(process_to_test, read=False)
    process_name = process_to_test[0].__name__
    if process_name in error_dict.keys():
        assert isinstance(error_dict[process_name], RuntimeError)
    else:
        raise AssertionError(
            "Expected a RuntimeError while reading locked credentials."
        )


def test_write_credentials_writing_locked_file(
    token_setup, concurrent_access_to_lock_file
):
    """Test that write_credentials writing a locked file end in error."""
    token_response, token_location = token_setup
    process_to_test = (
        write_credentials,
        {"token_response": token_response, "location": token_location},
    )
    error_dict = concurrent_access_to_lock_file(process_to_test)
    process_name = process_to_test[0].__name__
    if process_name in error_dict.keys():
        assert isinstance(error_dict[process_name], BlockingIOError)
    else:
        raise AssertionError(
            "Expected a BlockingIOError while writing locked credentials."
        )


def create_temp_file(content=None) -> Path:
    """Helper function to create a temporary file with optional content."""
    temp_file = NamedTemporaryFile(delete=False)
    temp_path = Path(temp_file.name)
    temp_file.close()
    if content is not None:
        temp_path.write_text(content)
    return temp_path


def test_read_credentials_empty_file():
    """Test that read_credentials raises an appropriate error for an empty file."""
    temp_file = create_temp_file("")

    with pytest.raises(RuntimeError) as exc_info:
        read_credentials(location=temp_file)

    temp_file.unlink()
    assert_read_credentials_error_message(exc_info)


def test_write_credentials_empty_file(token_setup):
    """Test that write_credentials raises an appropriate error for an empty file."""
    temp_file = create_temp_file("")
    token_response, _ = token_setup
    write_credentials(token_response, location=temp_file)
    temp_file.unlink()


def test_read_credentials_missing_file():
    """Test that read_credentials raises an appropriate error for a missing file."""
    missing_file = Path("/path/to/nonexistent/file.txt")
    with pytest.raises(RuntimeError) as exc_info:
        read_credentials(location=missing_file)
    assert_read_credentials_error_message(exc_info)


def test_write_credentials_unavailable_path(token_setup):
    """Test that write_credentials raises error when it can't create path."""
    wrong_path = Path("/wrong/path/file.txt")
    token_response, _ = token_setup
    with pytest.raises(PermissionError):
        write_credentials(token_response, location=wrong_path)


def test_read_credentials_invalid_content():
    """Test that read_credentials raises an appropriate error for a file with invalid content."""
    temp_file = create_temp_file("invalid content")

    with pytest.raises(RuntimeError) as exc_info:
        read_credentials(location=temp_file)

    temp_file.unlink()
    assert_read_credentials_error_message(exc_info)


def test_read_credentials_valid_file(token_setup):
    """Test that read_credentials works correctly with a valid file."""
    token_response, _ = token_setup
    temp_file = create_temp_file(content=CREDENTIALS_CONTENT)

    credentials = read_credentials(location=temp_file)
    temp_file.unlink()
    assert credentials.access_token == token_response.access_token
    assert credentials.expires_in < token_response.expires_in
    assert credentials.token_type == token_response.token_type
    assert credentials.refresh_token == token_response.refresh_token
