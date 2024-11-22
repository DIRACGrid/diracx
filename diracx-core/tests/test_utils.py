from __future__ import annotations

import fcntl
from datetime import datetime, timedelta, timezone
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
CREDENTIALS_CONTENT: str = serialize_credentials(TokenResponse(**TOKEN_RESPONSE_DICT))


@pytest.fixture
def token_setup() -> tuple[TokenResponse, Path]:
    """Setup token response and location."""
    with NamedTemporaryFile(delete=False) as tmp:
        token_location = Path(tmp.name)
    token_response = TokenResponse(**TOKEN_RESPONSE_DICT)

    yield token_response, token_location

    if token_location.exists():
        token_location.unlink()


def test_read_credentials_reading_locked_file(monkeypatch, token_setup):
    """Test that read_credentials is waiting to read a locked file end in error."""
    _, token_location = token_setup

    # Write valid credentials to the file to ensure read_credentials attempts to lock
    token_location.write_text(CREDENTIALS_CONTENT)

    # Patch 'fcntl.flock' within the 'diracx.core.utils' module
    flock_calls = []

    def mock_flock(file, operation):
        flock_calls.append((file, operation))
        if operation == fcntl.LOCK_SH:
            raise BlockingIOError("File is locked")

    monkeypatch.setattr("diracx.core.utils.fcntl.flock", mock_flock)

    # Attempt to read credentials, expecting a BlockingIOError due to the lock
    with pytest.raises(BlockingIOError) as exc_info:
        read_credentials(location=token_location)

    # Verify that flock was called with LOCK_SH
    assert len(flock_calls) == 1, "fcntl.flock was not called"
    assert (
        flock_calls[-1][1] == fcntl.LOCK_SH
    ), f"Expected LOCK_SH, got {flock_calls[-1][1]}"
    assert "File is locked" in str(exc_info.value)


def test_write_credentials_writing_locked_file(monkeypatch, token_setup):
    """Test that write_credentials is waiting to write a locked file end in error."""
    token_response, token_location = token_setup

    # Write valid credentials to the file to ensure write_credentials attempts to lock
    token_location.write_text(CREDENTIALS_CONTENT)

    # Patch 'fcntl.flock' within the 'diracx.core.utils' module
    flock_calls = []

    def mock_flock(file, operation):
        flock_calls.append((file, operation))
        if operation == fcntl.LOCK_EX:
            raise BlockingIOError("File is locked")

    monkeypatch.setattr("diracx.core.utils.fcntl.flock", mock_flock)

    # Attempt to write credentials, expecting a BlockingIOError due to the lock
    with pytest.raises(BlockingIOError) as exc_info:
        write_credentials(token_response, location=token_location)

    # Verify that flock was called (for LOCK_EX)
    assert len(flock_calls) == 1, "fcntl.flock was not called"
    assert (
        flock_calls[-1][1] == fcntl.LOCK_EX
    ), f"Expected LOCK_EX, got {flock_calls[-1][1]}"
    assert "File is locked" in str(exc_info.value)


def test_read_credentials_empty_file():
    """Test that read_credentials raises an appropriate error for an empty file."""
    with NamedTemporaryFile(delete=False) as empty_file:
        token_location = Path(empty_file.name)

    with pytest.raises(RuntimeError) as exc_info:
        read_credentials(location=token_location)

    token_location.unlink()
    assert "Error reading credentials:" in str(exc_info.value)
    assert "Expecting value" in str(exc_info.value)


def test_read_credentials_missing_file():
    """Test that read_credentials raises an appropriate error for a missing file."""
    missing_file = Path("/path/to/nonexistent/file.txt")
    with pytest.raises(RuntimeError) as exc_info:
        read_credentials(location=missing_file)

    assert "Error reading credentials:" in str(exc_info.value)
    assert "No such file or directory" in str(exc_info.value)


def test_write_credentials_unavailable_path(token_setup):
    """Test that write_credentials raises error when it can't create path."""
    wrong_path = Path("/wrong/path/file.txt")
    token_response, _ = token_setup
    with pytest.raises(PermissionError):
        write_credentials(token_response, location=wrong_path)


def test_read_credentials_invalid_content():
    """Test that read_credentials raises an appropriate error for a file with invalid content."""
    with NamedTemporaryFile(delete=False) as invalid_file:
        invalid_file.write(b"invalid content")
        token_location = Path(invalid_file.name)

    with pytest.raises(RuntimeError) as exc_info:
        read_credentials(location=token_location)

    token_location.unlink()
    assert "Error reading credentials:" in str(exc_info.value)
    assert "Expecting value" in str(exc_info.value)


def test_read_credentials_valid_file(token_setup):
    """Test that read_credentials works correctly with a valid file."""
    token_response, token_location = token_setup
    token_location.write_text(CREDENTIALS_CONTENT)

    credentials = read_credentials(location=token_location)

    token_location.unlink()
    assert credentials.access_token == token_response.access_token
    assert credentials.expires_in < token_response.expires_in
    assert credentials.token_type == token_response.token_type
    assert credentials.refresh_token == token_response.refresh_token
