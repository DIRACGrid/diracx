from __future__ import annotations

import fcntl
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


def test_read_credentials_reading_locked_file(monkeypatch, tmp_path):
    """Test that read_credentials waits to read a locked file.

    To keep the test simple and deterministic, we patch 'fcntl.flock' within the 'diracx.core.utils' module.
    This will raise a BlockingIOError when attempting to read the file.
    """
    token_location = tmp_path / "credentials.json"

    # Write valid credentials to the file to ensure read_credentials attempts to lock
    token_location.write_text(CREDENTIALS_CONTENT)

    # Patch 'fcntl.flock' within the 'diracx.core.utils' module
    # This will raise a BlockingIOError when attempting to read the file
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


def test_write_credentials_writing_locked_file(monkeypatch, tmp_path):
    """Test that write_credentials waits to write into a locked file.

    To keep the test simple and deterministic, we patch 'fcntl.flock' within the 'diracx.core.utils' module.
    This will raise a BlockingIOError when attempting to write to the file.
    """
    token_location = tmp_path / "credentials.json"

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
        write_credentials(TokenResponse(**TOKEN_RESPONSE_DICT), location=token_location)

    # Verify that flock was called (for LOCK_EX)
    assert len(flock_calls) == 1, "fcntl.flock was not called"
    assert (
        flock_calls[-1][1] == fcntl.LOCK_EX
    ), f"Expected LOCK_EX, got {flock_calls[-1][1]}"
    assert "File is locked" in str(exc_info.value)


def test_read_credentials_empty_file(tmp_path):
    """Test that read_credentials raises an appropriate error for an empty file."""
    empty_location = tmp_path / "credentials.json"
    empty_location.touch()

    with pytest.raises(RuntimeError) as exc_info:
        read_credentials(location=empty_location)

    assert "Error reading credentials:" in str(exc_info.value)
    assert "Expecting value" in str(exc_info.value)


def test_read_credentials_missing_file(tmp_path):
    """Test that read_credentials raises an appropriate error for a missing file."""
    missing_file = tmp_path / "missing.json"
    with pytest.raises(RuntimeError) as exc_info:
        read_credentials(location=missing_file)

    assert "Error reading credentials:" in str(exc_info.value)
    assert "No such file or directory" in str(exc_info.value)


def test_write_credentials_unavailable_path():
    """Test that write_credentials raises error when it can't create path."""
    wrong_path = Path("/wrong/path/file.txt")
    with pytest.raises((PermissionError, OSError)):
        write_credentials(TokenResponse(**TOKEN_RESPONSE_DICT), location=wrong_path)


def test_read_credentials_invalid_content(tmp_path):
    """Test that read_credentials raises an appropriate error for a file with invalid content."""
    malformed_token_location = tmp_path / "credentials.json"
    malformed_token_location.write_text("invalid content")

    with pytest.raises(RuntimeError) as exc_info:
        read_credentials(location=malformed_token_location)

    assert "Error reading credentials:" in str(exc_info.value)
    assert "Expecting value" in str(exc_info.value)


def test_read_credentials_valid_file(tmp_path):
    """Test that read_credentials works correctly with a valid file."""
    token_location = tmp_path / "credentials.json"
    token_location.write_text(CREDENTIALS_CONTENT)

    credentials = read_credentials(location=token_location)

    token_response = TokenResponse(**TOKEN_RESPONSE_DICT)

    assert credentials.access_token == token_response.access_token
    assert credentials.expires_in < token_response.expires_in
    assert credentials.token_type == token_response.token_type
    assert credentials.refresh_token == token_response.refresh_token
