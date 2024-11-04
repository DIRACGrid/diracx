from __future__ import annotations

import fcntl
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from diracx.core.models import TokenResponse

EXPIRES_GRACE_SECONDS = 15


def dotenv_files_from_environment(prefix: str) -> list[str]:
    """Get the sorted list of .env files to use for configuration."""
    env_files = {}
    for key, value in os.environ.items():
        if match := re.fullmatch(rf"{prefix}(?:_(\d+))?", key):
            env_files[int(match.group(1) or -1)] = value
    return [v for _, v in sorted(env_files.items())]


def serialize_credentials(token_response: TokenResponse) -> str:
    """Serialize DiracX client credentials to a string.

    This method is separated from write_credentials to allow for DIRAC to be
    able to serialize credentials for inclusion in the proxy file.
    """
    expires = datetime.now(tz=timezone.utc) + timedelta(
        seconds=token_response.expires_in - EXPIRES_GRACE_SECONDS
    )
    credential_data = {
        "access_token": token_response.access_token,
        "refresh_token": token_response.refresh_token,
        "expires_on": int(datetime.timestamp(expires)),
    }
    return json.dumps(credential_data)


def read_credentials(location: Path) -> TokenResponse:
    """Read credentials from a file."""
    from diracx.core.preferences import get_diracx_preferences

    credentials_path = location or get_diracx_preferences().credentials_path
    try:
        with open(credentials_path, "r") as f:
            # Lock the file to prevent other processes from writing to it at the same time
            fcntl.flock(f, fcntl.LOCK_SH)
            # Read the credentials from the file
            try:
                credentials = json.load(f)
            finally:
                # Release the lock
                fcntl.flock(f, fcntl.LOCK_UN)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Error reading credentials: {e}") from e

    return TokenResponse(
        access_token=credentials["access_token"],
        expires_in=credentials["expires_on"]
        - int(datetime.now(tz=timezone.utc).timestamp()),
        token_type="Bearer",  # noqa: S106
        refresh_token=credentials.get("refresh_token"),
    )


def write_credentials(token_response: TokenResponse, *, location: Path | None = None):
    """Write credentials received in dirax_preferences.credentials_path."""
    from diracx.core.preferences import get_diracx_preferences

    credentials_path = location or get_diracx_preferences().credentials_path
    credentials_path.parent.mkdir(parents=True, exist_ok=True)

    with open(credentials_path, "w") as f:
        # Lock the file to prevent other processes from writing to it at the same time
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            # Write the credentials to the file
            f.write(serialize_credentials(token_response))
            f.flush()
            os.fsync(f.fileno())
        finally:
            # Release the lock
            fcntl.flock(f, fcntl.LOCK_UN)
