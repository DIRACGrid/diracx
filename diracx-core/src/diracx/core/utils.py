from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from diracx.core.models import TokenResponse

EXPIRES_GRACE_SECONDS = 15


def dotenv_files_from_environment(prefix: str) -> list[str]:
    """Get the sorted list of .env files to use for configuration"""
    env_files = {}
    for key, value in os.environ.items():
        if match := re.fullmatch(rf"{prefix}(?:_(\d+))?", key):
            env_files[int(match.group(1) or -1)] = value
    return [v for _, v in sorted(env_files.items())]


def serialize_credentials(token_response: TokenResponse) -> str:
    """Serialize DiracX client credentials to a string

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


def write_credentials(token_response: TokenResponse, *, location: Path | None = None):
    """Write credentials received in dirax_preferences.credentials_path"""
    from diracx.core.preferences import get_diracx_preferences

    credentials_path = location or get_diracx_preferences().credentials_path
    credentials_path.parent.mkdir(parents=True, exist_ok=True)
    credentials_path.write_text(serialize_credentials(token_response))
