#!/usr/bin/env python
from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from diracx.core.models import TokenResponse
from diracx.core.properties import NORMAL_USER
from diracx.core.settings import AuthSettings
from diracx.core.utils import write_credentials
from diracx.logic.auth.token import create_token


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("token_key", type=Path, help="The key to sign the token with")
    args = parser.parse_args()
    main(args.token_key.read_text())


def main(token_key):
    vo = "diracAdmin"
    dirac_group = "admin"
    sub = "75212b23-14c2-47be-9374-eb0113b0575e"
    preferred_username = "localuser"
    dirac_properties = [NORMAL_USER]
    settings = AuthSettings(token_key=token_key)
    creation_time = datetime.now(tz=timezone.utc)
    expires_in = 7 * 24 * 60 * 60

    access_payload = {
        "sub": f"{vo}:{sub}",
        "vo": vo,
        "iss": settings.token_issuer,
        "dirac_properties": dirac_properties,
        "jti": str(uuid.uuid4()),
        "preferred_username": preferred_username,
        "dirac_group": dirac_group,
        "exp": creation_time + timedelta(seconds=expires_in),
    }
    token = TokenResponse(
        access_token=create_token(access_payload, settings),
        expires_in=expires_in,
        refresh_token=None,
    )
    write_credentials(token)


if __name__ == "__main__":
    parse_args()
