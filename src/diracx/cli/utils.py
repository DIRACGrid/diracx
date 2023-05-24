from __future__ import annotations

__all__ = ("AsyncTyper", "CREDENTIALS_PATH", "get_auth_headers")

import json
from asyncio import run
from functools import wraps
from pathlib import Path

import typer

CREDENTIALS_PATH = Path.home() / ".cache" / "diracx" / "credentials.json"


class AsyncTyper(typer.Typer):
    def async_command(self, *args, **kwargs):
        def decorator(async_func):
            @wraps(async_func)
            def sync_func(*_args, **_kwargs):
                return run(async_func(*_args, **_kwargs))

            self.command(*args, **kwargs)(sync_func)
            return async_func

        return decorator


def get_auth_headers():
    # TODO: Use autorest's actual mechanism for this
    if not CREDENTIALS_PATH.exists():
        raise NotImplementedError("Login first")
    credentials = json.loads(CREDENTIALS_PATH.read_text())
    return {"Authorization": f"Bearer {credentials['access_token']}"}
