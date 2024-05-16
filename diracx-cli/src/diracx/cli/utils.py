from __future__ import annotations

__all__ = ("AsyncTyper",)

from asyncio import run
from functools import wraps

import typer
from azure.core.exceptions import ClientAuthenticationError
from rich import print


class AsyncTyper(typer.Typer):
    def async_command(self, *args, **kwargs):
        def decorator(async_func):
            @wraps(async_func)
            def sync_func(*_args, **_kwargs):
                try:
                    return run(async_func(*_args, **_kwargs))
                except ClientAuthenticationError:
                    print(
                        ":x: [bold red]You are not authenticated. Log in with:[/bold red] "
                        "[bold] dirac login [OPTIONS] [VO] [/bold]"
                    )

            self.command(*args, **kwargs)(sync_func)
            return async_func

        return decorator
