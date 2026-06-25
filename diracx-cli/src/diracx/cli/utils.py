"""Utility helpers for the CLI.

This module provides a small helper class `AsyncTyper` that adapts
asynchronous command functions to Typer's synchronous command model by
running the coroutine with ``asyncio.run`` and handling common network and
authentication errors with user-friendly messages.
"""

from __future__ import annotations

__all__ = ["AsyncTyper"]

from asyncio import run
from functools import wraps

import typer
from azure.core.exceptions import ClientAuthenticationError
from httpx import ConnectError
from rich import print


class AsyncTyper(typer.Typer):
    """Typer subclass that supports async command registration.

    Register an async function as a Typer command using the ``async_command``
    decorator. The decorator wraps the coroutine so it can be run
    synchronously by Typer (via ``asyncio.run``) and prints friendly error
    messages for common exceptions like authentication or connection errors.
    """

    def async_command(self, *args, **kwargs):
        def decorator(async_func):
            @wraps(async_func)
            def sync_func(*_args, **_kwargs):
                """Wrapper that runs the async function and handles errors.

                The wrapper executes the provided coroutine using
                ``asyncio.run`` and intercepts common exceptions to print
                actionable messages rather than surfacing a stack trace to
                the end user.

                Args:
                    *_args: Positional arguments forwarded to the async function.
                    **_kwargs: Keyword arguments forwarded to the async function.

                Returns:
                    The result of the coroutine, if any.
                """
                try:
                    return run(async_func(*_args, **_kwargs))
                except ClientAuthenticationError:
                    print(
                        ":x: [bold red]You are not authenticated. Log in with:[/bold red] "
                        "[bold] dirac login [OPTIONS] [VO] [/bold]"
                    )
                except ConnectError:
                    print(
                        ":x: [bold red]Please configure a valid DiracX server.[/bold red]"
                    )

            self.command(*args, **kwargs)(sync_func)
            return async_func

        return decorator
