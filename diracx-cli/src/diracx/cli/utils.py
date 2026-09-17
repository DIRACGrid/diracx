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
from httpx2 import ConnectError
from rich import print


class AsyncTyper(typer.Typer):
    """Typer subclass that supports async command registration.

    Register an async function as a Typer command using the ``async_command``
    decorator. The decorator wraps the coroutine so it can be run
    synchronously by Typer (via ``asyncio.run``) and prints friendly error
    messages for common exceptions like authentication or connection errors.
    """

    def async_command(self, *args, **kwargs):
        """Register an async function as a Typer command.

        The returned decorator wraps the coroutine with ``asyncio.run`` so the
        command can be executed from Typer's synchronous runtime. Common
        authentication and connection errors are caught and shown with
        user-friendly messages.

        Args:
            *args: Positional arguments forwarded to ``Typer.command``.
            **kwargs: Keyword arguments forwarded to ``Typer.command``.

        Returns:
            A decorator that registers the async function and returns the
            original coroutine function.
        """

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
                except ConnectError:
                    print(
                        ":x: [bold red]Please configure a valid DiracX server.[/bold red]"
                    )

            self.command(*args, **kwargs)(sync_func)
            return async_func

        return decorator
