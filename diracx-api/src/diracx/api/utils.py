"""Shared helpers for the asynchronous DiracX API."""

from __future__ import annotations

__all__ = ["with_client"]

from functools import wraps

from diracx.client.aio import AsyncDiracClient


def with_client(func):
    """Decorate an async function to provide it with an ``AsyncDiracClient``.

    If the function call already supplies a ``client`` keyword argument, that
    client is used. Otherwise, a new client is created as an async context
    manager and passed to the function.

    Args:
        func: Async function that accepts a ``client`` keyword argument.

    Returns:
        Wrapped async function that uses the supplied client or manages a new one.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        if "client" in kwargs:
            return await func(*args, **kwargs)

        async with AsyncDiracClient() as client:
            return await func(*args, **kwargs, client=client)

    return wrapper
