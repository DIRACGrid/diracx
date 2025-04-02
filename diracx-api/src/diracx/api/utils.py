from __future__ import annotations

__all__ = ("with_client",)

from functools import wraps

from diracx.client.aio import AsyncDiracClient


def with_client(func):
    """Decorator to provide a AsyncDiracClient to a function.

    If the function already has a `client` keyword argument, it will be used.
    Otherwise, a new AsyncDiracClient will be created and passed as a keyword argument.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        if "client" in kwargs:
            return await func(*args, **kwargs)

        async with AsyncDiracClient() as client:
            return await func(*args, **kwargs, client=client)

    return wrapper
