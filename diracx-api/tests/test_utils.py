from __future__ import annotations

from diracx.api.utils import with_client
from diracx.client.aio import AsyncDiracClient


async def test_with_client_default(with_cli_login):
    """Ensure that the with_client decorator provides a AsyncDiracClient."""

    @with_client
    async def test_func(*, client):
        assert isinstance(client, AsyncDiracClient)

    await test_func()


async def test_with_client_override():
    """Ensure that the with_client can be overridden by providing a client kwarg."""

    @with_client
    async def test_func(*, client):
        assert client == "foobar"

    await test_func(client="foobar")
