# Can't using PEP-604 with typer: https://github.com/tiangolo/typer/issues/348
# from __future__ import annotations
from __future__ import annotations

__all__ = ["dump"]

import json

from rich import print_json

from diracx.client.aio import AsyncDiracClient
from diracx.core.preferences import OutputFormats, get_diracx_preferences

from .utils import AsyncTyper

app = AsyncTyper()


@app.async_command()
async def dump():
    """Fetch and display server configuration using the configured output format.

    This CLI command queries the server's `serve_config` endpoint and prints
    the returned configuration using the user's preferred output format (JSON
    or rich). The command delegates presentation to the `display` helper.

    Returns:
        None
    """
    async with AsyncDiracClient() as api:
        config = await api.config.serve_config()
        display(config)


def display(data):
    """Render `data` using the configured output format.

    The helper reads the `output_format` preference and selects an
    appropriate renderer. Supported formats are JSON (pretty-printed) and
    rich (uses Rich's `print_json`). An unknown format raises
    `NotImplementedError`.

    Args:
        data: Arbitrary JSON-serializable data to display.

    Raises:
        NotImplementedError: If the configured output format is unsupported.
    """
    output_format = get_diracx_preferences().output_format
    match output_format:
        case OutputFormats.JSON:
            print(json.dumps(data, indent=2))
        case OutputFormats.RICH:
            print_json(data=data)
        case _:
            raise NotImplementedError(output_format)
