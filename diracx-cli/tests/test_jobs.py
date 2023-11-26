from __future__ import annotations

import json

from diracx import cli


async def test_search(with_cli_login, capfd):
    await cli.jobs.search()
    cap = capfd.readouterr()
    assert cap.err == ""
    # By default the output should be in JSON format as capfd is not a TTY
    json.loads(cap.out)
