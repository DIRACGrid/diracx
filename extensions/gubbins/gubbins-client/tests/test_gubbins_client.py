"""
These tests make sure that we can access all the original client as well as the extension
We do it in subprocesses to avoid conflict between the MetaPathFinder and pytest test discovery
"""

import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def fake_cli_env(monkeypatch, tmp_path):
    from diracx.core.preferences import get_diracx_preferences

    env = {
        "DIRACX_URL": "http://localhost:9999",
        "DIRACX_CA_PATH": str(tmp_path),
        "HOME": str(tmp_path),
    }

    for key, value in env.items():
        monkeypatch.setenv(key, value)

    data_dir = (
        Path(__file__).parents[2] / "gubbins-routers/tests/data/idp-server.invalid"
    )

    run_server_cmd = f"{shutil.which('python')} -m http.server -d {data_dir} 9999"
    proc = subprocess.Popen(shlex.split(run_server_cmd))  # noqa
    print(proc)
    yield
    proc.kill()

    get_diracx_preferences.cache_clear()


@pytest.mark.parametrize(
    "import_name,object_name",
    [
        ("diracx.client.aio", "AsyncDiracClient"),
        ("diracx.client.sync", "SyncDiracClient"),
        ("gubbins.client.aio", "AsyncGubbinsClient"),
        ("gubbins.client.sync", "SyncGubbinsClient"),
    ],
)
def test_client_extension(fake_cli_env, tmp_path, import_name, object_name):
    """
    Make sure that the DiracClient can call gubbins routes

    We run the test as a separate python script to make sure that MetaPathFinder
    behaves as expected in a normal python code, and not inside pytest
    """
    is_async = object_name.startswith("Async")
    real_object_name = object_name.replace("Dirac", "Gubbins")
    aio_or_sync = "aio" if is_async else "sync"
    test_code = f"""
import asyncio
from {import_name} import {object_name} as TestClass

async def main():
    if TestClass.__name__ != {real_object_name!r}:
        raise ValueError(f"Expected {real_object_name} but got {{api.__class__.__name__}}")

    mro = [x.__module__ for x in TestClass.__mro__]
    print(f"{{TestClass.__mro__=}}")
    print(f"{{mro=}}")

    a = 'gubbins.client._generated{'.aio' if is_async else ''}._patch'
    b = 'diracx.client.patches.client.{aio_or_sync}'
    c = 'gubbins.client._generated{'.aio' if is_async else ''}._client'
    d = 'builtins'

    assert mro[0] == "gubbins.client.{aio_or_sync}", mro
    assert mro.index(a) < mro.index(b)
    assert mro.index(b) < mro.index(c)
    assert mro.index(c) < mro.index(d)

    {'async ' if is_async else ''}with TestClass() as api:
        print(f"{{api.jobs=}}")
        print(f"{{api.lollygag=}}")

    # Do the print without spaces to make it unabiguous to read in the output
    print("All", "is", "okay")

if __name__ == "__main__":
    asyncio.run(main())
"""

    script_path = tmp_path / "test_client_ext.py"
    script_path.write_text(test_code)
    proc = subprocess.run(  # noqa: S603
        [sys.executable, script_path], text=True, check=False, capture_output=True
    )
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
    assert proc.returncode == 0
    assert "All is okay" in proc.stdout
