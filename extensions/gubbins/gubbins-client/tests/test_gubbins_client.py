"""
These tests make sure that we can access all the original client as well as the extension
We do it in subprocesses to avoid conflict between the MetaPathFinder and pytest test discovery
"""

import os
import shlex
import shutil
import subprocess
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


def test_client_extension(fake_cli_env, tmp_path):
    """
    Make sure that the DiracClient can call gubbins routes

    We run the test as a separate python script to make sure that MetaPathFinder
    behaves as expected in a normal python code, and not inside pytest
    """
    test_code = """
from diracx.client import DiracClient
with DiracClient() as api:
    print(f"{api.jobs=}")
    assert "diracx.client.generated.operations._patch.JobsOperations" in str(api.jobs)
    print(f"{api.lollygag=}")
    assert "gubbins.client.generated.operations._operations.LollygagOperations" in str(api.lollygag)

"""
    with open(tmp_path / "test_client_ext.py", "wt") as f:
        f.write(test_code)
    try:
        with open(tmp_path / "std.out", "wt") as f:

            subprocess.run(  # noqa
                [shutil.which("python"), tmp_path / "test_client_ext.py"],
                env=os.environ,
                text=True,
                stdout=f,
                stderr=f,
                check=True,
            )
    except subprocess.CalledProcessError as e:
        raise AssertionError(Path(tmp_path / "std.out").read_text()) from e


def test_gubbins_client(fake_cli_env, tmp_path):
    """Make sure that we can use the GubbinsClient directly

    We run the test as a separate python script to make sure that MetaPathFinder
    behaves as expected in a normal python code, and not inside pytest
    """

    test_code = """
from gubbins.client import GubbinsClient
with GubbinsClient() as api:
    print(f"{api.jobs=}")
    assert "diracx.client.generated.operations._patch.JobsOperations" in str(api.jobs)
    print(f"{api.lollygag=}")
    assert "gubbins.client.generated.operations._operations.LollygagOperations" in str(api.lollygag)

"""
    with open(tmp_path / "test_client_ext.py", "wt") as f:
        f.write(test_code)
    try:
        with open(tmp_path / "std.out", "wt") as f:
            subprocess.run(  # noqa
                [shutil.which("python"), tmp_path / "test_client_ext.py"],
                env=os.environ,
                text=True,
                stdout=f,
                stderr=f,
                check=True,
            )
    except subprocess.CalledProcessError as e:
        raise AssertionError(Path(tmp_path / "std.out").read_text()) from e


def test_async_client_extension(fake_cli_env, tmp_path):
    """
    Make sure that the DiracClient can call gubbins routes

    We run the test as a separate python script to make sure that MetaPathFinder
    behaves as expected in a normal python code, and not inside pytest
    """
    test_code = """

import asyncio

async def main():
    from diracx.client.aio import DiracClient
    async with DiracClient() as api:
        print(f"{api.jobs=}")
        assert "diracx.client.generated.aio.operations._patch.JobsOperations" in str(api.jobs)
        print(f"{api.lollygag=}")
        assert "gubbins.client.generated.aio.operations._operations.LollygagOperations" in str(api.lollygag)
asyncio.run(main())

"""
    with open(tmp_path / "test_client_ext.py", "wt") as f:
        f.write(test_code)
    try:
        with open(tmp_path / "std.out", "wt") as f:

            subprocess.run(  # noqa
                [shutil.which("python"), tmp_path / "test_client_ext.py"],
                env=os.environ,
                text=True,
                stdout=f,
                stderr=f,
                check=True,
            )
    except subprocess.CalledProcessError as e:
        raise AssertionError(Path(tmp_path / "std.out").read_text()) from e


def test_async_gubbins_client(fake_cli_env, tmp_path):
    """Make sure that we can use the GubbinsClient directly

    We run the test as a separate python script to make sure that MetaPathFinder
    behaves as expected in a normal python code, and not inside pytest
    """

    test_code = """

import asyncio

async def main():
    from gubbins.client.aio import GubbinsClient
    async with GubbinsClient() as api:
        print(f"{api.jobs=}")
        assert "diracx.client.generated.aio.operations._patch.JobsOperations" in str(api.jobs)
        print(f"{api.lollygag=}")
        assert "gubbins.client.generated.aio.operations._operations.LollygagOperations" in str(api.lollygag)
asyncio.run(main())

"""
    with open(tmp_path / "test_client_ext.py", "wt") as f:
        f.write(test_code)
    try:
        with open(tmp_path / "std.out", "wt") as f:
            subprocess.run(  # noqa
                [shutil.which("python"), tmp_path / "test_client_ext.py"],
                env=os.environ,
                text=True,
                stdout=f,
                stderr=f,
                check=True,
            )
    except subprocess.CalledProcessError as e:
        raise AssertionError(Path(tmp_path / "std.out").read_text()) from e
