from __future__ import annotations

import pytest
import requests
import yaml

from diracx.core.preferences import get_diracx_preferences


@pytest.fixture
def cli_env(monkeypatch, tmp_path, demo_dir):
    """Set up the environment for the CLI"""
    # HACK: Find the URL of the demo DiracX instance
    helm_values = yaml.safe_load((demo_dir / "values.yaml").read_text())
    host_url = helm_values["dex"]["config"]["issuer"].rsplit(":", 1)[0]
    diracx_url = f"{host_url}:8000"

    # Ensure the demo is working
    r = requests.get(f"{diracx_url}/openapi.json")
    r.raise_for_status()
    assert r.json()["info"]["title"] == "Dirac"

    env = {
        "DIRACX_URL": diracx_url,
        "HOME": tmp_path,
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    yield env

    # The DiracX preferences are cached however when testing this cache is invalid
    get_diracx_preferences.cache_clear()


@pytest.fixture
async def with_cli_login(monkeypatch, capfd, cli_env, tmp_path):
    from .test_login import do_successful_login

    try:
        await do_successful_login(monkeypatch, capfd, cli_env)
    except Exception:
        pytest.xfail("Login failed, fix test_login to re-enable this test")

    yield
