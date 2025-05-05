from __future__ import annotations

import asyncio
from functools import partial

import pytest
from git import Repo

from diracx.core.config import ConfigSource

pytestmark = pytest.mark.enabled_dependencies(["AuthDB", "ConfigSource"])


async def test_live(client_factory):
    config_source = client_factory.all_dependency_overrides[
        ConfigSource.create
    ].__self__

    with client_factory.unauthenticated() as client:
        probe_fcn = partial(client.get, "/api/health/live")
        r = await _test_after_clear(config_source, probe_fcn)
        assert r.json() == {"status": "live"}

        old_config = await config_source.read_config_non_blocking()
        repo = Repo(config_source.repo_location)
        commit = repo.index.commit("Test commit")
        assert commit.hexsha not in old_config._hexsha

        # This will trigger a config update but still return the old config
        config_source._revision_cache.soft_cache.clear()
        new_config = await config_source.read_config_non_blocking()
        assert commit.hexsha not in old_config._hexsha

        # If we wait long enough, the new config should be returned
        for _ in range(50):
            await asyncio.sleep(0.1)

            r = probe_fcn()
            assert r.status_code == 200, r.text
            new_config = await config_source.read_config_non_blocking()
            if commit.hexsha in new_config._hexsha:
                break
        else:
            raise AssertionError("New config not found in probe response", r.text)

        # Ensure everything is still okay
        r = probe_fcn()
        assert r.status_code == 200, r.text


async def test_ready(client_factory):
    config_source = client_factory.all_dependency_overrides[
        ConfigSource.create
    ].__self__

    with client_factory.unauthenticated() as client:
        probe_fcn = partial(client.get, "/api/health/ready")
        r = await _test_after_clear(config_source, probe_fcn)
        assert r.json() == {"status": "ready"}


async def test_startup(client_factory):
    config_source = client_factory.all_dependency_overrides[
        ConfigSource.create
    ].__self__

    with client_factory.unauthenticated() as client:
        probe_fcn = partial(client.get, "/api/health/startup")
        r = await _test_after_clear(config_source, probe_fcn)
        assert r.json() == {"status": "startup complete"}


async def _test_after_clear(config_source, probe_fcn):
    """Ensure that the probe fails after clearing the config source caches.

    :param config_source: The config source to clear.
    :param probe_fcn: The function to call to make the probe request.

    :return: The response from the probe.
    """
    orig_r = probe_fcn()
    assert orig_r.status_code == 200, orig_r.text

    # Ensure that a 503 error is returned if the config source is not ready
    config_source.clear_caches()
    r = probe_fcn()
    assert r.status_code == 503, r.text

    # Ensure that the startup probe returns to normal after a short delay
    for _ in range(50):
        r = probe_fcn()
        if r.status_code != 503:
            assert r.status_code == 200, r.text
            assert r.json() == orig_r.json(), (r.text, orig_r.text)
            return r
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("Probe did not return to normal", r.text)
