from __future__ import annotations

import asyncio

import pytest

from diracx.core.config import ConfigSource

pytestmark = pytest.mark.enabled_dependencies(["AuthDB", "ConfigSource"])


def test_live(client_factory):
    with client_factory.unauthenticated() as client:
        r = client.get("/api/health/live")
        assert r.status_code == 200, r.text
        assert r.json() == {"status": "live"}


def test_ready(client_factory):
    with client_factory.unauthenticated() as client:
        r = client.get("/api/health/ready")
        assert r.status_code == 200, r.text
        assert r.json() == {"status": "ready"}


async def test_startup(client_factory):
    with client_factory.unauthenticated() as client:
        r = client.get("/api/health/startup")
        assert r.status_code == 200, r.text
        assert r.json() == {"status": "startup complete"}

        # Ensure that a 503 error is returned if the config source is not ready
        client_factory.all_dependency_overrides[
            ConfigSource.create
        ].__self__.clear_caches()
        r = client.get("/api/health/startup")
        assert r.status_code == 503, r.text
        assert "is being loaded, please retry later" in r.json()["detail"]

        # Ensure that the startup probe returns to normal after a short delay
        for _ in range(50):
            r = client.get("/api/health/startup")
            if r.status_code != 503:
                assert r.status_code == 200, r.text
                assert r.json() == {"status": "startup complete"}
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("Start up check did not return to normal", r.text)
