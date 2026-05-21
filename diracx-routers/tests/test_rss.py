from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from fastapi import status

pytestmark = pytest.mark.enabled_dependencies(
    [
        "AuthSettings",
        "ResourceStatusDB",
        "SiteStatusSource",
        "FTSStatusSource",
        "ComputeElementStatusSource",
        "StorageElementStatusSource",
        "RSSAccessPolicy",
        "DevelopmentSettings",
    ]
)


async def _prepare_rss(client):
    """Seed the DB and warm every source cache inside a single connection."""
    from diracx.core.config.sources import AsyncCacheableSource
    from diracx.db.sql.rss.db import ResourceStatusDB

    db_override = client.app.dependency_overrides.get(ResourceStatusDB.no_transaction)
    if db_override is None:
        return
    # factory.py stores partial(db_transaction, db_instance); args[0] is the instance.
    db = db_override.args[0]
    now = datetime.now(tz=timezone.utc)

    # Seed — open one connection, insert all rows, then close it cleanly.
    async with db as conn:
        for status_type in ("ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"):
            await conn.insert_resource_status(
                name="SE-CERN",
                status="Active",
                status_type=status_type,
                vo="lhcb",
                reason="All good",
                date_effective=now,
            )
        await conn.insert_resource_status(
            name="CE-CERN",
            status="Active",
            status_type="all",
            vo="lhcb",
            reason="All good",
            date_effective=now,
        )
        await conn.insert_resource_status(
            name="FTS-CERN",
            status="Active",
            status_type="all",
            vo="lhcb",
            reason="All good",
            date_effective=now,
        )
        await conn.insert_site_status(
            name="LCG.CERN.cern",
            status="Active",
            vo="lhcb",
            reason="All good",
            date_effective=now,
        )
    # Connection is now fully closed and _conn ContextVar is reset.

    # Warm each source — each source.read() opens its own fresh connection.
    for override in client.app.dependency_overrides.values():
        source = getattr(override, "__self__", None)
        if isinstance(source, AsyncCacheableSource):
            await source.read()


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        asyncio.get_event_loop().run_until_complete(_prepare_rss(client))
        yield client


@pytest.fixture
def unauthenticated_client(client_factory):
    with client_factory.unauthenticated() as client:
        yield client


def test_unauthenticated(unauthenticated_client):
    response = unauthenticated_client.get("/api/rss/storage")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.parametrize(
    "endpoint",
    ["/api/rss/storage", "/api/rss/compute", "/api/rss/site", "/api/rss/fts"],
)
def test_get_resource_status(normal_user_client, endpoint):
    r = normal_user_client.get(endpoint)
    assert r.status_code == status.HTTP_200_OK, r.json()
    assert r.json(), r.text

    last_modified = r.headers["Last-Modified"]
    etag = r.headers["ETag"]

    # Matching ETag + matching Last-Modified → 304
    r = normal_user_client.get(
        endpoint,
        headers={"If-None-Match": etag, "If-Modified-Since": last_modified},
    )
    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text

    # Wrong ETag only → 200
    r = normal_user_client.get(
        endpoint,
        headers={"If-None-Match": "wrongEtag"},
    )
    assert r.status_code == status.HTTP_200_OK, r.json()
    assert r.json(), r.text

    # Past ETag + past timestamp → 200
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": "pastEtag",
            "If-Modified-Since": "Mon, 1 Apr 2000 00:42:42 GMT",
        },
    )
    assert r.status_code == status.HTTP_200_OK, r.json()
    assert r.json(), r.text

    # Wrong ETag + future timestamp → 304 (If-Modified-Since takes effect)
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": "futureEtag",
            "If-Modified-Since": "Mon, 1 Apr 9999 00:42:42 GMT",
        },
    )
    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text

    # Wrong ETag + invalid timestamp → 200
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": "futureEtag",
            "If-Modified-Since": "wrong format",
        },
    )
    assert r.status_code == status.HTTP_200_OK, r.json()
    assert r.json(), r.text

    # Correct ETag + past timestamp → 304 (ETag match takes priority)
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": "Mon, 1 Apr 2000 00:42:42 GMT",
        },
    )
    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text

    # Correct ETag + future timestamp → 304
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": "Mon, 1 Apr 9999 00:42:42 GMT",
        },
    )
    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text

    # Correct ETag + invalid timestamp → 304
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": "wrong format",
        },
    )
    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text
