from __future__ import annotations

from datetime import datetime, timezone
from http import HTTPStatus

import pytest

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

ALL_ENDPOINTS = [
    "/api/rss/storage",
    "/api/rss/compute",
    "/api/rss/site",
    "/api/rss/fts",
]


def _get_rss_db(client):
    from diracx.db.sql.rss.db import ResourceStatusDB

    db_override = client.app.dependency_overrides[ResourceStatusDB.no_transaction]
    # factory.py stores partial(db_no_transaction, db_instance); args[0] is the instance.
    return db_override.args[0]


async def _clear_source_caches(client):
    """Clear the singleton sources' caches.

    The sources live for the whole test session while each test gets a fresh
    database, so any snapshot cached by a previous test must be dropped.
    """
    from diracx.core.config.sources import AsyncCacheableSource

    for override in client.app.dependency_overrides.values():
        source = getattr(override, "__self__", None)
        if isinstance(source, AsyncCacheableSource):
            await source.clear_caches()


async def _prepare_rss(client):
    """Reset the source caches and seed the database."""
    await _clear_source_caches(client)

    db = _get_rss_db(client)
    now = datetime.now(tz=timezone.utc)

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
            # A storage element belonging to another VO, which the test user
            # (vo=lhcb) must not see.
            await conn.insert_resource_status(
                name="SE-OTHER",
                status="Active",
                status_type=status_type,
                vo="other_vo",
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
        # A site visible to every VO.
        await conn.insert_site_status(
            name="LCG.Shared.ch",
            status="Active",
            vo="all",
            reason="All good",
            date_effective=now,
        )


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        # Run on the TestClient's portal so async primitives are bound to the
        # same event loop that serves the requests.
        client.portal.call(_prepare_rss, client)
        yield client


@pytest.fixture
def empty_db_client(client_factory):
    with client_factory.normal_user() as client:
        client.portal.call(_clear_source_caches, client)
        yield client


@pytest.fixture
def unauthenticated_client(client_factory):
    with client_factory.unauthenticated() as client:
        yield client


def test_unauthenticated(unauthenticated_client):
    response = unauthenticated_client.get("/api/rss/storage")
    assert response.status_code == HTTPStatus.UNAUTHORIZED


@pytest.mark.parametrize("endpoint", ALL_ENDPOINTS)
def test_get_resource_status(normal_user_client, endpoint):
    r = normal_user_client.get(endpoint)
    assert r.status_code == HTTPStatus.OK, r.json()
    assert r.json(), r.text

    last_modified = r.headers["Last-Modified"]
    etag = r.headers["ETag"]
    # The same URL serves different content per VO, so the ETag must identify
    # the VO and caches must be told the response varies with the caller.
    assert etag.endswith("-lhcb")
    assert "Authorization" in r.headers["Vary"]

    # Matching ETag + matching Last-Modified → 304
    r = normal_user_client.get(
        endpoint,
        headers={"If-None-Match": etag, "If-Modified-Since": last_modified},
    )
    assert r.status_code == HTTPStatus.NOT_MODIFIED, r.text
    assert not r.text

    # Wrong ETag only → 200
    r = normal_user_client.get(
        endpoint,
        headers={"If-None-Match": "wrongEtag"},
    )
    assert r.status_code == HTTPStatus.OK, r.json()
    assert r.json(), r.text

    # Past ETag + past timestamp → 200
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": "pastEtag",
            "If-Modified-Since": "Mon, 1 Apr 2000 00:42:42 GMT",
        },
    )
    assert r.status_code == HTTPStatus.OK, r.json()
    assert r.json(), r.text

    # Wrong ETag + future timestamp → 304 (If-Modified-Since takes effect)
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": "futureEtag",
            "If-Modified-Since": "Mon, 1 Apr 9999 00:42:42 GMT",
        },
    )
    assert r.status_code == HTTPStatus.NOT_MODIFIED, r.text
    assert not r.text

    # Wrong ETag + invalid timestamp → 200
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": "futureEtag",
            "If-Modified-Since": "wrong format",
        },
    )
    assert r.status_code == HTTPStatus.OK, r.json()
    assert r.json(), r.text

    # Correct ETag + past timestamp → 304 (ETag match takes priority)
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": "Mon, 1 Apr 2000 00:42:42 GMT",
        },
    )
    assert r.status_code == HTTPStatus.NOT_MODIFIED, r.text
    assert not r.text

    # Correct ETag + future timestamp → 304
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": "Mon, 1 Apr 9999 00:42:42 GMT",
        },
    )
    assert r.status_code == HTTPStatus.NOT_MODIFIED, r.text
    assert not r.text

    # Correct ETag + invalid timestamp → 304
    r = normal_user_client.get(
        endpoint,
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": "wrong format",
        },
    )
    assert r.status_code == HTTPStatus.NOT_MODIFIED, r.text
    assert not r.text


def test_vo_filtering(normal_user_client):
    """Users only see "all" entries plus those of their own VO."""
    r = normal_user_client.get("/api/rss/storage")
    assert r.status_code == HTTPStatus.OK, r.json()
    assert set(r.json()) == {"SE-CERN"}  # not SE-OTHER (vo=other_vo)

    r = normal_user_client.get("/api/rss/site")
    assert r.status_code == HTTPStatus.OK, r.json()
    assert set(r.json()) == {"LCG.CERN.cern", "LCG.Shared.ch"}


@pytest.mark.parametrize("endpoint", ALL_ENDPOINTS)
def test_empty_db(empty_db_client, endpoint):
    """An empty database yields an empty result with valid cache headers."""
    r = empty_db_client.get(endpoint)
    assert r.status_code == HTTPStatus.OK, r.text
    assert r.json() == {}
    assert r.headers["ETag"] == "empty-0-lhcb"

    # A conditional request against the sentinel revision still works
    r = empty_db_client.get(endpoint, headers={"If-None-Match": "empty-0-lhcb"})
    assert r.status_code == HTTPStatus.NOT_MODIFIED, r.text


def test_served_from_cache(normal_user_client, monkeypatch):
    """Once populated, requests are served from the cache without DB access."""
    from diracx.db.sql.rss.db import ResourceStatusDB

    # Populate the cache for every endpoint
    for endpoint in ALL_ENDPOINTS:
        r = normal_user_client.get(endpoint)
        assert r.status_code == HTTPStatus.OK, r.text

    # Break every read path of the DB to prove the cache is used
    async def _fail(*args, **kwargs):
        raise AssertionError("The database should not be accessed")

    for method in (
        "get_site_statuses",
        "get_resource_statuses",
        "get_resource_status_date",
        "get_site_status_date",
    ):
        monkeypatch.setattr(ResourceStatusDB, method, _fail)

    for endpoint in ALL_ENDPOINTS:
        r = normal_user_client.get(endpoint)
        assert r.status_code == HTTPStatus.OK, r.text
        assert r.json(), r.text
