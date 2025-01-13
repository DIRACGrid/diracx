from __future__ import annotations

import pytest
from fastapi import status

pytestmark = pytest.mark.enabled_dependencies(
    ["AuthSettings", "ConfigSource", "OpenAccessPolicy"]
)


@pytest.fixture
def normal_user_client(client_factory):
    with client_factory.normal_user() as client:
        yield client


def test_unauthenticated(client_factory):
    with client_factory.unauthenticated() as client:
        response = client.get("/api/config/")
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_get_config(normal_user_client):
    r = normal_user_client.get("/api/config/")
    assert r.status_code == status.HTTP_200_OK, r.json()
    assert r.json(), r.text

    last_modified = r.headers["Last-Modified"]
    etag = r.headers["ETag"]

    r = normal_user_client.get(
        "/api/config/",
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": last_modified,
        },
    )

    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text

    # If only an invalid ETAG is passed, we expect a response
    r = normal_user_client.get(
        "/api/config/",
        headers={
            "If-None-Match": "wrongEtag",
        },
    )
    assert r.status_code == status.HTTP_200_OK, r.json()
    assert r.json(), r.text

    # If an past ETAG and an past timestamp as give, we expect an response
    r = normal_user_client.get(
        "/api/config/",
        headers={
            "If-None-Match": "pastEtag",
            "If-Modified-Since": "Mon, 1 Apr 2000 00:42:42 GMT",
        },
    )
    assert r.status_code == status.HTTP_200_OK, r.json()
    assert r.json(), r.text

    # If an future ETAG and an new timestamp as give, we expect 304
    r = normal_user_client.get(
        "/api/config/",
        headers={
            "If-None-Match": "futureEtag",
            "If-Modified-Since": "Mon, 1 Apr 9999 00:42:42 GMT",
        },
    )
    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text

    # If an invalid ETAG and an invalid modified time, we expect a response
    r = normal_user_client.get(
        "/api/config/",
        headers={
            "If-None-Match": "futureEtag",
            "If-Modified-Since": "wrong format",
        },
    )
    assert r.status_code == status.HTTP_200_OK, r.json()
    assert r.json(), r.text

    # If the correct ETAG and a past timestamp as give, we expect 304
    r = normal_user_client.get(
        "/api/config/",
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": "Mon, 1 Apr 2000 00:42:42 GMT",
        },
    )
    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text

    # If the correct ETAG and a new timestamp as give, we expect 304
    r = normal_user_client.get(
        "/api/config/",
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": "Mon, 1 Apr 9999 00:42:42 GMT",
        },
    )
    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text

    # If the correct ETAG and an invalid modified time, we expect 304
    r = normal_user_client.get(
        "/api/config/",
        headers={
            "If-None-Match": etag,
            "If-Modified-Since": "wrong format",
        },
    )
    assert r.status_code == status.HTTP_304_NOT_MODIFIED, r.text
    assert not r.text
