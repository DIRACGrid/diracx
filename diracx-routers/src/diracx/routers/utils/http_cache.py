"""Provide helpers for HTTP conditional-request caching.

These utilities set ETag and Last-Modified response headers and handle
conditional requests whose cached representations are still current.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from http import HTTPStatus

from fastapi import HTTPException, Response

logger = logging.getLogger(__name__)

LAST_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"


def apply_cache_headers(
    response: Response,
    *,
    etag: str,
    modified: datetime,
    if_none_match: str | None,
    if_modified_since: str | None,
    vary: str | None = None,
) -> None:
    """Set cache headers or raise an HTTP 304 for a current cached representation.

    A matching ``If-None-Match`` value raises ``HTTPException`` with status
    304. A valid ``If-Modified-Since`` value later than the current
    modification time raises the same exception, avoiding cache flip-flopping
    when a server is out of sync with the source of truth. Invalid date headers
    are ignored. If neither condition applies, the cache headers are added to
    the response.

    Args:
        response: Response whose cache headers are updated when no 304 is raised.
        etag: Entity tag identifying the current representation.
        modified: Time the representation was last modified; must be timezone-aware.
        if_none_match: Optional value of the request's ``If-None-Match`` header.
        if_modified_since: Optional value of the request's ``If-Modified-Since`` header.
        vary: Optional ``Vary`` header value when the response depends on more
            than the URL, such as the caller's identity.

    Raises:
        HTTPException: With status 304 when the client's cached representation
            is still current.
    """
    headers = {
        "ETag": etag,
        "Last-Modified": modified.strftime(LAST_MODIFIED_FORMAT),
    }
    if vary is not None:
        headers["Vary"] = vary

    if if_none_match == etag:
        raise HTTPException(status_code=HTTPStatus.NOT_MODIFIED, headers=headers)

    if if_modified_since:
        try:
            # The If-Modified-Since header is always GMT (RFC 9110)
            not_before = datetime.strptime(
                if_modified_since, LAST_MODIFIED_FORMAT
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            logger.debug(
                "Failed to parse If-Modified-Since header: %s", if_modified_since
            )
        else:
            if not_before > modified:
                raise HTTPException(
                    status_code=HTTPStatus.NOT_MODIFIED, headers=headers
                )

    response.headers.update(headers)
