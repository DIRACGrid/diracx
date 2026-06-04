"""Helpers for HTTP conditional-request caching (ETag / Last-Modified / 304)."""

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
    """Set ETag / Last-Modified headers and raise 304 when appropriate.

    If If-None-Match matches the current ETag, return 304.

    If If-Modified-Since is given and is newer than the current Last-Modified,
    return 304: this is to avoid flip/flopping in case a server gets out of
    sync with the source of truth.

    Args:
        response: The response whose headers should be updated.
        etag: The current entity tag.
        modified: The current modification time (timezone-aware).
        if_none_match: Value of the If-None-Match request header, if any.
        if_modified_since: Value of the If-Modified-Since request header, if any.
        vary: Optional value for the Vary header, for responses whose content
              depends on more than the URL (e.g. the caller's identity).

    Raises:
        HTTPException(304): when the client's cached copy is still current.

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
