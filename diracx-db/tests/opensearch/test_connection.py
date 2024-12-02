from __future__ import annotations

import pytest

from diracx.db.os.utils import OpenSearchDBUnavailableError
from diracx.testing.osdb import OPENSEARCH_PORT, DummyOSDB, require_port_availability


async def _ensure_db_unavailable(db: DummyOSDB):
    """Helper function which raises an exception if we manage to connect to the DB."""
    async with db.client_context():
        async with db:
            with pytest.raises(OpenSearchDBUnavailableError):
                await db.ping()


async def test_connection(dummy_opensearch_db: DummyOSDB):
    """Ensure we can connect to the OpenSearch database."""
    assert await dummy_opensearch_db.client.ping()


async def test_connection_error_bad_port(opensearch_conn_kwargs):
    """Check the connection behavior when the DB is unavailable.

    This failure mode is emulated by changing the port number.
    """
    require_port_availability(28001)
    assert f":{OPENSEARCH_PORT}" in opensearch_conn_kwargs["hosts"]
    db = DummyOSDB(
        {
            **opensearch_conn_kwargs,
            "hosts": opensearch_conn_kwargs["hosts"].replace(
                f":{OPENSEARCH_PORT}", ":28001"
            ),
        }
    )
    await _ensure_db_unavailable(db)


async def test_connection_error_ssl(opensearch_conn_kwargs):
    """Check the connection behavior when there is an SSL error."""
    db = DummyOSDB({**opensearch_conn_kwargs, "use_ssl": False})
    await _ensure_db_unavailable(db)


async def test_connection_error_certs(opensearch_conn_kwargs):
    """Check the connection behavior when there is an certificate verification error."""
    db = DummyOSDB({**opensearch_conn_kwargs, "verify_certs": True})
    await _ensure_db_unavailable(db)


async def test_connection_error_bad_username(opensearch_conn_kwargs):
    """Check the connection behavior when the username is incorrect."""
    assert "admin:admin" in opensearch_conn_kwargs["hosts"]
    db = DummyOSDB(
        {
            **opensearch_conn_kwargs,
            "hosts": opensearch_conn_kwargs["hosts"].replace(
                "admin:admin", "nobody:admin"
            ),
        }
    )
    await _ensure_db_unavailable(db)


async def test_connection_error_bad_password(opensearch_conn_kwargs):
    """Check the connection behavior when the password is incorrect."""
    assert "admin:admin" in opensearch_conn_kwargs["hosts"]
    db = DummyOSDB(
        {
            **opensearch_conn_kwargs,
            "hosts": opensearch_conn_kwargs["hosts"].replace(
                "admin:admin", "admin:wrong"
            ),
        }
    )
    await _ensure_db_unavailable(db)


async def test_sanity_checks(opensearch_conn_kwargs):
    """Check that the sanity checks are working as expected."""
    db = DummyOSDB(opensearch_conn_kwargs)
    # Check that the client is not available before entering the context manager
    with pytest.raises(RuntimeError):
        await db.ping()

    # It shouldn't be possible to enter the context manager twice
    async with db.client_context():
        async with db:
            await db.ping()
