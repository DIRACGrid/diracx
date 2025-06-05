from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError
from uuid_utils import uuid7

from diracx.db.exceptions import DBInBadStateError
from diracx.db.sql.auth.db import AuthDB
from diracx.db.sql.utils import DBStateAssertion


@pytest.fixture
async def auth_db(tmp_path):
    auth_db = AuthDB("sqlite+aiosqlite:///:memory:")
    async with auth_db.engine_context():
        async with auth_db.engine.begin() as conn:
            await conn.run_sync(auth_db.metadata.create_all)
        yield auth_db


async def test_context_manager(auth_db: AuthDB):
    """We will test with refresh tokens the context manager.

    1. Insert a refresh token in the DB with a proper jti
    2. Insert a refresh token with another jti
    3. Insert a refresh token with the first jti, except the right error /!\
    4. Insert a refresh token with the first jti, except the wrong error /!\

    1. and 2. should pass
    3. and 4. should raise DBInBadStateError

    3. Saying that the expected error was raised.
    4. Saying that an unexpected error was raised.
    """
    # Insert a refresh token details
    async with auth_db as auth_db:
        jti = uuid7()
        await auth_db.insert_refresh_token(
            jti,
            "subject",
            "scope",
        )

    # Revoke the token
    async with auth_db as auth_db:

        # No error should be raised
        async with DBStateAssertion(auth_db.conn, [IntegrityError]):
            await auth_db.insert_refresh_token(
                uuid7(),
                "subject",
                "scope",
            )

        # Await the right error (IntegrityError)
        with pytest.raises(DBInBadStateError) as exc_info:
            # DBInBadStateError because we say that IntegrityError should not be raised
            async with DBStateAssertion(auth_db.conn, [IntegrityError]):
                await auth_db.insert_refresh_token(
                    jti,
                    "subject",
                    "scope",
                )

        assert (
            str(exc_info.value)
            == "This error may NOT have been raised. Please report this at https://github.com/DIRACGrid/diracx/issues"
        )

        # Await the wrong error
        with pytest.raises(DBInBadStateError) as exc_info:
            # DBInBadStateError because we say that IntegrityError should not be raised
            async with DBStateAssertion(auth_db.conn, [ValueError]):
                await auth_db.insert_refresh_token(
                    jti,
                    "subject",
                    "scope",
                )

        # (We use "in" because the error details can change depending on the engine)
        assert str(exc_info.value).startswith("Unexpected error (IntegrityError):")
        assert "IntegrityError" in str(exc_info)
