from __future__ import annotations

import pytest
from sqlalchemy.exc import NoResultFound

from diracx.core.exceptions import AuthorizationError
from diracx.db.sql.auth.db import AuthDB
from diracx.db.sql.auth.schema import FlowStatus

MAX_VALIDITY = 2
EXPIRED = 0


@pytest.fixture
async def auth_db(tmp_path):
    auth_db = AuthDB("sqlite+aiosqlite:///:memory:")
    async with auth_db.engine_context():
        async with auth_db.engine.begin() as conn:
            await conn.run_sync(auth_db.metadata.create_all)
        yield auth_db


async def test_insert_id_token(auth_db: AuthDB):
    # First insert
    async with auth_db as auth_db:
        uuid = await auth_db.insert_authorization_flow(
            "client_id", "scope", "code_challenge", "S256", "redirect_uri"
        )

    id_token = {"sub": "myIdToken"}

    async with auth_db as auth_db:
        with pytest.raises(AuthorizationError):
            code, redirect_uri = await auth_db.authorization_flow_insert_id_token(
                uuid, id_token, EXPIRED
            )
        code, redirect_uri = await auth_db.authorization_flow_insert_id_token(
            uuid, id_token, MAX_VALIDITY
        )
        assert redirect_uri == "redirect_uri"

    # Cannot add a id_token a second time
    async with auth_db as auth_db:
        with pytest.raises(AuthorizationError):
            await auth_db.authorization_flow_insert_id_token(
                uuid, id_token, MAX_VALIDITY
            )

    async with auth_db as auth_db:
        with pytest.raises(NoResultFound):
            await auth_db.get_authorization_flow(code, EXPIRED)
        res = await auth_db.get_authorization_flow(code, MAX_VALIDITY)
        assert res["IDToken"] == id_token

    # Cannot add a id_token after finishing the flow
    async with auth_db as auth_db:
        with pytest.raises(AuthorizationError):
            await auth_db.authorization_flow_insert_id_token(
                uuid, id_token, MAX_VALIDITY
            )


async def test_insert(auth_db: AuthDB):
    # First insert
    async with auth_db as auth_db:
        uuid1 = await auth_db.insert_authorization_flow(
            "client_id", "scope", "code_challenge", "S256", "redirect_uri"
        )
        uuid2 = await auth_db.insert_authorization_flow(
            "client_id2",
            "scope2",
            "code_challenge2",
            "S256",
            "redirect_uri2",
        )

    assert uuid1 != uuid2


async def test_clean_authorization_flows(auth_db: AuthDB):
    # Insert two authorization flows
    async with auth_db as auth_db:
        uuid1 = await auth_db.insert_authorization_flow(
            "client_id", "scope", "code_challenge", "S256", "redirect_uri"
        )
        uuid2 = await auth_db.insert_authorization_flow(
            "client_id2", "scope2", "code_challenge2", "S256", "redirect_uri2"
        )

    id_token = {"sub": "myIdToken"}

    async with auth_db as auth_db:
        code1, _ = await auth_db.authorization_flow_insert_id_token(uuid1, id_token, 1)
        code2, _ = await auth_db.authorization_flow_insert_id_token(uuid2, id_token, 1)

    async with auth_db as auth_db:
        await auth_db.update_authorization_flow_status(code1, FlowStatus.DONE)
        await auth_db.update_authorization_flow_status(code2, FlowStatus.ERROR)

    # Check the number of deleted authorization flow (should be 0)
    async with auth_db as auth_db:
        deleted_auth = await auth_db.clean_expired_authorization_flows(max_retention=30)
        assert deleted_auth == 0

    # Check the number of deleted authorization flow (should be 2)
    async with auth_db as auth_db:
        deleted_auth = await auth_db.clean_expired_authorization_flows(max_retention=0)
        assert deleted_auth == 2

    # Check the number of deleted authorization flow (should be 0 because there is nothing left to delete)
    async with auth_db as auth_db:
        deleted_auth = await auth_db.clean_expired_authorization_flows(max_retention=0)
        assert deleted_auth == 0
