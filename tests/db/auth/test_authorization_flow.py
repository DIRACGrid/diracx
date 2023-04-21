from __future__ import annotations

import pytest
from pytest_asyncio import fixture
from sqlalchemy.exc import NoResultFound

from chrishackaton.db.auth.db import AuthDB
from chrishackaton.exceptions import AuthorizationError

MAX_VALIDITY = 2
EXPIRED = 0


@fixture
async def auth_engine():
    await AuthDB.make_engine("sqlite+aiosqlite:///:memory:")

    yield


@pytest.mark.asyncio
async def test_insert_id_token(auth_engine: None):
    # First insert
    async with AuthDB() as auth_db:
        uuid = await auth_db.insert_authorization_flow(
            "client_id", "scope", "audience", "code_challenge", "S256", "redirect_uri"
        )

    id_token = {"sub": "myIdToken"}

    async with AuthDB() as auth_db:
        with pytest.raises(AuthorizationError):
            code, redirect_uri = await auth_db.authorization_flow_insert_id_token(
                uuid, id_token, EXPIRED
            )
        code, redirect_uri = await auth_db.authorization_flow_insert_id_token(
            uuid, id_token, MAX_VALIDITY
        )
        assert redirect_uri == "redirect_uri"

    # Cannot add a id_token a second time
    async with AuthDB() as auth_db:
        with pytest.raises(AuthorizationError):
            await auth_db.authorization_flow_insert_id_token(
                uuid, id_token, MAX_VALIDITY
            )

    async with AuthDB() as auth_db:
        with pytest.raises(NoResultFound):
            await auth_db.get_authorization_flow(code, EXPIRED)
        res = await auth_db.get_authorization_flow(code, MAX_VALIDITY)
        assert res["id_token"] == id_token

    # Cannot add a id_token after finishing the flow
    async with AuthDB() as auth_db:
        with pytest.raises(AuthorizationError):
            await auth_db.authorization_flow_insert_id_token(
                uuid, id_token, MAX_VALIDITY
            )

    # We shouldn't be able to retrieve it twice
    async with AuthDB() as auth_db:
        with pytest.raises(AuthorizationError, match="already used"):
            res = await auth_db.get_authorization_flow(code, MAX_VALIDITY)


@pytest.mark.asyncio
async def test_insert(auth_engine: None):
    # First insert
    async with AuthDB() as auth_db:
        uuid1 = await auth_db.insert_authorization_flow(
            "client_id", "scope", "audience", "code_challenge", "S256", "redirect_uri"
        )
        uuid2 = await auth_db.insert_authorization_flow(
            "client_id2",
            "scope2",
            "audience2",
            "code_challenge2",
            "S256",
            "redirect_uri2",
        )

    assert uuid1 != uuid2
