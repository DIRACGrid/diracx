from __future__ import annotations

import secrets

import pytest
from pytest_asyncio import fixture
from sqlalchemy.exc import NoResultFound

from chrishackaton.db.auth.db import AuthDB
from chrishackaton.db.auth.schema import USER_CODE_LENGTH
from chrishackaton.exceptions import AuthorizationError


@fixture
async def auth_engine():
    await AuthDB.make_engine("sqlite+aiosqlite:///:memory:")

    yield


@pytest.mark.asyncio
async def test_device_user_code_collision(auth_engine: None, monkeypatch):
    monkeypatch.setattr(secrets, "choice", lambda _: "A")

    # First insert should work
    async with AuthDB() as auth_db:
        code, device = await auth_db.insert_device_flow(
            "client_id", "scope", "audience"
        )
        assert code == "A" * USER_CODE_LENGTH
        assert device

    async with AuthDB() as auth_db:
        with pytest.raises(NotImplementedError, match="insert new device flow"):
            await auth_db.insert_device_flow("client_id", "scope", "audience")

    monkeypatch.setattr(secrets, "choice", lambda _: "B")

    async with AuthDB() as auth_db:
        code, device = await auth_db.insert_device_flow(
            "client_id", "scope", "audience"
        )
        assert code == "B" * USER_CODE_LENGTH
        assert device


@pytest.mark.asyncio
async def test_device_flow_lookup(auth_engine: None, monkeypatch):
    async with AuthDB() as auth_db:
        with pytest.raises(NoResultFound):
            await auth_db.device_flow_validate_user_code("NotInserted")

    async with AuthDB() as auth_db:
        with pytest.raises(NoResultFound):
            await auth_db.get_device_flow("NotInserted")

    # First insert
    async with AuthDB() as auth_db:
        user_code1, device_code1 = await auth_db.insert_device_flow(
            "client_id1", "scope1", "audience1"
        )
        user_code2, device_code2 = await auth_db.insert_device_flow(
            "client_id2", "scope2", "audience2"
        )

        assert user_code1 != user_code2

    async with AuthDB() as auth_db:
        await auth_db.device_flow_validate_user_code(user_code1)

        # Cannot get it with device_code because no id_token
        with pytest.raises(AuthorizationError):
            await auth_db.get_device_flow(device_code1)

        await auth_db.device_flow_validate_user_code(user_code2)

        # Cannot get it with device_code because no id_token
        with pytest.raises(AuthorizationError):
            await auth_db.get_device_flow(device_code2)

    async with AuthDB() as auth_db:
        await auth_db.device_flow_insert_id_token(user_code1, {"token": "mytoken"})

        # We should not be able to insert a id_token a second time
        with pytest.raises(KeyError):
            await auth_db.device_flow_insert_id_token(user_code1, {"token": "mytoken2"})

        res = await auth_db.get_device_flow(device_code1)
        assert res["user_code"] == user_code1
        assert res["id_token"] == {"token": "mytoken"}

    # cannot get it a second time
    async with AuthDB() as auth_db:
        with pytest.raises(AuthorizationError):
            await auth_db.get_device_flow(device_code1)

    # Re-adding a token should not work after it's been minted
    async with AuthDB() as auth_db:
        with pytest.raises(KeyError):
            await auth_db.device_flow_insert_id_token(user_code1, {"token": "mytoken"})


@pytest.mark.asyncio
async def test_device_flow_insert_id_token(auth_engine: None):
    # First insert
    async with AuthDB() as auth_db:
        user_code, device_code = await auth_db.insert_device_flow(
            "client_id", "scope", "audience"
        )

    # Make sure it exists, and is Pending
    async with AuthDB() as auth_db:
        await auth_db.device_flow_validate_user_code(user_code)

    id_token = {"sub": "myIdToken"}

    async with AuthDB() as auth_db:
        await auth_db.device_flow_insert_id_token(user_code, id_token)

    # The user code has been invalidated
    async with AuthDB() as auth_db:
        with pytest.raises(NoResultFound):
            await auth_db.device_flow_validate_user_code(user_code)

    async with AuthDB() as auth_db:
        res = await auth_db.get_device_flow(device_code)
        assert res["id_token"] == id_token
