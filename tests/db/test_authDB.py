from __future__ import annotations

import secrets

from pytest import mark, raises
from pytest_asyncio import fixture
from sqlalchemy.exc import NoResultFound

from chrishackaton.db.auth.db import AuthDB


@fixture
async def auth_engine():
    await AuthDB.make_engine("sqlite+aiosqlite:///:memory:")

    yield


@mark.asyncio
async def test_device_user_code_collision(auth_engine: None, monkeypatch):
    monkeypatch.setattr(secrets, "choice", lambda _: "A")

    # First insert should work
    async with AuthDB() as auth_db:
        code, device = await auth_db.insert_device_flow(
            "client_id", "scope", "audience"
        )
        assert code == "A"
        assert device

    async with AuthDB() as auth_db:
        with raises(NotImplementedError, match="insert new device flow"):
            await auth_db.insert_device_flow("client_id", "scope", "audience")

    monkeypatch.setattr(secrets, "choice", lambda _: "B")

    async with AuthDB() as auth_db:
        code, device = await auth_db.insert_device_flow(
            "client_id", "scope", "audience"
        )
        assert code == "B"
        assert device


@mark.asyncio
async def test_device_flow_lookup(auth_engine: None, monkeypatch):
    async with AuthDB() as auth_db:
        with raises(AssertionError):
            await auth_db.get_device_flow()

    async with AuthDB() as auth_db:
        with raises(NoResultFound):
            await auth_db.get_device_flow(user_code="NotInserted")

    async with AuthDB() as auth_db:
        with raises(NoResultFound):
            await auth_db.get_device_flow(device_code="NotInserted")

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
        res1a = await auth_db.get_device_flow(user_code=user_code1)
        assert res1a["device_code"] == device_code1
        res1b = await auth_db.get_device_flow(device_code=device_code1)
        assert res1b["user_code"] == user_code1
        assert res1a == res1b

        res2a = await auth_db.get_device_flow(user_code=user_code2)
        assert res2a["device_code"] == device_code2
        res2b = await auth_db.get_device_flow(device_code=device_code2)
        assert res2b["user_code"] == user_code2
        assert res2a == res2b

    # async with AuthDB() as auth_db:
    #     with raises(NotImplementedError, match="insert new device flow"):
    #         await auth_db.insert_device_flow("client_id", "scope", "audience")

    # monkeypatch.setattr(secrets, "choice", lambda _: "B")

    # async with AuthDB() as auth_db:
    #     code, device = await auth_db.insert_device_flow(
    #         "client_id", "scope", "audience"
    #     )
    #     assert code == "B"
    #     assert device
