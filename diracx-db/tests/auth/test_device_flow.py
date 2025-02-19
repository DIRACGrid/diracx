from __future__ import annotations

import secrets
from datetime import timezone

import pytest
from sqlalchemy.exc import NoResultFound

from diracx.core.exceptions import AuthorizationError
from diracx.db.sql.auth.db import AuthDB
from diracx.db.sql.auth.schema import USER_CODE_LENGTH
from diracx.db.sql.utils.functions import substract_date

MAX_VALIDITY = 2
EXPIRED = 0


@pytest.fixture
async def auth_db(tmp_path):
    auth_db = AuthDB("sqlite+aiosqlite:///:memory:")
    async with auth_db.engine_context():
        async with auth_db.engine.begin() as conn:
            await conn.run_sync(auth_db.metadata.create_all)
        yield auth_db


async def test_device_user_code_collision(auth_db: AuthDB, monkeypatch):
    monkeypatch.setattr(secrets, "choice", lambda _: "A")

    # First insert should work
    async with auth_db as auth_db:
        code, device = await auth_db.insert_device_flow(
            "client_id",
            "scope",
        )
        assert code == "A" * USER_CODE_LENGTH
        assert device

    async with auth_db as auth_db:
        with pytest.raises(NotImplementedError, match="insert new device flow"):
            await auth_db.insert_device_flow("client_id", "scope")

    monkeypatch.setattr(secrets, "choice", lambda _: "B")

    async with auth_db as auth_db:
        code, device = await auth_db.insert_device_flow(
            "client_id",
            "scope",
        )
        assert code == "B" * USER_CODE_LENGTH
        assert device


async def test_device_flow_lookup(auth_db: AuthDB, monkeypatch):
    async with auth_db as auth_db:
        with pytest.raises(NoResultFound):
            await auth_db.device_flow_validate_user_code("NotInserted", MAX_VALIDITY)

    async with auth_db as auth_db:
        with pytest.raises(NoResultFound):
            await auth_db.get_device_flow("NotInserted")

    # First insert
    async with auth_db as auth_db:
        user_code1, device_code1 = await auth_db.insert_device_flow(
            "client_id1",
            "scope1",
        )
        user_code2, device_code2 = await auth_db.insert_device_flow(
            "client_id2",
            "scope2",
        )

        assert user_code1 != user_code2

    async with auth_db as auth_db:
        with pytest.raises(NoResultFound):
            await auth_db.device_flow_validate_user_code(user_code1, EXPIRED)

        await auth_db.device_flow_validate_user_code(user_code1, MAX_VALIDITY)
        await auth_db.device_flow_validate_user_code(user_code2, MAX_VALIDITY)

    async with auth_db as auth_db:
        with pytest.raises(AuthorizationError):
            await auth_db.device_flow_insert_id_token(
                user_code1, {"token": "mytoken"}, EXPIRED
            )

        await auth_db.device_flow_insert_id_token(
            user_code1, {"token": "mytoken"}, MAX_VALIDITY
        )

        # We should not be able to insert a id_token a second time
        with pytest.raises(AuthorizationError):
            await auth_db.device_flow_insert_id_token(
                user_code1, {"token": "mytoken2"}, MAX_VALIDITY
            )

        res = await auth_db.get_device_flow(device_code1)
        # The device code should be expired
        assert res["CreationTime"].replace(tzinfo=timezone.utc) > substract_date(
            seconds=MAX_VALIDITY
        )

        res = await auth_db.get_device_flow(device_code1)
        assert res["UserCode"] == user_code1
        assert res["IDToken"] == {"token": "mytoken"}

    # Re-adding a token should not work after it's been minted
    async with auth_db as auth_db:
        with pytest.raises(AuthorizationError):
            await auth_db.device_flow_insert_id_token(
                user_code1, {"token": "mytoken"}, MAX_VALIDITY
            )


async def test_device_flow_insert_id_token(auth_db: AuthDB):
    # First insert
    async with auth_db as auth_db:
        user_code, device_code = await auth_db.insert_device_flow(
            "client_id",
            "scope",
        )

    # Make sure it exists, and is Pending
    async with auth_db as auth_db:
        await auth_db.device_flow_validate_user_code(user_code, MAX_VALIDITY)

    id_token = {"sub": "myIdToken"}

    async with auth_db as auth_db:
        await auth_db.device_flow_insert_id_token(user_code, id_token, MAX_VALIDITY)

    # The user code has been invalidated
    async with auth_db as auth_db:
        with pytest.raises(NoResultFound):
            await auth_db.device_flow_validate_user_code(user_code, MAX_VALIDITY)

    async with auth_db as auth_db:
        res = await auth_db.get_device_flow(device_code)
        assert res["IDToken"] == id_token
