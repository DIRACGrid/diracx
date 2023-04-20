from __future__ import annotations

import secrets

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError

from ..utils import BaseDB
from .schema import Base as AuthDBBase
from .schema import DeviceFlows

# https://datatracker.ietf.org/doc/html/rfc8628#section-6.1
USER_CODE_ALPHABET = "BCDFGHJKLMNPQRSTVWXZ"
MAX_RETRY = 5


class AuthDB(BaseDB):
    # This needs to be here for the BaseDB to create the engine
    metadata = AuthDBBase.metadata

    async def get_device_flow(
        self, *, user_code: str | None = None, device_code: str | None = None
    ):
        assert user_code or device_code

        stmt = select(DeviceFlows)

        if user_code:
            stmt = stmt.where(DeviceFlows.user_code == user_code)
        if device_code:
            stmt = stmt.where(DeviceFlows.device_code == device_code)
        return (await self.conn.execute(stmt)).one()._mapping

    async def device_flow_insert_id_token(
        self, user_code: str, id_token: dict[str, str]
    ):
        stmt = update(DeviceFlows)
        stmt = stmt.where(DeviceFlows.user_code == user_code)
        stmt = stmt.values(id_token=id_token)
        await self.conn.execute(stmt)

    async def insert_device_flow(
        self,
        client_id: str,
        scope: str,
        audience: str,
    ):
        for _ in range(MAX_RETRY):
            user_code = "".join(
                secrets.choice(USER_CODE_ALPHABET)
                for _ in range(DeviceFlows.user_code.type.length)
            )
            # user_code = "2QRKPY"
            device_code = secrets.token_urlsafe()
            stmt = insert(DeviceFlows).values(
                client_id=client_id,
                scope=scope,
                audience=audience,
                user_code=user_code,
                device_code=device_code,
            )
            try:
                await self.conn.execute(stmt)

            except IntegrityError:
                continue

            return user_code, device_code
        raise NotImplementedError(
            f"Could not insert new device flow after {MAX_RETRY} retries"
        )


async def get_auth_db():
    async with AuthDB() as db:
        yield db
