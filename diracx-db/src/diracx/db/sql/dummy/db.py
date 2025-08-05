from __future__ import annotations

from sqlalchemy import insert
from uuid_utils import UUID

from diracx.db.sql.utils import BaseSQLDB

from .schema import Base as DummyDBBase
from .schema import Cars, Owners


class DummyDB(BaseSQLDB):
    """This DummyDB is just to illustrate some important aspect of writing
    DB classes in DiracX.

    It is mostly pure SQLAlchemy, with a few convention

    Document the secrets
    """

    # This needs to be here for the BaseSQLDB to create the engine
    metadata = DummyDBBase.metadata

    async def summary(self, group_by, search) -> list[dict[str, str | int]]:
        return await self._summary(Cars, group_by, search)

    async def insert_owner(self, name: str) -> int:
        stmt = insert(Owners).values(name=name)
        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid

    async def insert_car(self, license_plate: UUID, model: str, owner_id: int) -> int:
        stmt = insert(Cars).values(
            license_plate=license_plate, model=model, owner_id=owner_id
        )

        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid
