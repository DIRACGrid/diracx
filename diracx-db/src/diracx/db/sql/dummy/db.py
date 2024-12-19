from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, insert, select

from diracx.db.sql.utils import BaseSQLDB, apply_search_filters

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
        columns = [Cars.__table__.columns[x] for x in group_by]

        stmt = select(*columns, func.count(Cars.license_plate).label("count"))
        stmt = apply_search_filters(Cars.__table__.columns.__getitem__, stmt, search)
        stmt = stmt.group_by(*columns)

        # Execute the query
        return [
            dict(row._mapping)
            async for row in (await self.conn.stream(stmt))
            if row.count > 0  # type: ignore
        ]

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
