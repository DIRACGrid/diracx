"""Example SQL database implementation used to demonstrate DiracX conventions."""

from __future__ import annotations

from sqlalchemy import insert
from uuid_utils import UUID

from diracx.core.models.search import SearchSpec
from diracx.db.sql.utils import BaseSQLDB

from .schema import Base as DummyDBBase
from .schema import Cars, Owners


class DummyDB(BaseSQLDB):
    """Illustrate some important aspect of writing DB classes in DiracX.

    It is mostly pure SQLAlchemy, with a few DiracX conventions.

    Attributes:
        metadata: SQLAlchemy metadata for the dummy database tables.
    """

    # This needs to be here for the BaseSQLDB to create the engine
    metadata = DummyDBBase.metadata

    async def summary(
        self, group_by: list[str], search: list[SearchSpec]
    ) -> list[dict[str, str | int]]:
        """Get a summary of the cars.

        Args:
            group_by: Car fields used to group the summary.
            search: Search conditions to apply before summarizing.

        Returns:
            Summary rows containing grouped values and counts.
        """
        return await self._summary(table=Cars, group_by=group_by, search=search)

    async def insert_owner(self, name: str) -> int:
        """Insert a car owner.

        Args:
            name: Owner's name.

        Returns:
            Database identifier assigned to the new owner.
        """
        stmt = insert(Owners).values(name=name)
        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid

    async def insert_car(self, license_plate: UUID, model: str, owner_id: int) -> int:
        """Insert a car associated with an owner.

        Args:
            license_plate: Unique license plate identifier for the car.
            model: Car model name.
            owner_id: Database identifier of the car's owner.

        Returns:
            Database identifier assigned to the new car.
        """
        stmt = insert(Cars).values(
            license_plate=license_plate, model=model, owner_id=owner_id
        )

        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid
