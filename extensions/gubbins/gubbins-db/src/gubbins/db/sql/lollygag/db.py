"""Lollygag DB of the Gubbins extension, an example of a DiracX SQL DB."""

from __future__ import annotations

from diracx.db.sql.utils import BaseSQLDB
from sqlalchemy import insert, select
from uuid_utils import UUID

from .schema import Base as LollygagDBBase
from .schema import Cars, Owners


class LollygagDB(BaseSQLDB):
    """This LollygagDB is just to illustrate some important aspects of writing DB classes in DiracX.

    It is mostly pure SQLAlchemy, with a few conventions.

    Document the secrets.
    """

    # This needs to be here for the BaseSQLDB to create the engine
    metadata = LollygagDBBase.metadata

    async def summary(self, group_by, search) -> list[dict[str, str | int]]:
        """Summarize the cars, grouped by the given parameters.

        Args:
            group_by: Parameters to group the cars by.
            search: Search criteria selecting the cars to summarize.

        Returns:
            One dict per group, with the grouped parameters and their count.
        """
        return await self._summary(Cars, group_by, search)

    async def insert_owner(self, name: str) -> int:
        """Insert an owner.

        Args:
            name: Name of the owner.

        Returns:
            The ID of the new owner.
        """
        stmt = insert(Owners).values(name=name)
        result = await self.conn.execute(stmt)
        return result.lastrowid

    async def get_owner(self) -> list[str]:
        """Get the names of all the owners.

        Returns:
            The owner names.
        """
        stmt = select(Owners.name)
        result = await self.conn.execute(stmt)
        return [row[0] for row in result]

    async def insert_car(self, license_plate: UUID, model: str, owner_id: int) -> int:
        """Insert a car.

        Args:
            license_plate: License plate of the car.
            model: Model of the car.
            owner_id: ID of the owner of the car.

        Returns:
            The ID of the new car.
        """
        stmt = insert(Cars).values(
            license_plate=license_plate, model=model, owner_id=owner_id
        )

        result = await self.conn.execute(stmt)
        return result.lastrowid

    async def get_car(self) -> list[str]:
        """Get the models of all the cars.

        Returns:
            The car models.
        """
        stmt = select(Cars.model)
        result = await self.conn.execute(stmt)
        return [row[0] for row in result]
