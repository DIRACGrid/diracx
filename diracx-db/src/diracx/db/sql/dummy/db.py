"""Dummy SQL DB used in tests and examples.

This module provides a minimal ``DummyDB`` implementation showing how to
implement small database helpers using the :class:`BaseSQLDB` conventions.
The implementation is intentionally tiny and used for documentation and
tests; it is not production-grade.
"""

from __future__ import annotations

from sqlalchemy import insert
from uuid_utils import UUID

from diracx.db.sql.utils import BaseSQLDB

from .schema import Base as DummyDBBase
from .schema import Cars, Owners


class DummyDB(BaseSQLDB):
    """Minimal database helper used for examples and tests.

    Attributes:
        metadata: SQLAlchemy metadata object required by :class:`BaseSQLDB`.
    """

    # This needs to be here for the BaseSQLDB to create the engine
    metadata = DummyDBBase.metadata

    async def summary(self, group_by, search) -> list[dict[str, str | int]]:
        """Return a summary of cars grouped by a column.

        This wraps the generic ``_summary`` helper from :class:`BaseSQLDB`.

        Args:
            group_by: Column name or expression to group the summary by.
            search: Search expression or mapping used to filter rows.

        Returns:
            A list of mappings with aggregated summary values.
        """
        return await self._summary(Cars, group_by, search)

    async def insert_owner(self, name: str) -> int:
        """Insert a new owner row.

        Args:
            name: Owner name to insert.

        Returns:
            The integer primary key of the inserted owner (``lastrowid``).
        """
        stmt = insert(Owners).values(name=name)
        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid

    async def insert_car(self, license_plate: UUID, model: str, owner_id: int) -> int:
        """Insert a new car row.

        Args:
            license_plate: Unique identifier for the vehicle.
            model: Car model string.
            owner_id: Primary key of the owner row.

        Returns:
            The integer primary key of the inserted car (``lastrowid``).
        """
        stmt = insert(Cars).values(
            license_plate=license_plate, model=model, owner_id=owner_id
        )

        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid
