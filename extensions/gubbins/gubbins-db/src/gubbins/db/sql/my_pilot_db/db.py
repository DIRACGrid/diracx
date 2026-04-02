from __future__ import annotations

from datetime import datetime, timezone

from diracx.db.sql.utils import BaseSQLDB
from sqlalchemy import func, insert, select, update

from .schema import Base as MyPilotDBBase
from .schema import MyComputeElements, MyPilotStatus, MyPilotSubmissions


# --8<-- [start:my_pilot_db_class_header]
class MyPilotDB(BaseSQLDB):
    """Database for managing pilot submissions to compute elements."""

    metadata = MyPilotDBBase.metadata
    # --8<-- [end:my_pilot_db_class_header]

    async def add_ce(
        self, name: str, capacity: int, success_rate: float, enabled: bool = True
    ) -> None:
        stmt = insert(MyComputeElements).values(
            name=name, capacity=capacity, success_rate=success_rate, enabled=enabled
        )
        await self.conn.execute(stmt)

    # --8<-- [start:my_pilot_db_get_available_ces]
    async def get_available_ces(self) -> list[dict]:
        active_counts = (
            select(
                MyPilotSubmissions.ce_name,
                func.count().label("active"),
            )
            .where(
                MyPilotSubmissions.status.in_(
                    [MyPilotStatus.SUBMITTED, MyPilotStatus.RUNNING]
                )
            )
            .group_by(MyPilotSubmissions.ce_name)
            .subquery()
        )

        active = func.coalesce(active_counts.c.active, 0)
        stmt = (
            select(
                MyComputeElements.name.label("name"),
                MyComputeElements.capacity.label("capacity"),
                MyComputeElements.success_rate.label("success_rate"),
                active.label("active_pilots"),
            )
            .outerjoin(
                active_counts,
                MyComputeElements.name == active_counts.c.ce_name,
            )
            .where(
                MyComputeElements.enabled.is_(True),
                MyComputeElements.capacity > active,
            )
        )
        result = await self.conn.execute(stmt)
        return [
            {
                "name": row.name,
                "capacity": row.capacity,
                "success_rate": row.success_rate,
                "available_slots": row.capacity - row.active_pilots,
            }
            for row in result
        ]

    # --8<-- [end:my_pilot_db_get_available_ces]

    async def submit_pilot(self, ce_name: str) -> int:
        stmt = insert(MyPilotSubmissions).values(
            ce_name=ce_name,
            status=MyPilotStatus.SUBMITTED,
        )
        result = await self.conn.execute(stmt)
        return result.lastrowid

    async def update_pilot_status(self, pilot_id: int, status: MyPilotStatus) -> None:
        stmt = (
            update(MyPilotSubmissions)
            .where(MyPilotSubmissions.pilot_id == pilot_id)
            .values(
                status=status,
                updated_at=datetime.now(tz=timezone.utc),
            )
        )
        await self.conn.execute(stmt)

    async def get_pilots_by_status(self, status: MyPilotStatus) -> list[dict]:
        stmt = select(
            MyPilotSubmissions.pilot_id.label("pilot_id"),
            MyPilotSubmissions.ce_name.label("ce_name"),
            MyPilotSubmissions.status.label("status"),
            MyPilotSubmissions.submitted_at.label("submitted_at"),
        ).where(MyPilotSubmissions.status == status)
        result = await self.conn.execute(stmt)
        return [
            {
                "pilot_id": row.pilot_id,
                "ce_name": row.ce_name,
                "status": row.status,
                "submitted_at": row.submitted_at,
            }
            for row in result
        ]

    async def get_ce_success_rate(self, ce_name: str) -> float:
        stmt = select(MyComputeElements.success_rate).where(
            MyComputeElements.name == ce_name
        )
        result = await self.conn.execute(stmt)
        row = result.one()
        return row[0]

    async def get_pilot_summary(self) -> dict[str, int]:
        stmt = select(
            MyPilotSubmissions.status.label("status"),
            func.count().label("total"),
        ).group_by(MyPilotSubmissions.status)
        result = await self.conn.execute(stmt)
        return {row.status: row.total for row in result}
