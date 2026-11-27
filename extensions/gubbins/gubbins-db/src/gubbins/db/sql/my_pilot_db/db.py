"""Example database managing pilot submissions to compute elements."""

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
        """Add a compute element.

        Args:
            name: Name of the compute element.
            capacity: Maximum number of active pilots.
            success_rate: Reliability of the compute element, as the
                probability (0.0 to 1.0) that a pilot succeeds.
            enabled: Whether pilots can be submitted to the compute element.
        """
        stmt = insert(MyComputeElements).values(
            name=name, capacity=capacity, success_rate=success_rate, enabled=enabled
        )
        await self.conn.execute(stmt)

    # --8<-- [start:my_pilot_db_get_available_ces]
    async def get_available_ces(self) -> list[dict]:
        """List the enabled compute elements with free capacity.

        Returns:
            For each compute element, its name, capacity, success rate and
            number of available slots (capacity minus submitted and running
            pilots).
        """
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
        """Record the submission of a pilot to a compute element.

        Args:
            ce_name: Name of the compute element.

        Returns:
            The ID of the new pilot submission.
        """
        stmt = insert(MyPilotSubmissions).values(
            ce_name=ce_name,
            status=MyPilotStatus.SUBMITTED,
        )
        result = await self.conn.execute(stmt)
        return result.lastrowid

    async def update_pilot_status(self, pilot_id: int, status: MyPilotStatus) -> None:
        """Set the status of a pilot and record the time of the update.

        Args:
            pilot_id: ID of the pilot submission.
            status: New status of the pilot.
        """
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
        """List the pilots having a given status.

        Args:
            status: Status to select.

        Returns:
            For each pilot, its ID, compute element name, status and
            submission time.
        """
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
        """Get the success rate of a compute element.

        Args:
            ce_name: Name of the compute element.

        Returns:
            The success rate of the compute element.
        """
        stmt = select(MyComputeElements.success_rate).where(
            MyComputeElements.name == ce_name
        )
        result = await self.conn.execute(stmt)
        row = result.one()
        return row[0]

    async def get_pilot_summary(self) -> dict[str, int]:
        """Count the pilots by status.

        Returns:
            The number of pilots for each status.
        """
        stmt = select(
            MyPilotSubmissions.status.label("status"),
            func.count().label("total"),
        ).group_by(MyPilotSubmissions.status)
        result = await self.conn.execute(stmt)
        return {row.status: row.total for row in result}
