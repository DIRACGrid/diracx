from __future__ import annotations

from datetime import datetime, timezone
from os import urandom

from sqlalchemy import DateTime, insert, select, update
from sqlalchemy.exc import IntegrityError

from diracx.core.exceptions import (
    AuthorizationError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
)
from diracx.db.sql.utils.functions import hash

from ..utils import BaseSQLDB
from .schema import PilotAgents, PilotAgentsDBBase, PilotRegistrations


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    async def add_pilot_references(
        self,
        pilot_ref: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        pilot_stamps: dict | None = None,
    ) -> None:

        if pilot_stamps is None:
            pilot_stamps = {}

        now = datetime.now(tz=timezone.utc)

        # Prepare the list of dictionaries for bulk insertion
        values = [
            {
                "PilotJobReference": ref,
                "VO": vo,
                "GridType": grid_type,
                "SubmissionTime": now,
                "LastUpdateTime": now,
                "Status": "Submitted",
                "PilotStamp": pilot_stamps.get(ref, ""),
            }
            for ref in pilot_ref
        ]

        # Insert multiple rows in a single execute call
        stmt = insert(PilotAgents).values(values)
        await self.conn.execute(stmt)
        return

    async def increment_pilot_secret_use(
        self,
        pilot_id: int,
    ) -> None:

        #  Prepare the update statement
        stmt = (
            update(PilotRegistrations)
            .values(
                pilot_secret_use_count=PilotRegistrations.pilot_secret_use_count + 1
            )
            .where(PilotRegistrations.pilot_id == pilot_id)
        )

        # Execute the update using the connection
        res = await self.conn.execute(stmt)

        if res.rowcount == 0:
            raise PilotNotFoundError(pilot_id=pilot_id)

    async def verify_pilot_secret(self, pilot_id: int, pilot_secret: str) -> None:
        hashed_secret = hash(pilot_secret)

        stmt = (
            select(PilotRegistrations)
            .where(PilotRegistrations.pilot_hashed_secret == hashed_secret)
            .where(PilotRegistrations.pilot_id == pilot_id)
        )

        # Execute the request
        res = await self.conn.execute(stmt)

        result = res.fetchone()

        if result is None:
            raise AuthorizationError(detail="bad pilot_id / pilot_secret")

        # Increment the count
        await self.increment_pilot_secret_use(pilot_id=pilot_id)

    async def register_new_pilot(
        self,
        vo: str,
        initial_job_id: int = 0,
        current_job_id: int = 0,
        benchmark: float = 0.0,
        pilot_job_reference: str = "Unknown",
        pilot_stamp: str = "",
        status: str = "Unknown",
        status_reason: str = "Unknown",
        queue: str = "Unknown",
        grid_site: str = "Unknown",
        destination_site: str = "NotAssigned",
        grid_type: str = "LCG",
        submission_time: DateTime | None = None,  # ?
        last_update_time: DateTime | None = None,  # = now?
        accounting_sent: bool = False,
    ) -> int | None:
        stmt = insert(PilotAgents).values(
            initial_job_id=initial_job_id,
            current_job_id=current_job_id,
            pilot_job_reference=pilot_job_reference,
            pilot_stamp=pilot_stamp,
            destination_site=destination_site,
            queue=queue,
            grid_site=grid_site,
            vo=vo,
            grid_type=grid_type,
            benchmark=benchmark,
            submission_time=submission_time,
            last_update_time=last_update_time,
            status=status,
            status_reason=status_reason,
            accounting_sent=accounting_sent,
        )

        # Execute the request
        res = await self.conn.execute(stmt)

        new_pilot_id = res.inserted_primary_key

        # Returns the new pilot ID
        return int(new_pilot_id[0]) if new_pilot_id else None

    async def add_pilot_credentials(self, pilot_id: int) -> str:

        # Get a random string
        # Can be customized
        random_secret = urandom(30).hex()

        hashed_random_secret = hash(random_secret)

        stmt = insert(PilotRegistrations).values(
            pilot_id=pilot_id, pilot_hashed_secret=hashed_random_secret
        )

        try:
            await self.conn.execute(stmt)
        except IntegrityError as e:
            if "foreign key" in str(e.orig).lower():
                raise PilotNotFoundError(pilot_id=pilot_id) from e
            if "duplicate entry" in str(e.orig).lower():
                raise PilotAlreadyExistsError(
                    pilot_id=pilot_id, detail="this pilot has already credentials"
                ) from e

        return random_secret

    async def get_pilots(self):
        stmt = select(PilotRegistrations).with_for_update()
        result = await self.conn.execute(stmt)

        # Convert results into a dictionnary
        pilots = [dict(row._mapping) for row in result]

        return pilots

    async def get_pilot_by_id(self, pilot_id: int):
        stmt = (
            select(PilotAgents)
            .with_for_update()
            .where(PilotAgents.pilot_id == pilot_id)
        )

        return dict((await self.conn.execute(stmt)).one()._mapping)
