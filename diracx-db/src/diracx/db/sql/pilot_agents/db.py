from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError, NoResultFound

from diracx.core.exceptions import (
    AuthorizationError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
)

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
    ) -> Sequence:  # Return a list of primary keys

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

        # Insert multiple rows in a single execute call and use 'returning' to get primary keys
        stmt = (
            insert(PilotAgents).values(values).returning(PilotAgents.pilot_id)
        )  # Assuming 'id' is the primary key
        result = await self.conn.execute(stmt)

        # Use .scalars() and .all() to get the primary keys directly in a list
        primary_keys = (
            result.scalars().all()
        )  # This returns a flat list of primary keys

        return primary_keys

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

    async def verify_pilot_secret(
        self, pilot_job_reference: str, pilot_hashed_secret: str
    ) -> None:

        try:
            pilot = await self.get_pilot_by_reference(pilot_job_reference)
        except NoResultFound as e:
            raise PilotNotFoundError(pilot_ref=pilot_job_reference) from e

        pilot_id = pilot["PilotID"]

        stmt = (
            select(PilotRegistrations)
            .where(PilotRegistrations.pilot_hashed_secret == pilot_hashed_secret)
            .where(PilotRegistrations.pilot_id == pilot_id)
        )

        # Execute the request
        res = await self.conn.execute(stmt)

        result = res.fetchone()

        if result is None:
            raise AuthorizationError(detail="bad pilot_id / pilot_secret")

        # Increment the count
        await self.increment_pilot_secret_use(pilot_id=pilot_id)

    async def add_pilot_credentials(self, pilot_id: int, pilot_hashed_secret: str):

        stmt = insert(PilotRegistrations).values(
            pilot_id=pilot_id, pilot_hashed_secret=pilot_hashed_secret
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

    async def fetch_all_pilots(self):
        stmt = select(PilotRegistrations).with_for_update()
        result = await self.conn.execute(stmt)

        # Convert results into a dictionary
        pilots = [dict(row._mapping) for row in result]

        return pilots

    async def get_pilot_by_reference(self, pilot_ref: str):
        stmt = (
            select(PilotAgents)
            .with_for_update()
            .where(PilotAgents.pilot_job_reference == pilot_ref)
        )

        # We assume it is unique...
        return dict((await self.conn.execute(stmt)).one()._mapping)
