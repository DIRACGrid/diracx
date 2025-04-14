from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, asc, bindparam, insert, select, update
from sqlalchemy.exc import IntegrityError, NoResultFound

from diracx.core.exceptions import (
    AuthorizationError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
)

from ..utils import BaseSQLDB, rows_to_dicts, utcnow
from .schema import PilotAgents, PilotAgentsDBBase, PilotRegistrations


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    async def add_pilot_references(
        self,
        pilot_refs: list[str],
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
            for ref in pilot_refs
        ]

        # Insert multiple rows in a single execute call and use 'returning' to get primary keys
        stmt = insert(PilotAgents).values(values)  # Assuming 'id' is the primary key

        await self.conn.execute(stmt)

    async def increment_pilot_secret_and_last_time_use(
        self,
        pilot_id: int,
    ) -> None:

        #  Prepare the update statement
        stmt = (
            update(PilotRegistrations)
            .values(
                pilot_secret_use_count=PilotRegistrations.pilot_secret_use_count + 1,
                pilot_secret_last_use_time=utcnow(),
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
            pilots = await self.get_pilots_by_references_bulk([pilot_job_reference])
            assert len(pilots) == 1
        except NoResultFound as e:
            raise PilotNotFoundError(pilot_ref=pilot_job_reference) from e

        pilot_id = pilots[0]["PilotID"]

        stmt = (
            select(PilotRegistrations)
            .with_for_update()
            .where(PilotRegistrations.pilot_hashed_secret == pilot_hashed_secret)
            .where(PilotRegistrations.pilot_id == pilot_id)
            .where(
                PilotRegistrations.pilot_secret_expiration_date
                > datetime.now(tz=timezone.utc)
            )
            .where(
                PilotRegistrations.pilot_secret_use_count
                < PilotRegistrations.pilot_secret_use_count_max
            )
        )

        # Execute the request
        res = await self.conn.execute(stmt)

        result = res.fetchone()

        if result is None:
            raise AuthorizationError(
                detail="bad pilot_id / pilot_secret or secret has expired"
            )

        # Increment the count
        await self.increment_pilot_secret_and_last_time_use(pilot_id=pilot_id)

    async def add_pilots_credentials(
        self,
        pilot_ids: list[int],
        pilot_hashed_secrets: list[str],
        pilot_secret_use_count_max: int = 1,
    ) -> list[datetime]:

        if len(pilot_ids) != len(pilot_hashed_secrets):
            raise ValueError("Each pilot has to have a secret")

        values = [
            {
                "PilotID": pilot_id,
                "PilotHashedSecret": pilot_secret,
                "PilotSecretUseCountMax": pilot_secret_use_count_max,
            }
            for pilot_id, pilot_secret in zip(pilot_ids, pilot_hashed_secrets)
        ]

        stmt = insert(PilotRegistrations).values(values)

        try:
            await self.conn.execute(stmt)
            await self.conn.commit()
        except IntegrityError as e:
            # Undo changes
            await self.conn.rollback()

            if "foreign key" in str(e.orig).lower():
                raise PilotNotFoundError(pilot_id=pilot_ids) from e
            if "duplicate entry" in str(e.orig).lower():
                raise PilotAlreadyExistsError(
                    pilot_id=pilot_ids,
                    detail="at least one of these pilots already have a secret",
                ) from e

        added_creds = await self.get_pilots_credentials_by_id_bulk(pilot_ids)

        return [cred["PilotSecretCreationDate"] for cred in added_creds]

    async def set_pilot_credentials_expiration(
        self, pilot_ids: list[int], pilot_secret_expiration_dates: list[DateTime]
    ):
        values = [
            {"b_PilotID": pilot_id, "PilotSecretExpirationDate": pilot_secret}
            for pilot_id, pilot_secret in zip(pilot_ids, pilot_secret_expiration_dates)
        ]

        #  Prepare the update statement
        stmt = (
            update(PilotRegistrations)
            .where(PilotRegistrations.pilot_id == bindparam("b_PilotID"))
            .values(
                {"PilotSecretExpirationDate": bindparam("PilotSecretExpirationDate")}
            )
        )

        await self.conn.execute(stmt, values)

    async def get_pilots_by_references_bulk(self, refs: list[str]) -> list[dict]:
        """Bulk fetch pilots. Ensure all refs are found, else raise error."""
        stmt = (
            select(PilotAgents)
            .where(PilotAgents.pilot_job_reference.in_(refs))
            .order_by(asc(PilotAgents.pilot_id))
        )
        results = rows_to_dicts(await self.conn.execute(stmt))

        # Build a map to verify all refs are found
        result_map = {pilot["PilotJobReference"]: pilot for pilot in results}
        missing = set(refs) - result_map.keys()

        if missing:
            raise PilotNotFoundError(pilot_ref=missing, detail=str(missing))

        return results

    async def get_pilots_credentials_by_id_bulk(self, ids: list[int]) -> list[dict]:
        """Bulk fetch pilots. Ensure all refs are found, else raise error."""
        stmt = (
            select(PilotRegistrations)
            .where(PilotRegistrations.pilot_id.in_(ids))
            .order_by(asc(PilotRegistrations.pilot_id))
        )
        results = rows_to_dicts(await self.conn.execute(stmt))

        # Build a map to verify all refs are found
        result_map = {pilot["PilotID"]: pilot for pilot in results}
        missing = set(ids) - result_map.keys()

        if missing:
            raise PilotNotFoundError(pilot_id=set(missing), detail=str(missing))

        return results
