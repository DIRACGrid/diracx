from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, bindparam
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.sql import delete, insert, update
from uuid_utils import uuid7

from diracx.core.exceptions import (
    PilotAlreadyAssociatedWithJobError,
    PilotNotFoundError,
    SecretAlreadyExistsError,
    SecretNotFoundError,
)
from diracx.core.models import (
    PilotFieldsMapping,
    PilotSecretConstraints,
    PilotStatus,
    SearchSpec,
    SortSpec,
)
from diracx.db.sql.utils.functions import utcnow

from ..utils import (
    BaseSQLDB,
)
from .schema import (
    JobToPilotMapping,
    PilotAgents,
    PilotAgentsDBBase,
    PilotOutput,
    PilotSecrets,
)


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    # ----------------------------- Insert Functions -----------------------------

    async def add_pilots(
        self,
        pilot_stamps: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        grid_site: str = "Unknown",
        destination_site: str = "NotAssigned",
        pilot_references: dict[str, str] | None = None,
        status_reason: str = "Unknown",
    ):
        """Bulk add pilots in the DB.

        If we can't find a pilot_reference associated with a stamp, we take the stamp by default.
        """
        if pilot_references is None:
            pilot_references = {}

        now = datetime.now(tz=timezone.utc)

        # Prepare the list of dictionaries for bulk insertion
        values = [
            {
                "PilotJobReference": pilot_references.get(stamp, stamp),
                "VO": vo,
                "GridType": grid_type,
                "GridSite": grid_site,
                "DestinationSite": destination_site,
                "SubmissionTime": now,
                "LastUpdateTime": now,
                "Status": "Submitted",
                "StatusReason": status_reason,
                "PilotStamp": stamp,
            }
            for stamp in pilot_stamps
        ]

        # Insert multiple rows in a single execute call and use 'returning' to get primary keys
        stmt = insert(PilotAgents).values(values)  # Assuming 'id' is the primary key

        await self.conn.execute(stmt)

    async def add_jobs_to_pilot(self, job_to_pilot_mapping: list[dict[str, Any]]):
        """Associate a pilot with jobs.

        job_to_pilot_mapping format:
            ```py
            job_to_pilot_mapping = [
                {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
            ]
            ```

        Raises:
        - PilotNotFoundError if a pilot_id is not associated with a pilot.
        - PilotAlreadyAssociatedWithJobError if the pilot is already associated with one of the given jobs.
        - NotImplementedError if the integrity error is not caught.

        **Important note**: We assume that a job exists.

        """
        # Insert multiple rows in a single execute call
        stmt = insert(JobToPilotMapping).values(job_to_pilot_mapping)

        try:
            await self.conn.execute(stmt)
        except IntegrityError as e:
            if "foreign key" in str(e.orig).lower():
                raise PilotNotFoundError(
                    data={"pilot_stamps": str(job_to_pilot_mapping)},
                    detail="at least one of these pilots do not exist",
                ) from e

            if (
                "duplicate entry" in str(e.orig).lower()
                or "unique constraint" in str(e.orig).lower()
            ):
                raise PilotAlreadyAssociatedWithJobError(
                    data={"job_to_pilot_mapping": str(job_to_pilot_mapping)}
                ) from e

            # Other errors to catch
            raise NotImplementedError(
                "Engine Specific error not caught" + str(e)
            ) from e

    async def insert_unique_secrets(
        self,
        hashed_secrets: list[bytes],
        secret_global_use_count_max: int | None = 1,
        secret_constraints: dict[bytes, PilotSecretConstraints] = {},
    ):
        """Bulk insert secrets.

        Raises:
        - SecretAlreadyExists if the secret already exists
        - NotImplementedError if we have an IntegrityError not caught

        """
        values = [
            {
                "SecretUUID": str(uuid7()),
                "SecretRemainingUseCount": secret_global_use_count_max,
                "HashedSecret": hashed_secret,
                "SecretConstraints": secret_constraints.get(hashed_secret, {}),
            }
            for hashed_secret in hashed_secrets
        ]

        stmt = insert(PilotSecrets).values(values)

        try:
            await self.conn.execute(stmt)
        except IntegrityError as e:
            if (
                "duplicate entry" in str(e.orig).lower()
                or "unique constraint" in str(e.orig).lower()
            ):
                raise SecretAlreadyExistsError(
                    data={"hashed_secrets": str(hashed_secrets)},
                    detail="at least one of these secrets already exists",
                ) from e

            # Other errors to catch
            raise NotImplementedError(
                "Engine Specific error not caught" + str(e)
            ) from e

    # ----------------------------- Delete Functions -----------------------------

    async def delete_pilots(self, pilot_ids: list[int]):
        """Destructive function. Delete pilots."""
        stmt = delete(PilotAgents).where(PilotAgents.pilot_id.in_(pilot_ids))

        await self.conn.execute(stmt)

    async def remove_jobs_from_pilots(self, pilot_ids: list[int]):
        """Destructive function. De-associate jobs and pilots."""
        stmt = delete(JobToPilotMapping).where(
            JobToPilotMapping.pilot_id.in_(pilot_ids)
        )

        await self.conn.execute(stmt)

    async def delete_pilot_logs(self, pilot_ids: list[int]):
        """Destructive function. Remove logs from pilots."""
        stmt = delete(PilotOutput).where(PilotOutput.pilot_id.in_(pilot_ids))

        await self.conn.execute(stmt)

    async def delete_secrets(self, secret_uuids: list[str]):
        """Bulk delete secrets.

        Raises SecretNotFoundError if one of the secret was not found.
        """
        stmt = delete(PilotSecrets).where(PilotSecrets.secret_uuid.in_(secret_uuids))

        res = await self.conn.execute(stmt)

        if res.rowcount != len(secret_uuids):
            raise SecretNotFoundError(data={"secrets": str(secret_uuids)})

        # We NEED to commit here, because we will raise an error after this function
        await self.conn.commit()

    # ----------------------------- Update Functions -----------------------------

    async def update_pilot_fields(
        self, pilot_stamps_to_fields_mapping: list[PilotFieldsMapping]
    ):
        """Bulk update pilots with a mapping.

        pilot_stamps_to_fields_mapping format:
            ```py
            [
                {
                    "PilotStamp": pilot_stamp,
                    "BenchMark": bench_mark,
                    "StatusReason": pilot_reason,
                    "AccountingSent": accounting_sent,
                    "Status": status,
                    "CurrentJobID": current_job_id,
                    "Queue": queue,
                    ...
                }
            ]
            ```

        The mapping helps to update multiple fields at a time.

        Raises PilotNotFoundError if one of the pilots is not found.
        """
        stmt = (
            update(PilotAgents)
            .where(PilotAgents.pilot_stamp == bindparam("b_pilot_stamp"))
            .values(
                {
                    key: bindparam(key)
                    for key in pilot_stamps_to_fields_mapping[0]
                    .model_dump(exclude_none=True)
                    .keys()
                    if key != "PilotStamp"
                }
            )
        )

        values = [
            {
                **{"b_pilot_stamp": mapping.PilotStamp},
                **mapping.model_dump(exclude={"PilotStamp"}, exclude_none=True),
            }
            for mapping in pilot_stamps_to_fields_mapping
        ]

        res = await self.conn.execute(stmt, values)

        if res.rowcount != len(pilot_stamps_to_fields_mapping):
            raise PilotNotFoundError(
                data={"mapping": str(pilot_stamps_to_fields_mapping)}
            )

    async def update_pilot_secret_use_time(self, secret_uuid: str) -> None:
        """Updates when a pilot uses a secret.

        Raises PilotNotFoundError if the pilot does not exist

        """
        #  Prepare the update statement
        stmt = (
            update(PilotSecrets)
            .values(
                pilot_secret_use_date=utcnow(),
                secret_remaining_use_count=PilotSecrets.secret_remaining_use_count - 1,
            )
            .where(PilotSecrets.secret_uuid == secret_uuid)
        )

        # Execute the update using the connection
        res = await self.conn.execute(stmt)

        if res.rowcount == 0:
            raise PilotNotFoundError(
                data={
                    "secret_uuid": str(secret_uuid),
                }
            )

    async def update_pilot_secrets_constraints(
        self, hashed_secrets_to_pilot_stamps_mapping: list[dict[str, Any]]
    ):
        """Bulk associate pilots with secrets by updating theirs constraints.

        Important: We have to provide the updated constraints.

        Raises:
        - PilotNotFoundError if one of the pilot does not exist
        - NotImplementedError if at least of the pilot

        """
        # Better to give as a parameter pilot to secret associations, rather than associating here.

        stmt = (
            update(PilotSecrets)
            .where(PilotSecrets.hashed_secret == bindparam("PilotHashedSecret"))
            .values({"SecretConstraints": bindparam("PilotSecretConstraints")})
        )

        try:
            await self.conn.execute(stmt, hashed_secrets_to_pilot_stamps_mapping)
        except (IntegrityError, OperationalError) as e:
            if "foreign key" in str(e.orig).lower():
                raise SecretNotFoundError(
                    data={"mapping": str(hashed_secrets_to_pilot_stamps_mapping)},
                    detail="at least one of these secrets does not exist",
                ) from e
            raise NotImplementedError(f"This error is not caught: {str(e.orig)}") from e

    async def set_secret_expirations(
        self, secret_uuids: list[str], pilot_secret_expiration_dates: list[DateTime]
    ):
        """Bulk set expiration dates to secrets.

        Raises:
        - SecretNotFoundError if one of the secret_uuid is not associated with a secret.
        - NotImplementedError if a integrity error is not caught.
        -

        """
        values = [
            {"b_SecretUUID": secret_uuid, "SecretExpirationDate": pilot_secret}
            for secret_uuid, pilot_secret in zip(
                secret_uuids, pilot_secret_expiration_dates
            )
        ]

        #  Prepare the update statement
        stmt = (
            update(PilotSecrets)
            .where(PilotSecrets.secret_uuid == bindparam("b_SecretUUID"))
            .values({"SecretExpirationDate": bindparam("SecretExpirationDate")})
        )

        try:
            await self.conn.execute(stmt, values)
        except IntegrityError as e:
            if "foreign key" in str(e.orig).lower():
                raise SecretNotFoundError(
                    data={"secret_uuids": str(secret_uuids)},
                    detail="at least one of these secrets does not exist",
                ) from e
            raise NotImplementedError(f"This error is not caught: {str(e.orig)}") from e

    # ----------------------------- Search Functions -----------------------------

    async def search_pilots(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[Any, Any]]]:
        """Search for pilot information in the database."""
        return await self.search(
            model=PilotAgents,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )

    async def search_pilot_to_job_mapping(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[Any, Any]]]:
        """Search for jobs that are associated with pilots."""
        return await self.search(
            model=JobToPilotMapping,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )

    async def search_secrets(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[Any, Any]]]:
        """Search for secrets in the database."""
        return await self.search(
            model=PilotSecrets,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )
