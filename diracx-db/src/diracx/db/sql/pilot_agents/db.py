from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

from sqlalchemy import DateTime, RowMapping, bindparam, func
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.sql import delete, insert, select, update
from uuid_utils import uuid7

from diracx.core.exceptions import (
    InvalidQueryError,
    PilotAlreadyAssociatedWithJobError,
    PilotJobsNotFoundError,
    PilotNotFoundError,
    SecretAlreadyExistsError,
    SecretNotFoundError,
)
from diracx.core.models import PilotFieldsMapping, SearchSpec, SortSpec
from diracx.db.exceptions import DBInBadStateError

from ..utils import (
    BaseSQLDB,
    _get_columns,
    apply_search_filters,
    apply_sort_constraints,
    fetch_records_bulk_or_raises,
    utcnow,
)
from .schema import (
    JobToPilotMapping,
    PilotAgents,
    PilotAgentsDBBase,
    PilotSecrets,
)


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    async def update_pilot_secret_use_time(self, pilot_stamp: str) -> None:
        """Update when a pilot used a secret.

        Raises:
        - PilotNotFoundError if the pilot does not exist
        - DBInBadStateError if multiple pilots shares the same stamp

        """
        #  Prepare the update statement
        stmt = (
            update(PilotAgents)
            .values(
                pilot_secret_use_time=utcnow(),
            )
            .where(PilotAgents.pilot_stamp == pilot_stamp)
        )

        # Execute the update using the connection
        res = await self.conn.execute(stmt)

        if res.rowcount == 0:
            raise PilotNotFoundError(
                data={
                    "pilot_stamp": pilot_stamp,
                }
            )
        if res.rowcount != 1:
            raise DBInBadStateError(
                detail="This should not happen. Pilot should have a unique stamp, but multiple were found."
            )

    async def increment_global_secret_use(
        self,
        secret_uuid: str,
    ) -> None:
        """Increment the global secret count.

        Raises:
        - SecretNotFoundError if the secret does not exist
        - DBInBadStateError if we updated more that one secret

        """
        #  Prepare the update statement
        stmt = (
            update(PilotSecrets)
            .values(secret_global_use_count=PilotSecrets.secret_global_use_count + 1)
            .where(PilotSecrets.secret_uuid == secret_uuid)
        )

        # Execute the update using the connection
        res = await self.conn.execute(stmt)

        if res.rowcount == 0:
            raise SecretNotFoundError(data={"secret_uuid": secret_uuid})
        if res.rowcount != 1:
            raise DBInBadStateError(
                detail="This should not happen. Pilot should have a secret, but is not found."
            )

    async def add_pilots_bulk(
        self,
        pilot_stamps: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        pilot_references: dict | None = None,
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
                "SubmissionTime": now,
                "LastUpdateTime": now,
                "Status": "Submitted",
                "PilotStamp": stamp,
            }
            for stamp in pilot_stamps
        ]

        # Insert multiple rows in a single execute call and use 'returning' to get primary keys
        stmt = insert(PilotAgents).values(values)  # Assuming 'id' is the primary key

        await self.conn.execute(stmt)

    async def delete_secrets_bulk(self, secret_uuids: list[str]):
        """Bulk delete secrets.

        Raises SecretNotFoundError if one of the secret was not found.
        """
        stmt = delete(PilotSecrets).where(PilotSecrets.secret_uuid.in_(secret_uuids))

        res = await self.conn.execute(stmt)

        if res.rowcount != len(secret_uuids):
            raise SecretNotFoundError(data={"secrets": str(secret_uuids)})

        # We NEED to commit here, because we will raise an error after this function
        await self.conn.commit()

    async def delete_pilots_by_stamps_bulk(self, pilot_stamps: list[str]):
        """Bulk delete pilots.

        Raises SecretNotFoundError if one of the pilot was not found.
        """
        stmt = delete(PilotAgents).where(PilotAgents.pilot_stamp.in_(pilot_stamps))

        res = await self.conn.execute(stmt)

        if res.rowcount != len(pilot_stamps):
            raise PilotNotFoundError(data={"pilot_stamps": str(pilot_stamps)})

    async def insert_unique_secrets_bulk(
        self,
        hashed_secrets: list[str],
        vo: str | None,
        secret_global_use_count_max: int | None = 1,
    ):
        """Bulk insert secrets.

        Raises:
        - SecretAlreadyExists if the secret already exists
        - NotImplementedError if we have an IntegrityError not caught

        """
        values = [
            {
                "SecretUUID": str(uuid7()),
                "SecretGlobalUseCountMax": secret_global_use_count_max,
                "HashedSecret": hashed_secret,
                "SecretVO": vo,
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

    async def associate_pilots_with_secrets_bulk(
        self, pilot_to_secret_uuid_mapping_values: list[dict[str, Any]]
    ):
        """Bulk associate pilots with secrets.

        Raises:
        - PilotNotFoundError if one of the pilot does not exist
        - NotImplementedError if at least of the pilot

        """
        # Better to give as a parameter pilot to secret associations, rather than associating here.

        stmt = (
            update(PilotAgents)
            .where(PilotAgents.pilot_stamp == bindparam("b_PilotStamp"))
            .values({"PilotSecretUUID": bindparam("b_PilotSecretUUID")})
        )

        try:
            await self.conn.execute(stmt, pilot_to_secret_uuid_mapping_values)
        except (IntegrityError, OperationalError) as e:
            if "foreign key" in str(e.orig).lower():
                raise PilotNotFoundError(
                    data={"pilot_stamps": str(pilot_to_secret_uuid_mapping_values)},
                    detail="at least one of these pilots or secrets does not exist",
                ) from e
            raise NotImplementedError(f"This error is not caught: {str(e.orig)}") from e

    async def associate_pilot_with_jobs(self, job_to_pilot_mapping: list[dict]):
        """Associate a pilot with jobs.

        job_to_pilot_mapping format:
            ```py
            job_to_pilot_mapping = [
                {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
            ]
            ```

        Raises:
        - PilotNotFoundError if a pilot_id is not associated with a pilot.
        - PilotAlreadyAssociatedWithJobError if the pilot is already associated with a job.
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
                    detail="at least one of these pilots does not exist",
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

    async def update_pilot_fields_bulk(
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

    async def set_secret_expirations_bulk(
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

    async def get_pilots_by_stamp_bulk(
        self, pilot_stamps: list[str]
    ) -> Sequence[RowMapping]:
        """Bulk fetch pilots.

        Raises:
        - PilotNotFoundError if one of the stamp is not associated with a pilot.
        - DBInBadStateError if we find multiple pilots associated with the same stamp.

        """
        results = await fetch_records_bulk_or_raises(
            self.conn,
            PilotAgents,
            PilotNotFoundError,
            "pilot_stamp",
            "PilotStamp",
            pilot_stamps,
            allow_no_result=True,
        )

        # Custom handling, to see which pilot_stamp does not exist (if so, say which one)
        found_keys = {row["PilotStamp"] for row in results}
        missing = set(pilot_stamps) - found_keys

        if missing:
            raise PilotNotFoundError(
                data={"pilot_stamp": str(missing)},
                detail=str(missing),
                non_existing_pilots=missing,
            )

        return results

    async def get_secrets_by_hashed_secrets_bulk(
        self, hashed_secrets: list[str]
    ) -> Sequence[RowMapping]:
        """Bulk fetch secrets.

        Raises:
        - SecretNotFoundError if one of the hash is not associated with a secret.
        - DBInBadStateError if we find multiple secrets associated with the same secret.

        """
        return await fetch_records_bulk_or_raises(
            self.conn,
            PilotSecrets,
            SecretNotFoundError,
            "hashed_secret",
            "HashedSecret",
            hashed_secrets,
            order_by=("secret_uuid", "asc"),
        )

    async def get_secrets_by_secret_uuids_bulk(
        self, secret_uuids: list[str]
    ) -> Sequence[RowMapping]:
        """Bulk fetch secrets.

        Raises:
        - SecretNotFound if one of the secret_uuid is not associated with a secret.
        - DBInBadStateError if multiple secrets are associated with the same secret_uuid.

        """
        return await fetch_records_bulk_or_raises(
            self.conn,
            PilotSecrets,
            SecretNotFoundError,
            "secret_uuid",
            "SecretUUID",
            secret_uuids,
            order_by=("secret_uuid", "asc"),
        )

    async def get_pilot_jobs_ids_by_pilot_id(self, pilot_id: int) -> list[int]:
        """Fetch pilot jobs."""
        job_to_pilot_mapping = await fetch_records_bulk_or_raises(
            self.conn,
            JobToPilotMapping,
            PilotJobsNotFoundError,
            "pilot_id",
            "PilotID",
            [pilot_id],
            allow_more_than_one_result_per_input=True,
            allow_no_result=True,
        )

        return [mapping["JobID"] for mapping in job_to_pilot_mapping]

    async def get_pilot_ids_by_stamps(self, pilot_stamps: list[str]) -> list[int]:
        """Get pilot ids."""
        # This function is currently needed while we are relying on pilot_ids instead of pilot_stamps
        # (Ex: JobToPilotMapping)
        pilots = await self.get_pilots_by_stamp_bulk(pilot_stamps)

        return [pilot["PilotID"] for pilot in pilots]

    async def search(
        self,
        parameters: list[str] | None,
        search: list[SearchSpec],
        sorts: list[SortSpec],
        *,
        distinct: bool = False,
        per_page: int = 100,
        page: int | None = None,
    ) -> tuple[int, list[dict[Any, Any]]]:
        """Search for pilots in the database."""
        # TODO: Refactorize with the search function for jobs.
        # Find which columns to select
        columns = _get_columns(PilotAgents.__table__, parameters)

        stmt = select(*columns)

        stmt = apply_search_filters(
            PilotAgents.__table__.columns.__getitem__, stmt, search
        )
        stmt = apply_sort_constraints(
            PilotAgents.__table__.columns.__getitem__, stmt, sorts
        )

        if distinct:
            stmt = stmt.distinct()

        # Calculate total count before applying pagination
        total_count_subquery = stmt.alias()
        total_count_stmt = select(func.count()).select_from(total_count_subquery)
        total = (await self.conn.execute(total_count_stmt)).scalar_one()

        # Apply pagination
        if page is not None:
            if page < 1:
                raise InvalidQueryError("Page must be a positive integer")
            if per_page < 1:
                raise InvalidQueryError("Per page must be a positive integer")
            stmt = stmt.offset((page - 1) * per_page).limit(per_page)

        # Execute the query
        return total, [
            dict(row._mapping) async for row in (await self.conn.stream(stmt))
        ]

    async def clear_pilots_bulk(
        self, cutoff_date: datetime, delete_only_aborted: bool
    ) -> int:
        """Bulk delete pilots that have SubmissionTime before the 'cutoff_date'.
        Returns the number of deletion.
        """
        # TODO: Add test (Millisec?)
        stmt = delete(PilotAgents).where(PilotAgents.submission_time < cutoff_date)

        # If delete_only_aborted is True, add the condition for 'Status' being 'Aborted'
        if delete_only_aborted:
            stmt = stmt.where(PilotAgents.status == "Aborted")

        # Execute the statement
        res = await self.conn.execute(stmt)

        return res.rowcount
