from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, bindparam, func
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.sql import delete, insert, select, update

from diracx.core.exceptions import (
    CredentialsAlreadyExistError,
    CredentialsNotFoundError,
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
    PilotToSecretMapping,
)


class PilotAgentsDB(BaseSQLDB):
    """PilotAgentsDB class is a front-end to the PilotAgents Database."""

    metadata = PilotAgentsDBBase.metadata

    async def increment_pilot_local_secret_and_last_time_use(
        self, pilot_stamp: str, pilot_secret_id: int
    ) -> None:
        """First, increment how many times a pilot used a secret. Then increment the global count."""
        #  Prepare the update statement
        stmt = (
            update(PilotToSecretMapping)
            .values(
                pilot_secret_use_count=PilotToSecretMapping.pilot_secret_use_count + 1,
                pilot_secret_last_use_time=utcnow(),
            )
            .where(PilotToSecretMapping.pilot_stamp == pilot_stamp)
            .where(PilotToSecretMapping.pilot_secret_id == pilot_secret_id)
        )

        # Execute the update using the connection
        res = await self.conn.execute(stmt)

        if res.rowcount == 0:
            await self.conn.rollback()
            raise PilotNotFoundError(
                data={
                    "pilot_stamp": pilot_stamp,
                    "pilot_secret_id": str(pilot_secret_id),
                }
            )

    async def increment_global_secret_use(
        self,
        secret_id: int,
    ) -> None:
        """Increment the global secret count."""
        #  Prepare the update statement
        stmt = (
            update(PilotSecrets)
            .values(secret_global_use_count=PilotSecrets.secret_global_use_count + 1)
            .where(PilotSecrets.secret_id == secret_id)
        )

        # Execute the update using the connection
        res = await self.conn.execute(stmt)

        if res.rowcount == 0:
            await self.conn.rollback()
            raise SecretNotFoundError(data={"secret_id": str(secret_id)})
        if res.rowcount != 1:
            await self.conn.rollback()
            raise DBInBadStateError(
                detail="This should not happen. Pilot should have a secret, but is not found."
            )

    async def add_pilots_bulk(
        self,
        pilot_stamps: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        pilot_references: dict | None = None,
    ) -> None:

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

    async def delete_secrets_bulk(self, secret_ids: list[int]):
        """Bulk delete secrets."""
        stmt = delete(PilotSecrets).where(PilotSecrets.secret_id.in_(secret_ids))

        res = await self.conn.execute(stmt)

        if res.rowcount != len(secret_ids):
            await self.conn.rollback()

            raise SecretNotFoundError(data={"secrets": str(secret_ids)})

        # To avoid raise condition
        await self.conn.commit()

    async def delete_pilots_by_stamps_bulk(self, pilot_stamps: list[str]):
        """Bulk delete pilots."""
        stmt = delete(PilotAgents).where(PilotAgents.pilot_stamp.in_(pilot_stamps))

        res = await self.conn.execute(stmt)

        if res.rowcount != len(pilot_stamps):
            await self.conn.rollback()

            raise PilotNotFoundError(data={"pilot_stamps": str(pilot_stamps)})

        # To avoid raise condition
        await self.conn.commit()

    async def insert_unique_secrets_bulk(
        self,
        hashed_secrets: list[str],
        vo: str | None,
        secret_global_use_count_max: int | None = 1,
    ):
        """Bulk insert secrets. Raises an error in case of a Integrity violation."""
        values = [
            {
                "SecretGlobalUseCountMax": secret_global_use_count_max,
                "HashedSecret": hashed_secret,
                "SecretVO": vo,
            }
            for hashed_secret in hashed_secrets
        ]

        stmt = insert(PilotSecrets).values(values)

        try:
            await self.conn.execute(stmt)
            await self.conn.commit()
        except IntegrityError as e:
            # In case of an error, undo insertion
            await self.conn.rollback()

            if "duplicate entry" in str(e.orig).lower():
                raise SecretAlreadyExistsError(
                    data={"hashed_secrets": str(hashed_secrets)},
                    detail="at least one of these secrets already exists",
                ) from e

            # Other errors to catch
            raise DBInBadStateError("Engine Specific error not caught" + str(e)) from e

    async def associate_pilots_with_secrets_bulk(
        self, pilot_to_secret_id_mapping_values: list[dict[str, Any]]
    ):
        """Bulk associate pilots with secrets. Raises an error in case of a Integrity violation."""
        # Better to give as a parameter pilot to secret associations, rather than associating here.

        stmt = insert(PilotToSecretMapping).values(pilot_to_secret_id_mapping_values)

        try:
            await self.conn.execute(stmt)
            await self.conn.commit()

        except (IntegrityError, OperationalError) as e:
            # Undo changes
            await self.conn.rollback()
            if "foreign key" in str(e.orig).lower():
                raise PilotNotFoundError(
                    data={"pilot_stamps": str(pilot_to_secret_id_mapping_values)},
                    detail="at least one of these pilots or secrets does not exist",
                ) from e
            if any(
                el in str(e.orig).lower()
                for el in ["duplicate entry", "unique constraint"]
            ):
                raise CredentialsAlreadyExistError(
                    data={"pilot_stamps": str(pilot_to_secret_id_mapping_values)},
                    detail="at least one of these pilots already have a secret",
                ) from e
            raise NotImplementedError(f"This error is not caught: {str(e.orig)}") from e

    async def associate_pilot_with_jobs(self, job_to_pilot_mapping: list[dict]):
        """Associate a pilot with jobs. Raises an error if the pilot does  not exist and in case of a IntegrityError.

        **Important note**: We don't verify if a job exists in the JobDB
        """
        # Insert multiple rows in a single execute call
        stmt = insert(JobToPilotMapping).values(job_to_pilot_mapping)

        try:
            res = await self.conn.execute(stmt)
        except IntegrityError as e:
            raise PilotAlreadyAssociatedWithJobError(
                data={"job_to_pilot_mapping": str(job_to_pilot_mapping)}
            ) from e

        if res.rowcount != len(job_to_pilot_mapping):
            # If doubles
            await self.conn.rollback()
            raise PilotJobsNotFoundError(
                data={"job_to_pilot_mapping": str(job_to_pilot_mapping)}
            )

    async def update_pilot_fields_bulk(
        self, pilot_stamps_to_fields_mapping: list[PilotFieldsMapping]
    ):
        """Bulk update pilots with a mapping.

        The mapping helps to update multiple fields at a time.
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
            await self.conn.rollback()

            raise PilotNotFoundError(
                data={"mapping": str(pilot_stamps_to_fields_mapping)}
            )

        await self.conn.commit()

    async def set_secret_expirations_bulk(
        self, secret_ids: list[int], pilot_secret_expiration_dates: list[DateTime]
    ):
        """Bulk set expiration dates to secrets."""
        values = [
            {"b_SecretID": secret_id, "SecretExpirationDate": pilot_secret}
            for secret_id, pilot_secret in zip(
                secret_ids, pilot_secret_expiration_dates
            )
        ]

        #  Prepare the update statement
        stmt = (
            update(PilotSecrets)
            .where(PilotSecrets.secret_id == bindparam("b_SecretID"))
            .values({"SecretExpirationDate": bindparam("SecretExpirationDate")})
        )

        await self.conn.execute(stmt, values)

    async def get_pilots_by_stamp_bulk(self, pilot_stamps: list[str]) -> list[dict]:
        """Bulk fetch pilots. Ensure all stamps are found, else raise an error."""
        return await fetch_records_bulk_or_raises(
            self.conn,
            PilotAgents,
            PilotNotFoundError,
            "pilot_stamp",
            "PilotStamp",
            pilot_stamps,
        )

    async def get_pilot_credentials_by_stamp(
        self, pilot_stamps: list[str]
    ) -> list[dict]:
        """Fetch pilot credentials. Ensure all stamps are found, else raise an error."""
        return await fetch_records_bulk_or_raises(
            self.conn,
            PilotToSecretMapping,
            CredentialsNotFoundError,
            "pilot_stamp",
            "PilotStamp",
            pilot_stamps,
            allow_more_than_one_result_per_input=True,
        )

    async def get_secrets_by_hashed_secrets_bulk(
        self, hashed_secrets: list[str]
    ) -> list[dict]:
        """Bulk fetch secrets. Ensure all secrets are found, else raise an error."""
        return await fetch_records_bulk_or_raises(
            self.conn,
            PilotSecrets,
            SecretNotFoundError,
            "hashed_secret",
            "HashedSecret",
            hashed_secrets,
            order_by=("secret_id", "asc"),
        )

    async def get_secrets_by_secret_ids_bulk(self, secret_ids: list[int]) -> list[dict]:
        """Bulk fetch secrets. Ensure all secrets are found, else raise an error."""
        return await fetch_records_bulk_or_raises(
            self.conn,
            PilotSecrets,
            SecretNotFoundError,
            "secret_id",
            "SecretID",
            secret_ids,
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
        """Get pilot ids. Ensure all pilots are found, else raise an error."""
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
        await self.conn.commit()

        return res.rowcount
