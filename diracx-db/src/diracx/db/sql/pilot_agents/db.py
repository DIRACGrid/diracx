from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, bindparam, func
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.sql import delete, insert, select, update

from diracx.core.exceptions import (
    BadPilotCredentialsError,
    BadPilotVOError,
    CredentialsAlreadyExistError,
    CredentialsNotFoundError,
    InvalidQueryError,
    PilotAlreadyAssociatedWithJobError,
    PilotJobsNotFoundError,
    PilotNotFoundError,
    SecretAlreadyExistsError,
    SecretHasExpiredError,
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

    async def verify_pilot_secret(
        self, pilot_stamp: str, pilot_hashed_secret: str
    ) -> None:
        """Verify that a pilot can login with the given credentials."""
        # 1. Get the pilot to secret association
        pilots_credentials = await self.get_pilot_credentials_by_stamp([pilot_stamp])

        # 2. Get the pilot secret itself
        secrets = await self.get_secrets_by_hashed_secrets_bulk([pilot_hashed_secret])
        secret = secrets[0]  # Semantic, assured by fetch_records_bulk_or_raises

        matches = [
            pilot_credential
            for pilot_credential in pilots_credentials
            if secret["SecretID"] == pilot_credential["PilotSecretID"]
        ]

        # 3. Compare the secret_id
        if len(matches) == 0:

            raise BadPilotCredentialsError(
                data={
                    "pilot_stamp": pilot_stamp,
                    "pilot_hashed_secret": pilot_hashed_secret,
                    "real_hashed_secret": secret["HashedSecret"],
                    "pilot_secret_id[]": str(
                        [
                            pilot_credential["PilotSecretID"]
                            for pilot_credential in pilots_credentials
                        ]
                    ),
                    "secret_id": secret["SecretID"],
                    "test": str(pilots_credentials),
                }
            )
        elif len(matches) > 1:

            raise DBInBadStateError(
                detail="This should not happen. Duplicates in the database."
            )
        pilot_credentials = matches[0]  # Semantic

        # 4. Check if the secret is expired
        now = datetime.now(tz=timezone.utc)
        # Convert the timezone, TODO: Change with #454: https://github.com/DIRACGrid/diracx/pull/454
        expiration = secret["SecretExpirationDate"].replace(tzinfo=timezone.utc)
        if expiration < now:

            try:
                await self.delete_secrets_bulk([secret["SecretID"]])
            except SecretNotFoundError as e:
                await self.conn.rollback()

                raise DBInBadStateError(
                    detail="This should not happen. Pilot should have a secret, but not found."
                ) from e

            raise SecretHasExpiredError(
                data={
                    "pilot_hashed_secret": pilot_hashed_secret,
                    "now": str(now),
                    "expiration_date": secret["SecretExpirationDate"],
                }
            )

        # 5. Now the pilot is authorized, increment the counters (globally and locally).
        try:
            # 5.1 Increment the local count
            await self.increment_pilot_local_secret_and_last_time_use(
                pilot_secret_id=pilot_credentials["PilotSecretID"],
                pilot_stamp=pilot_credentials["PilotStamp"],
            )

            # 5.2 Increment the global count
            await self.increment_global_secret_use(
                secret_id=pilot_credentials["PilotSecretID"]
            )
        except Exception as e:  # Generic, to catch it.
            # Should NOT happen
            # Wrapped in a try/catch to still catch in case of an error in the counters
            # Caught and raised here to avoid raising a 4XX error
            await self.conn.rollback()

            raise DBInBadStateError(
                detail="This should not happen. Pilot has credentials, but has a corrupted secret."
            ) from e

        # 6. Delete all secrets if its count attained the secret_global_use_count_max
        if secret["SecretGlobalUseCountMax"]:
            if secret["SecretGlobalUseCount"] + 1 == secret["SecretGlobalUseCountMax"]:
                try:
                    await self.delete_secrets_bulk([secret["SecretID"]])
                except SecretNotFoundError as e:
                    # Should NOT happen
                    await self.conn.rollback()
                    raise DBInBadStateError(
                        detail="This should not happen. Pilot has credentials, but has corrupted secret."
                    ) from e

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

        # First verify that pilots can access a certain secret
        await self.verify_that_pilot_can_access_secret_bulk(
            pilot_to_secret_id_mapping_values
        )

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

    async def associate_pilot_with_jobs(self, pilot_stamp: str, job_ids: list[int]):
        """Associate a pilot with jobs. Raises an error if the pilot does  not exist and in case of a IntegrityError.

        **Important note**: We don't verify if a job exists in the JobDB
        """
        pilot_ids = await self.get_pilot_ids_by_stamps([pilot_stamp])
        # Semantic assured by fetch_records_bulk_or_raises
        pilot_id = pilot_ids[0]

        now = datetime.now(tz=timezone.utc)

        # Prepare the list of dictionaries for bulk insertion
        values = [
            {"PilotID": pilot_id, "JobID": job_id, "StartTime": now}
            for job_id in job_ids
        ]

        # Insert multiple rows in a single execute call
        stmt = insert(JobToPilotMapping).values(values)

        try:
            res = await self.conn.execute(stmt)
        except IntegrityError as e:
            raise PilotAlreadyAssociatedWithJobError(
                data={"pilot_stamp": pilot_stamp, "job_ids": str(job_ids)}
            ) from e

        if res.rowcount != len(job_ids):
            # If doubles
            await self.conn.rollback()
            raise PilotJobsNotFoundError(
                data={"pilot_stamp": pilot_stamp, "job_ids": str(job_ids)}
            )

    async def get_pilot_jobs_ids_by_stamp(self, pilot_stamp: str) -> list[int]:
        """Fetch pilot jobs by stamp."""
        pilot_ids = await self.get_pilot_ids_by_stamps([pilot_stamp])
        # Semantic assured by fetch_records_bulk_or_raises
        pilot_id = pilot_ids[0]

        return await self.get_pilot_jobs_ids_by_pilot_id(pilot_id)

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

    async def verify_that_pilot_can_access_secret_bulk(
        self, pilot_to_secret_id_mapping_values: list[dict[str, Any]]
    ):
        # 1. Extract unique pilot_stamps and secret_ids
        pilot_stamps = [
            entry["PilotStamp"] for entry in pilot_to_secret_id_mapping_values
        ]
        secret_ids = [
            entry["PilotSecretID"] for entry in pilot_to_secret_id_mapping_values
        ]

        # 2. Bulk fetch pilot and secret info
        pilots = await self.get_pilots_by_stamp_bulk(pilot_stamps)
        secrets = await self.get_secrets_by_secret_ids_bulk(secret_ids)

        # 3. Build lookup maps
        pilot_vo_map = {pilot["PilotStamp"]: pilot["VO"] for pilot in pilots}
        secret_vo_map = {secret["SecretID"]: secret["SecretVO"] for secret in secrets}

        # 4. Validate access
        bad_mapping = []

        for mapping in pilot_to_secret_id_mapping_values:
            pilot_stamp = mapping["PilotStamp"]
            secret_id = mapping["PilotSecretID"]

            pilot_vo = pilot_vo_map[pilot_stamp]
            secret_vo = secret_vo_map[secret_id]

            # If secret_vo is set to NULL, everybody can access it
            if not secret_vo:
                continue

            # Access allowed only if VOs match or secret_vo is open (None)
            if secret_vo is not None and pilot_vo != secret_vo:
                bad_mapping.append(
                    {
                        "pilot_stamp": pilot_stamp,
                        "given_vo": pilot_vo,
                        "expected_vo": secret_vo,
                    }
                )

        if bad_mapping:
            raise BadPilotVOError(data={"bad_mapping": str(bad_mapping)})

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
