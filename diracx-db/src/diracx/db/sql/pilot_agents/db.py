from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, bindparam, insert, update
from sqlalchemy.exc import IntegrityError, OperationalError

from diracx.core.exceptions import (
    BadPilotCredentialsError,
    BadPilotVOError,
    CredentialsNotFoundError,
    OverusedSecretError,
    PilotAlreadyExistsError,
    PilotNotFoundError,
    SecretAlreadyExistsError,
    SecretHasExpiredError,
    SecretNotFoundError,
)
from diracx.db.exceptions import DBInBadStateError

from ..utils import BaseSQLDB, fetch_records_bulk_or_raises, utcnow
from .schema import PilotAgents, PilotAgentsDBBase, PilotSecrets, PilotToSecretMapping


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
            raise SecretNotFoundError(data={"secret_id": str(secret_id)})

    async def verify_pilot_secret(
        self, pilot_stamp: str, pilot_hashed_secret: str
    ) -> None:
        """Verify that a pilot can login with the given credentials."""
        # 1. Get the pilot to secret association
        pilots_credentials = await self.get_pilots_credentials_by_stamps_bulk(
            [pilot_stamp]
        )
        pilot_credentials = pilots_credentials[
            0
        ]  # Semantic, assured by fetch_records_bulk_or_raises

        # 2. Get the pilot secret itself
        secrets = await self.get_secrets_by_hashed_secrets_bulk([pilot_hashed_secret])
        secret = secrets[0]  # Semantic, assured by fetch_records_bulk_or_raises

        # 3. Compare the secret_id
        if not secret["SecretID"] == pilot_credentials["PilotSecretID"]:
            raise BadPilotCredentialsError(
                data={
                    "pilot_stamp": pilot_stamp,
                    "pilot_hashed_secret": pilot_hashed_secret,
                    "real_hashed_secret": secret["HashedSecret"],
                    str(secret["SecretID"]): str(pilot_credentials["PilotSecretID"]),
                }
            )

        # 4. Check if the secret is expired
        now = datetime.now(tz=timezone.utc)
        # Convert the timezone, TODO: Change with #454: https://github.com/DIRACGrid/diracx/pull/454
        expiration = secret["SecretExpirationDate"].replace(tzinfo=timezone.utc)
        if expiration < now:
            raise SecretHasExpiredError(
                data={
                    "pilot_hashed_secret": pilot_hashed_secret,
                    "now": str(now),
                    "expiration_date": secret["SecretExpirationDate"],
                }
            )

        # 5. Verify the secret counter
        # 5.1 Only check if the SecretGlobalUseCountMax is defined
        # If not defined, there is an infinite use.
        if secret["SecretGlobalUseCountMax"]:
            # 5.2 Finite use, we check if we can still login
            if secret["SecretGlobalUseCount"] + 1 > secret["SecretGlobalUseCountMax"]:
                raise OverusedSecretError(
                    data={
                        "pilot_stamp" "pilot_hashed_secret": pilot_hashed_secret,
                        "secret_global_use_count": secret["SecretGlobalUseCount"],
                        "secret_global_use_count_max": secret[
                            "SecretGlobalUseCountMax"
                        ],
                    }
                )

        # 6. Now the pilot is authorized, increment the counters (globally and locally).
        try:
            # 6.1 Increment the local count
            await self.increment_pilot_local_secret_and_last_time_use(
                pilot_secret_id=pilot_credentials["PilotSecretID"],
                pilot_stamp=pilot_credentials["PilotStamp"],
            )

            # 6.2 Increment the global count
            await self.increment_global_secret_use(
                secret_id=pilot_credentials["PilotSecretID"]
            )
        except Exception as e:  # Generic, to catch it.
            # Should NOT happen
            # Wrapped in a try/catch to still catch in case of an error in the counters
            # Caught and raised here to avoid raising a 4XX error
            raise DBInBadStateError(
                detail="This should not happen. Pilot has credentials, but has a corrupted secret."
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

    async def add_pilots_credentials_bulk(
        self,
        pilot_stamps: list[str],
        pilot_hashed_secrets: list[str],
        vo: str | None,
        pilot_secret_use_count_max: int = 1,
    ) -> list[dict]:

        assert len(pilot_stamps) == len(
            pilot_hashed_secrets
        ), "Each pilot has to have a secret"

        # Insert secrets
        await self.insert_unique_secrets_bulk(
            hashed_secrets=pilot_hashed_secrets,
            secret_global_use_count_max=pilot_secret_use_count_max,
            vo=vo,
        )

        # Get the secret ids to later associate them with pilots
        secrets = await self.get_secrets_by_hashed_secrets_bulk(pilot_hashed_secrets)
        secret_ids = [secret["SecretID"] for secret in secrets]

        # Associates pilots with their secrets
        pilot_to_secret_id_mapping_values = [
            {
                "PilotSecretID": secret_id,
                "PilotStamp": pilot_stamp,
            }
            for pilot_stamp, secret_id in zip(pilot_stamps, secret_ids)
        ]
        await self.associate_pilots_with_secrets_bulk(pilot_to_secret_id_mapping_values)

        # Return the added credentials
        # Used later to add an expiration date to the credentials
        return secrets

    async def insert_unique_secrets_bulk(
        self,
        hashed_secrets: list[str],
        vo: str | None,
        secret_global_use_count_max: int = 1,
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
            if "duplicate entry" in str(e.orig).lower():
                raise PilotAlreadyExistsError(
                    data={"pilot_stamps": str(pilot_to_secret_id_mapping_values)},
                    detail="at least one of these pilots already have a secret",
                ) from e

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

    async def get_pilots_credentials_by_stamps_bulk(
        self, pilot_stamps: list[str]
    ) -> list[dict]:
        """Bulk fetch pilots credentials. Ensure all stamps are found, else raise an error."""
        return await fetch_records_bulk_or_raises(
            self.conn,
            PilotToSecretMapping,
            CredentialsNotFoundError,
            "pilot_stamp",
            "PilotStamp",
            pilot_stamps,
        )

    async def get_secrets_by_hashed_secrets_bulk(self, hashed_secrets: list[str]):
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

    async def get_secrets_by_secret_ids_bulk(self, secret_ids: list[int]):
        """Bulk fetch secrets. Ensure all secrets are found, else raise an error."""
        return await fetch_records_bulk_or_raises(
            self.conn,
            PilotSecrets,
            SecretNotFoundError,
            "secret_id",
            "SecretID",
            secret_ids,
        )
