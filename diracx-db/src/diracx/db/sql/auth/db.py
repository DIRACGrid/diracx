from __future__ import annotations

import secrets
from typing import Any

from sqlalchemy import DateTime, bindparam, delete, insert, select, update
from sqlalchemy.exc import IntegrityError, NoResultFound
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import (
    AuthorizationError,
    SecretNotFoundError,
    TokenNotFoundError,
)
from diracx.core.models import PilotSecretConstraints, SearchSpec, SortSpec
from diracx.db.sql.utils import BaseSQLDB, hash, substract_date
from diracx.db.sql.utils.functions import utcnow

from .schema import (
    AuthorizationFlows,
    DeviceFlows,
    FlowStatus,
    PilotSecrets,
    RefreshTokens,
    RefreshTokenStatus,
)
from .schema import Base as AuthDBBase

# https://datatracker.ietf.org/doc/html/rfc8628#section-6.1
USER_CODE_ALPHABET = "BCDFGHJKLMNPQRSTVWXZ"
MAX_RETRY = 5


class AuthDB(BaseSQLDB):
    metadata = AuthDBBase.metadata

    async def device_flow_validate_user_code(
        self, user_code: str, max_validity: int
    ) -> str:
        """Validate that the user_code can be used (Pending status, not expired).

        Returns the scope field for the given user_code

        :raises:
            NoResultFound if no such user code currently Pending
        """
        stmt = select(DeviceFlows.scope).where(
            DeviceFlows.user_code == user_code,
            DeviceFlows.status == FlowStatus.PENDING,
            DeviceFlows.creation_time > substract_date(seconds=max_validity),
        )

        return (await self.conn.execute(stmt)).scalar_one()

    async def get_device_flow(self, device_code: str):
        """:raises: NoResultFound"""
        # The with_for_update
        # prevents that the token is retrieved
        # multiple time concurrently
        stmt = select(DeviceFlows).with_for_update()
        stmt = stmt.where(
            DeviceFlows.device_code == hash(device_code),
        )
        return dict((await self.conn.execute(stmt)).one()._mapping)

    async def update_device_flow_status(
        self, device_code: str, status: FlowStatus
    ) -> None:
        stmt = update(DeviceFlows).where(
            DeviceFlows.device_code == hash(device_code),
        )
        stmt = stmt.values(status=status)
        await self.conn.execute(stmt)

    async def device_flow_insert_id_token(
        self, user_code: str, id_token: dict[str, str], max_validity: int
    ) -> None:
        """:raises: AuthorizationError if no such code or status not pending"""
        stmt = update(DeviceFlows)
        stmt = stmt.where(
            DeviceFlows.user_code == user_code,
            DeviceFlows.status == FlowStatus.PENDING,
            DeviceFlows.creation_time > substract_date(seconds=max_validity),
        )
        stmt = stmt.values(id_token=id_token, status=FlowStatus.READY)
        res = await self.conn.execute(stmt)
        if res.rowcount != 1:
            raise AuthorizationError(
                f"{res.rowcount} rows matched user_code {user_code}"
            )

    async def insert_device_flow(
        self,
        client_id: str,
        scope: str,
    ) -> tuple[str, str]:
        # Because the user_code might be short, there is a risk of conflicts
        # This is why we retry multiple times
        for _ in range(MAX_RETRY):
            user_code = "".join(
                secrets.choice(USER_CODE_ALPHABET)
                for _ in range(DeviceFlows.user_code.type.length)  # type: ignore
            )
            device_code = secrets.token_urlsafe()

            # Hash the the device_code to avoid leaking information
            hashed_device_code = hash(device_code)

            stmt = insert(DeviceFlows).values(
                client_id=client_id,
                scope=scope,
                user_code=user_code,
                device_code=hashed_device_code,
            )
            try:
                await self.conn.execute(stmt)

            except IntegrityError:
                continue

            return user_code, device_code
        raise NotImplementedError(
            f"Could not insert new device flow after {MAX_RETRY} retries"
        )

    async def insert_authorization_flow(
        self,
        client_id: str,
        scope: str,
        code_challenge: str,
        code_challenge_method: str,
        redirect_uri: str,
    ) -> str:
        uuid = str(uuid7())

        stmt = insert(AuthorizationFlows).values(
            uuid=uuid,
            client_id=client_id,
            scope=scope,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            redirect_uri=redirect_uri,
        )

        await self.conn.execute(stmt)

        return uuid

    async def authorization_flow_insert_id_token(
        self, uuid: str, id_token: dict[str, str], max_validity: int
    ) -> tuple[str, str]:
        """Returns code, redirect_uri
        :raises: AuthorizationError if no such uuid or status not pending.
        """
        # Hash the code to avoid leaking information
        code = secrets.token_urlsafe()
        hashed_code = hash(code)

        stmt = update(AuthorizationFlows)

        stmt = stmt.where(
            AuthorizationFlows.uuid == uuid,
            AuthorizationFlows.status == FlowStatus.PENDING,
            AuthorizationFlows.creation_time > substract_date(seconds=max_validity),
        )

        stmt = stmt.values(id_token=id_token, code=hashed_code, status=FlowStatus.READY)
        res = await self.conn.execute(stmt)

        if res.rowcount != 1:
            raise AuthorizationError(f"{res.rowcount} rows matched uuid {uuid}")

        stmt = select(AuthorizationFlows.code, AuthorizationFlows.redirect_uri)
        stmt = stmt.where(AuthorizationFlows.uuid == uuid)
        row = (await self.conn.execute(stmt)).one()
        return code, row.RedirectURI

    async def get_authorization_flow(self, code: str, max_validity: int):
        """Get the authorization flow details based on the code."""
        hashed_code = hash(code)
        # The with_for_update
        # prevents that the token is retrieved
        # multiple time concurrently
        stmt = select(AuthorizationFlows).with_for_update()
        stmt = stmt.where(
            AuthorizationFlows.code == hashed_code,
            AuthorizationFlows.creation_time > substract_date(seconds=max_validity),
        )

        return dict((await self.conn.execute(stmt)).one()._mapping)

    async def update_authorization_flow_status(
        self, code: str, status: FlowStatus
    ) -> None:
        """Update the status of an authorization flow based on the code."""
        hashed_code = hash(code)
        await self.conn.execute(
            update(AuthorizationFlows)
            .where(AuthorizationFlows.code == hashed_code)
            .values(status=status)
        )

    async def insert_refresh_token(
        self,
        jti: UUID,
        subject: str,
        scope: str,
    ) -> None:
        """Insert a refresh token in the DB as well as user attributes
        required to generate access tokens.
        """
        # Insert values into the DB
        stmt = insert(RefreshTokens).values(
            jti=str(jti),
            sub=subject,
            scope=scope,
        )
        await self.conn.execute(stmt)

    async def get_refresh_token(self, jti: UUID) -> dict:
        """Get refresh token details bound to a given JWT ID."""
        jti = str(jti)
        # The with_for_update
        # prevents that the token is retrieved
        # multiple time concurrently
        stmt = select(RefreshTokens).with_for_update()
        stmt = stmt.where(
            RefreshTokens.jti == jti,
        )
        try:
            res = dict((await self.conn.execute(stmt)).one()._mapping)
        except NoResultFound as e:
            raise TokenNotFoundError(jti) from e

        return res

    async def get_user_refresh_tokens(self, subject: str | None = None) -> list[dict]:
        """Get a list of refresh token details based on a subject ID (not revoked)."""
        # Get a list of refresh tokens
        stmt = select(RefreshTokens).with_for_update()

        if subject:
            stmt = stmt.where(
                RefreshTokens.sub == subject,
                RefreshTokens.status != RefreshTokenStatus.REVOKED,
            )

        res = (await self.conn.execute(stmt)).all()

        # Convert the results into dict
        refresh_tokens = []
        for refresh_token in res:
            refresh_tokens.append(dict(refresh_token._mapping))

        return refresh_tokens

    async def revoke_refresh_token(self, jti: UUID):
        """Revoke a token given by its JWT ID."""
        await self.conn.execute(
            update(RefreshTokens)
            .where(RefreshTokens.jti == str(jti))
            .values(status=RefreshTokenStatus.REVOKED)
        )

    async def revoke_user_refresh_tokens(self, subject):
        """Revoke all the refresh tokens belonging to a user (subject ID)."""
        await self.conn.execute(
            update(RefreshTokens)
            .where(RefreshTokens.sub == subject)
            .values(status=RefreshTokenStatus.REVOKED)
        )

    # ------------- Pilot secrets mechanism -------------

    async def insert_unique_secrets(
        self,
        hashed_secrets: list[bytes],
        secret_global_use_count_max: int | None = 1,
        secret_constraints: dict[bytes, PilotSecretConstraints] = {},
    ):
        """Bulk insert secrets.

        Raises:
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
        await self.conn.execute(stmt)

    async def delete_secrets(self, secret_uuids: list[str]):
        """Bulk delete secrets.

        Raises SecretNotFoundError if one of the secret was not found.
        """
        stmt = delete(PilotSecrets).where(PilotSecrets.secret_uuid.in_(secret_uuids))

        res = await self.conn.execute(stmt)

        if res.rowcount != len(secret_uuids):
            raise SecretNotFoundError(
                "At least one of the secret has not been deleted."
            )

        # We NEED to commit here, because we will raise an error after this function
        await self.conn.commit()

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
            raise SecretNotFoundError("Unknown secret")

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
        except IntegrityError as e:
            if "foreign key" in str(e.orig).lower():
                raise SecretNotFoundError(
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
                    detail="at least one of these secrets does not exist",
                ) from e
            raise NotImplementedError(f"This error is not caught: {str(e.orig)}") from e

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
        return await self._search(
            table=PilotSecrets,
            parameters=parameters,
            search=search,
            sorts=sorts,
            distinct=distinct,
            per_page=per_page,
            page=page,
        )
