from __future__ import annotations

import secrets
from uuid import uuid4

from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError

from diracx.core.exceptions import (
    AuthorizationError,
    ExpiredFlowError,
    PendingAuthorizationError,
)

from ..utils import BaseDB, substract_date
from .schema import AuthorizationFlows, DeviceFlows, FlowStatus
from .schema import Base as AuthDBBase

# https://datatracker.ietf.org/doc/html/rfc8628#section-6.1
USER_CODE_ALPHABET = "BCDFGHJKLMNPQRSTVWXZ"
MAX_RETRY = 5


class AuthDB(BaseDB):
    # This needs to be here for the BaseDB to create the engine
    metadata = AuthDBBase.metadata

    async def device_flow_validate_user_code(self, user_code: str, max_validity: int) -> str:
        """Validate that the user_code can be used (Pending status, not expired)

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

    async def get_device_flow(self, device_code: str, max_validity: int):
        """
        :raises: NoResultFound
        """
        # The with_for_update
        # prevents that the token is retrieved
        # multiple time concurrently
        stmt = select(
            DeviceFlows,
            (DeviceFlows.creation_time < substract_date(seconds=max_validity)).label("is_expired"),
        ).with_for_update()
        stmt = stmt.where(
            DeviceFlows.device_code == device_code,
        )
        res = dict((await self.conn.execute(stmt)).one()._mapping)

        if res["is_expired"]:
            raise ExpiredFlowError()

        if res["status"] == FlowStatus.READY:
            # Update the status to Done before returning
            await self.conn.execute(
                update(DeviceFlows).where(DeviceFlows.device_code == device_code).values(status=FlowStatus.DONE)
            )

            return res

        if res["status"] == FlowStatus.DONE:
            raise AuthorizationError("Code was already used")

        if res["status"] == FlowStatus.PENDING:
            raise PendingAuthorizationError()

        raise AuthorizationError("Bad state in device flow")

    async def device_flow_insert_id_token(self, user_code: str, id_token: dict[str, str], max_validity: int) -> None:
        """
        :raises: AuthorizationError if no such code or status not pending
        """
        stmt = update(DeviceFlows)
        stmt = stmt.where(
            DeviceFlows.user_code == user_code,
            DeviceFlows.status == FlowStatus.PENDING,
            DeviceFlows.creation_time > substract_date(seconds=max_validity),
        )
        stmt = stmt.values(id_token=id_token, status=FlowStatus.READY)
        res = await self.conn.execute(stmt)
        if res.rowcount != 1:
            raise AuthorizationError(f"{res.rowcount} rows matched user_code {user_code}")

    async def insert_device_flow(
        self,
        client_id: str,
        scope: str,
        audience: str,
    ) -> tuple[str, str]:
        for _ in range(MAX_RETRY):
            user_code = "".join(
                secrets.choice(USER_CODE_ALPHABET) for _ in range(DeviceFlows.user_code.type.length)  # type: ignore
            )
            # user_code = "2QRKPY"
            device_code = secrets.token_urlsafe()
            stmt = insert(DeviceFlows).values(
                client_id=client_id,
                scope=scope,
                audience=audience,
                user_code=user_code,
                device_code=device_code,
            )
            try:
                await self.conn.execute(stmt)

            except IntegrityError:
                continue

            return user_code, device_code
        raise NotImplementedError(f"Could not insert new device flow after {MAX_RETRY} retries")

    async def insert_authorization_flow(
        self,
        client_id: str,
        scope: str,
        audience: str,
        code_challenge: str,
        code_challenge_method: str,
        redirect_uri: str,
    ) -> str:
        uuid = str(uuid4())

        stmt = insert(AuthorizationFlows).values(
            uuid=uuid,
            client_id=client_id,
            scope=scope,
            audience=audience,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            redirect_uri=redirect_uri,
        )

        await self.conn.execute(stmt)

        return uuid

    async def authorization_flow_insert_id_token(
        self, uuid: str, id_token: dict[str, str], max_validity: int
    ) -> tuple[str, str]:
        """
        returns code, redirect_uri
        :raises: AuthorizationError if no such uuid or status not pending
        """

        code = secrets.token_urlsafe()
        stmt = update(AuthorizationFlows)

        stmt = stmt.where(
            AuthorizationFlows.uuid == uuid,
            AuthorizationFlows.status == FlowStatus.PENDING,
            AuthorizationFlows.creation_time > substract_date(seconds=max_validity),
        )

        stmt = stmt.values(id_token=id_token, code=code, status=FlowStatus.READY)
        res = await self.conn.execute(stmt)

        if res.rowcount != 1:
            raise AuthorizationError(f"{res.rowcount} rows matched uuid {uuid}")

        stmt = select(AuthorizationFlows.code, AuthorizationFlows.redirect_uri)
        stmt = stmt.where(AuthorizationFlows.uuid == uuid)
        row = (await self.conn.execute(stmt)).one()
        return row.code, row.redirect_uri

    async def get_authorization_flow(self, code: str, max_validity: int):
        # The with_for_update
        # prevents that the token is retrieved
        # multiple time concurrently
        stmt = select(AuthorizationFlows).with_for_update()
        stmt = stmt.where(
            AuthorizationFlows.code == code,
            AuthorizationFlows.creation_time > substract_date(seconds=max_validity),
        )

        res = dict((await self.conn.execute(stmt)).one()._mapping)

        if res["status"] == FlowStatus.READY:
            # Update the status to Done before returning
            await self.conn.execute(
                update(AuthorizationFlows).where(AuthorizationFlows.code == code).values(status=FlowStatus.DONE)
            )

            return res

        if res["status"] == FlowStatus.DONE:
            raise AuthorizationError("Code was already used")

        raise AuthorizationError("Bad state in authorization flow")
