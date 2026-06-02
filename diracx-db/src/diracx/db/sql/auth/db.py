from __future__ import annotations

import logging
import re
import secrets
from datetime import UTC, datetime
from itertools import pairwise

from dateutil.relativedelta import relativedelta
from dateutil.rrule import MONTHLY, rrule
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.ext.asyncio import AsyncConnection
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import (
    AuthorizationError,
    TokenNotFoundError,
)
from diracx.db.sql.utils import BaseSQLDB, hash, substract_date, uuid7_from_datetime

from .schema import (
    AuthorizationFlows,
    DeviceFlows,
    FlowStatus,
    RefreshTokens,
    RefreshTokenStatus,
)
from .schema import Base as AuthDBBase

# https://datatracker.ietf.org/doc/html/rfc8628#section-6.1
USER_CODE_ALPHABET = "BCDFGHJKLMNPQRSTVWXZ"
MAX_RETRY = 5

logger = logging.getLogger(__name__)

# Always keep at least this many months of future RefreshTokens partitions ahead
# of "now" so the ``p_future`` catch-all partition never accumulates rows.
PARTITION_MONTHS_AHEAD = 12


def _month_start(dt: datetime) -> datetime:
    """Truncate ``dt`` to the first instant of its month."""
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _partition_name(month_start: datetime) -> str:
    """Name of the partition holding the tokens created during ``month_start``."""
    return f"p_{month_start.year}_{month_start.month}"


def _partition_boundary(dt: datetime) -> str:
    """``RANGE COLUMNS(JTI)`` upper bound (exclusive) for tokens created before ``dt``."""
    return str(uuid7_from_datetime(dt, randomize=False)).replace("-", "")


def plan_partition_maintenance(
    existing_months: list[datetime],
    now: datetime,
    retention_months: int,
    months_ahead: int,
) -> tuple[list[datetime], list[datetime]]:
    """Decide which monthly ``RefreshTokens`` partitions to drop and to add.

    ``existing_months`` are the month-start datetimes of the existing
    ``p_<year>_<month>`` partitions (excluding ``p_future``). Returns
    ``(months_to_drop, months_to_add)`` as month-start datetimes.
    """
    existing = sorted(existing_months)

    # A partition for month ``m`` holds tokens created before ``m + 1 month``, so
    # the whole partition is expired once that upper bound is older than the
    # retention horizon. Keeping ``retention_months`` worth of partitions never
    # drops a token younger than that many calendar months.
    horizon = now - relativedelta(months=retention_months)
    months_to_drop = [m for m in existing if m + relativedelta(months=1) <= horizon]

    # Ensure a partition exists for every month up to ``now + months_ahead`` by
    # appending months above the highest existing partition.
    target_last = _month_start(now) + relativedelta(months=months_ahead)
    cursor = max(existing) if existing else _month_start(now) - relativedelta(months=1)
    months_to_add: list[datetime] = []
    while cursor < target_last:
        cursor += relativedelta(months=1)
        months_to_add.append(cursor)

    return months_to_drop, months_to_add


class AuthDB(BaseSQLDB):
    metadata = AuthDBBase.metadata

    @classmethod
    async def post_create(cls, conn: AsyncConnection) -> None:
        """Create partitions.

        If it is a MySQL DB and it does not have
        it yet and the table does not have any data yet.
        We do this as a post_create step as sqlalchemy does not support
        partition so well.
        """
        if conn.dialect.name == "mysql":
            check_partition_query = text(
                "SELECT PARTITION_NAME FROM information_schema.partitions "
                "WHERE TABLE_NAME = 'RefreshTokens' AND PARTITION_NAME is not NULL"
            )
            partition_names = (await conn.execute(check_partition_query)).all()

            if not partition_names:
                # Create a monthly partition from today until 2 years
                # The partition are named p_<year>_<month>
                start_date = datetime.now(tz=UTC).replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                )
                end_date = start_date.replace(year=start_date.year + 2)

                dates = [
                    dt for dt in rrule(MONTHLY, dtstart=start_date, until=end_date)
                ]

                partition_list = []
                for name, limit in pairwise(dates):
                    partition_list.append(
                        f"PARTITION {_partition_name(name)} "
                        f"VALUES LESS THAN ('{_partition_boundary(limit)}')"
                    )
                partition_list.append("PARTITION p_future VALUES LESS THAN (MAXVALUE)")

                alter_query = text(
                    f"ALTER TABLE RefreshTokens PARTITION BY RANGE COLUMNS (JTI) ({','.join(partition_list)})"
                )

                check_table_empty_query = text("SELECT * FROM RefreshTokens LIMIT 1")
                refresh_table_content = (
                    await conn.execute(check_table_empty_query)
                ).all()
                if refresh_table_content:
                    logger.warning(
                        "RefreshTokens table not empty. Run the following query yourself"
                    )
                    logger.warning(alter_query)
                    return

                await conn.execute(alter_query)

                partition_names = (
                    await conn.execute(
                        check_partition_query, {"table_name": "RefreshTokens"}
                    )
                ).all()
                assert partition_names, (
                    f"There should be partitions now {partition_names}"
                )

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
        """raise: NoResultFound."""
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
        """raise: AuthorizationError if no such code or status not pending."""
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
                for _ in range(DeviceFlows.user_code.type.length)
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
                logger.warning(
                    "Device flow code collision detected, retrying (user_code=%s)",
                    user_code,
                )
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
        """Return code, redirect_uri.

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
        """Insert a refresh token in the DB.

        As well as user attributes required to generate access tokens.
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

    async def maintain_refresh_token_partitions(
        self,
        retention_months: int,
        months_ahead: int = PARTITION_MONTHS_AHEAD,
    ) -> None:
        """Maintain the monthly partitions of the RefreshTokens table.

        Drops partitions whose entire month is older than ``retention_months``
        and adds partitions ahead of time so the ``p_future`` catch-all never
        fills. Cleanup of expired refresh tokens is achieved by dropping whole
        partitions rather than deleting rows.

        Only implemented for MySQL; raises ``NotImplementedError`` for any other
        dialect (the table is only partitioned on MySQL).
        """
        dialect = self.conn.dialect.name
        if dialect != "mysql":
            raise NotImplementedError(
                "Refresh token partition maintenance is only implemented for "
                f"MySQL, not {dialect!r}"
            )

        check_partition_query = text(
            "SELECT PARTITION_NAME FROM information_schema.partitions "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'RefreshTokens' "
            "AND PARTITION_NAME IS NOT NULL"
        )
        partition_names = (await self.conn.execute(check_partition_query)).all()

        existing_months = []
        for (name,) in partition_names:
            if match := re.fullmatch(r"p_(\d+)_(\d+)", name):
                existing_months.append(
                    datetime(int(match.group(1)), int(match.group(2)), 1, tzinfo=UTC)
                )

        if not existing_months:
            logger.warning(
                "RefreshTokens is not partitioned; skipping partition maintenance. "
                "Partition the table manually (see AuthDB.post_create)."
            )
            return

        months_to_drop, months_to_add = plan_partition_maintenance(
            existing_months,
            now=datetime.now(tz=UTC),
            retention_months=retention_months,
            months_ahead=months_ahead,
        )

        # Add new partitions first, by splitting the p_future catch-all.
        if months_to_add:
            new_partitions = [
                f"PARTITION {_partition_name(m)} "
                f"VALUES LESS THAN ('{_partition_boundary(m + relativedelta(months=1))}')"
                for m in months_to_add
            ]
            new_partitions.append("PARTITION p_future VALUES LESS THAN (MAXVALUE)")
            await self.conn.execute(
                text(
                    "ALTER TABLE RefreshTokens REORGANIZE PARTITION p_future INTO ("
                    + ", ".join(new_partitions)
                    + ")"
                )
            )

        # Then drop the partitions whose whole month is past the retention horizon.
        if months_to_drop:
            drop_names = ", ".join(_partition_name(m) for m in months_to_drop)
            await self.conn.execute(
                text(f"ALTER TABLE RefreshTokens DROP PARTITION {drop_names}")
            )

        logger.info(
            "Refresh token partition maintenance: added %d, dropped %d",
            len(months_to_add),
            len(months_to_drop),
        )

    async def clean_expired_authorization_flows(self, max_retention: int) -> int:
        """Delete old authorization flows.

        max_retention: Maximum retention time in minutes for expired authorization flows.
        Must be bigger than authorization_flow_expiration_seconds.
        """
        stmt_auth = delete(AuthorizationFlows).where(
            AuthorizationFlows.creation_time < substract_date(minutes=max_retention),
        )
        res_auth = await self.conn.execute(stmt_auth)

        return res_auth.rowcount

    async def clean_expired_device_flows(self, max_retention: int) -> int:
        """Delete old device flows.

        max_retention: Maximum retention time in minutes for expired device flows.
        Must be bigger than device_flow_expiration_seconds.
        """
        stmt_device = delete(DeviceFlows).where(
            DeviceFlows.creation_time < substract_date(minutes=max_retention),
        )
        res_device = await self.conn.execute(stmt_device)

        return res_device.rowcount
