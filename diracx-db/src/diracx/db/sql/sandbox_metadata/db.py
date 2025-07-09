from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from functools import partial
from typing import Any, AsyncGenerator

from sqlalchemy import (
    BigInteger,
    Column,
    Executable,
    MetaData,
    Table,
    and_,
    delete,
    exists,
    insert,
    literal,
    or_,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError, NoResultFound

from diracx.core.exceptions import (
    SandboxAlreadyAssignedError,
    SandboxAlreadyInsertedError,
    SandboxNotFoundError,
)
from diracx.core.models import SandboxInfo, SandboxType, UserInfo
from diracx.db.sql.utils.base import BaseSQLDB
from diracx.db.sql.utils.functions import days_since, utcnow

from .schema import Base as SandboxMetadataDBBase
from .schema import SandBoxes, SBEntityMapping, SBOwners

logger = logging.getLogger(__name__)


class SandboxMetadataDB(BaseSQLDB):
    metadata = SandboxMetadataDBBase.metadata

    # Temporary table to store the sandboxes to delete, see `select_and_delete_expired`
    _temp_table = Table(
        "sb_to_delete",
        MetaData(),
        Column("SBId", BigInteger, primary_key=True),
        prefixes=["TEMPORARY"],
    )

    async def get_owner_id(self, user: UserInfo) -> int | None:
        """Get the id of the owner from the database."""
        stmt = select(SBOwners.OwnerID).where(
            SBOwners.Owner == user.preferred_username,
            SBOwners.OwnerGroup == user.dirac_group,
            SBOwners.VO == user.vo,
        )
        return (await self.conn.execute(stmt)).scalar_one_or_none()

    async def get_sandbox_owner_id(self, pfn: str, se_name: str) -> int | None:
        """Get the id of the owner of a sandbox."""
        stmt = select(SBOwners.OwnerID).where(
            SBOwners.OwnerID == SandBoxes.OwnerId,
            SandBoxes.SEName == se_name,
            SandBoxes.SEPFN == pfn,
        )
        return (await self.conn.execute(stmt)).scalar_one_or_none()

    async def insert_owner(self, user: UserInfo) -> int:
        stmt = insert(SBOwners).values(
            Owner=user.preferred_username,
            OwnerGroup=user.dirac_group,
            VO=user.vo,
        )
        result = await self.conn.execute(stmt)
        return result.lastrowid

    @staticmethod
    def get_pfn(bucket_name: str, user: UserInfo, sandbox_info: SandboxInfo) -> str:
        """Get the sandbox's user namespaced and content addressed PFN."""
        parts = [
            "S3",
            bucket_name,
            user.vo,
            user.dirac_group,
            user.preferred_username,
            f"{sandbox_info.checksum_algorithm}:{sandbox_info.checksum}.{sandbox_info.format}",
        ]
        return "/" + "/".join(parts)

    async def insert_sandbox(
        self, owner_id: int, se_name: str, pfn: str, size: int
    ) -> None:
        """Add a new sandbox in SandboxMetadataDB."""
        stmt = insert(SandBoxes).values(
            OwnerId=owner_id,
            SEName=se_name,
            SEPFN=pfn,
            Bytes=size,
            RegistrationTime=utcnow(),
            LastAccessTime=utcnow(),
        )
        try:
            await self.conn.execute(stmt)
        except IntegrityError as e:
            raise SandboxAlreadyInsertedError(pfn, se_name) from e

    async def update_sandbox_last_access_time(self, se_name: str, pfn: str) -> None:
        stmt = (
            update(SandBoxes)
            .where(SandBoxes.SEName == se_name, SandBoxes.SEPFN == pfn)
            .values(LastAccessTime=utcnow())
        )
        result = await self.conn.execute(stmt)
        if result.rowcount == 0:
            # If the update didn't affect any row, the sandbox doesn't exist
            raise SandboxNotFoundError(pfn, se_name)
        elif result.rowcount != 1:
            raise NotImplementedError(
                "More than one sandbox was updated. This should not happen."
            )

    async def sandbox_is_assigned(self, pfn: str, se_name: str) -> bool | None:
        """Checks if a sandbox exists and has been assigned."""
        stmt: Executable = select(SandBoxes.Assigned).where(
            SandBoxes.SEName == se_name, SandBoxes.SEPFN == pfn
        )
        result = await self.conn.execute(stmt)
        try:
            is_assigned = result.scalar_one()
        except NoResultFound as e:
            raise SandboxNotFoundError(pfn, se_name) from e

        return is_assigned

    @staticmethod
    def jobid_to_entity_id(job_id: int) -> str:
        """Define the entity id as 'Entity:entity_id' due to the DB definition."""
        return f"Job:{job_id}"

    async def get_sandbox_assigned_to_job(
        self, job_id: int, sb_type: SandboxType
    ) -> list[Any]:
        """Get the sandbox assign to job."""
        entity_id = self.jobid_to_entity_id(job_id)
        stmt = (
            select(SandBoxes.SEPFN)
            .where(SandBoxes.SBId == SBEntityMapping.SBId)
            .where(
                SBEntityMapping.EntityId == entity_id,
                SBEntityMapping.Type == sb_type,
            )
        )
        result = await self.conn.execute(stmt)
        return [result.scalar()]

    async def assign_sandbox_to_jobs(
        self,
        jobs_ids: list[int],
        pfn: str,
        sb_type: SandboxType,
        se_name: str,
    ) -> None:
        """Map sandbox and jobs."""
        for job_id in jobs_ids:
            # Define the entity id as 'Entity:entity_id' due to the DB definition:
            entity_id = self.jobid_to_entity_id(job_id)
            select_sb_id = select(
                SandBoxes.SBId,
                literal(entity_id).label("EntityId"),
                literal(sb_type).label("Type"),
            ).where(
                SandBoxes.SEName == se_name,
                SandBoxes.SEPFN == pfn,
            )
            stmt = insert(SBEntityMapping).from_select(
                ["SBId", "EntityId", "Type"], select_sb_id
            )
            try:
                await self.conn.execute(stmt)
            except IntegrityError as e:
                raise SandboxAlreadyAssignedError(pfn, se_name) from e

            stmt = update(SandBoxes).where(SandBoxes.SEPFN == pfn).values(Assigned=True)
            result = await self.conn.execute(stmt)
            if result.rowcount == 0:
                # If the update didn't affect any row, the sandbox doesn't exist
                # It means the previous insert didn't have any effect
                raise SandboxNotFoundError(pfn, se_name)

            assert result.rowcount == 1

    async def unassign_sandboxes_to_jobs(self, jobs_ids: list[int]) -> None:
        """Delete mapping between jobs and sandboxes."""
        for job_id in jobs_ids:
            entity_id = self.jobid_to_entity_id(job_id)
            sb_sel_stmt = select(SandBoxes.SBId)
            sb_sel_stmt = sb_sel_stmt.join(
                SBEntityMapping, SBEntityMapping.SBId == SandBoxes.SBId
            )
            sb_sel_stmt = sb_sel_stmt.where(SBEntityMapping.EntityId == entity_id)

            result = await self.conn.execute(sb_sel_stmt)
            sb_ids = [row.SBId for row in result]

            del_stmt = delete(SBEntityMapping).where(
                SBEntityMapping.EntityId == entity_id
            )
            await self.conn.execute(del_stmt)

            sb_entity_sel_stmt = select(SBEntityMapping.SBId).where(
                SBEntityMapping.SBId.in_(sb_ids)
            )
            result = await self.conn.execute(sb_entity_sel_stmt)
            remaining_sb_ids = [row.SBId for row in result]
            if not remaining_sb_ids:
                unassign_stmt = (
                    update(SandBoxes)
                    .where(SandBoxes.SBId.in_(sb_ids))
                    .values(Assigned=False)
                )
                await self.conn.execute(unassign_stmt)

    @asynccontextmanager
    async def delete_unused_sandboxes(
        self, *, limit: int | None = None
    ) -> AsyncGenerator[AsyncGenerator[str, None], None]:
        """Get the sandbox PFNs to delete.

        The result of this function can be used as an async context manager
        to yield the PFNs of the sandboxes to delete. The context manager
        will automatically remove the sandboxes from the database upon exit.

        Args:
            limit: If not None, the maximum number of sandboxes to delete.

        """
        conditions = [
            # If it has assigned to a job but is no longer mapped it can be removed
            and_(
                SandBoxes.Assigned,
                ~exists().where(SBEntityMapping.SBId == SandBoxes.SBId),
            ),
            # If the sandbox is still unassigned after 15 days, remove it
            and_(~SandBoxes.Assigned, days_since(SandBoxes.LastAccessTime) >= 15),
        ]
        # Sandboxes which are not on S3 will be handled by legacy DIRAC
        condition = and_(SandBoxes.SEPFN.like("/S3/%"), or_(*conditions))

        # Copy the in-flight rows to a temporary table
        await self.conn.run_sync(partial(self._temp_table.create, checkfirst=True))
        select_stmt = select(SandBoxes.SBId).where(condition)
        if limit:
            select_stmt = select_stmt.limit(limit)
        insert_stmt = insert(self._temp_table).from_select(["SBId"], select_stmt)
        await self.conn.execute(insert_stmt)

        try:
            # Select the sandbox PFNs from the temporary table and yield them
            select_stmt = select(SandBoxes.SEPFN).join(
                self._temp_table, self._temp_table.c.SBId == SandBoxes.SBId
            )

            async def yield_pfns() -> AsyncGenerator[str, None]:
                async for row in await self.conn.stream(select_stmt):
                    yield row.SEPFN

            yield yield_pfns()

            # Delete the sandboxes from the main table
            delete_stmt = delete(SandBoxes).where(
                SandBoxes.SBId.in_(select(self._temp_table.c.SBId))
            )
            result = await self.conn.execute(delete_stmt)
            logger.info("Deleted %d expired/unassigned sandboxes", result.rowcount)

        finally:
            await self.conn.run_sync(partial(self._temp_table.drop, checkfirst=True))
