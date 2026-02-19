from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import (
    Executable,
    and_,
    delete,
    exists,
    insert,
    literal,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError, NoResultFound

from diracx.core.exceptions import (
    SandboxAlreadyAssignedError,
    SandboxAlreadyInsertedError,
    SandboxNotFoundError,
)
from diracx.core.models.auth import UserInfo
from diracx.core.models.sandbox import SandboxInfo, SandboxType
from diracx.db.sql.utils.base import BaseSQLDB
from diracx.db.sql.utils.functions import substract_date, utcnow

from .schema import Base as SandboxMetadataDBBase
from .schema import SandBoxes, SBEntityMapping, SBOwners

logger = logging.getLogger(__name__)


class SandboxMetadataDB(BaseSQLDB):
    metadata = SandboxMetadataDBBase.metadata

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
        """Check if a sandbox exists and has been assigned."""
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

    async def select_sandboxes_for_deletion(
        self,
        *,
        batch_size: int = 500,
        cursor: int = 0,
    ) -> tuple[list[int], list[str], int]:
        """Select a batch of sandboxes for deletion.

        Uses cursor-based pagination (SBId > cursor) to avoid re-scanning
        rows that were already checked in previous batches.

        Runs two separate queries so MySQL can use indexes effectively:
        - Orphaned assigned sandboxes (NOT EXISTS on entity mapping)
        - Unassigned sandboxes older than 15 days (sargable comparison)

        Args:
            batch_size: Maximum number of sandboxes to select.
            cursor: Only consider sandboxes with SBId > cursor.

        Returns:
            Tuple of (sb_ids, pfns, new_cursor) for the selected sandboxes.
            new_cursor is the maximum SBId seen, to pass as cursor next time.

        """
        # Fetch distinct SEName values so the queries can use the Location
        # unique key (SEName, SEPFN) instead of doing a full table scan.
        se_names_result = await self.conn.execute(select(SandBoxes.SEName).distinct())
        se_names = [row.SEName for row in se_names_result]
        if not se_names:
            return [], [], cursor

        s3_condition = and_(
            SandBoxes.SEName.in_(se_names),
            SandBoxes.SEPFN.like("/S3/%"),
        )

        conditions = [
            # Orphaned: assigned but no longer mapped to any entity
            and_(
                SandBoxes.SBId > cursor,
                SandBoxes.Assigned,
                ~exists().where(SBEntityMapping.SBId == SandBoxes.SBId),
                s3_condition,
            ),
            # Unassigned: never assigned and older than 15 days (sargable)
            and_(
                SandBoxes.SBId > cursor,
                ~SandBoxes.Assigned,
                SandBoxes.LastAccessTime < substract_date(days=15),
                s3_condition,
            ),
        ]

        is_mysql = self.conn.dialect.name == "mysql"

        rows: list = []
        for condition in conditions:
            remaining = batch_size - len(rows)
            if remaining <= 0:
                break
            stmt = (
                select(SandBoxes.SBId, SandBoxes.SEPFN)
                .where(condition)
                .order_by(SandBoxes.SBId)
                .limit(remaining)
            )
            if is_mysql:
                stmt = stmt.with_for_update(skip_locked=True)
            result = await self.conn.execute(stmt)
            rows.extend(result.all())

        sb_ids = [row.SBId for row in rows]
        pfns = [row.SEPFN for row in rows]
        new_cursor = max(sb_ids) if sb_ids else cursor

        return sb_ids, pfns, new_cursor

    async def delete_sandboxes(self, sb_ids: list[int]) -> int:
        """Delete sandboxes by their IDs.

        Args:
            sb_ids: List of sandbox IDs to delete.

        Returns:
            Number of rows deleted.

        """
        if not sb_ids:
            return 0

        delete_stmt = delete(SandBoxes).where(SandBoxes.SBId.in_(sb_ids))
        result = await self.conn.execute(delete_stmt)
        logger.info("Deleted %d expired/unassigned sandboxes", result.rowcount)
        return result.rowcount
