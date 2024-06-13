from __future__ import annotations

from typing import Any

import sqlalchemy

from diracx.core.models import SandboxInfo, SandboxType, UserInfo
from diracx.db.sql.utils import BaseSQLDB, utcnow

from .schema import Base as SandboxMetadataDBBase
from .schema import sb_EntityMapping, sb_Owners, sb_SandBoxes


class SandboxMetadataDB(BaseSQLDB):
    metadata = SandboxMetadataDBBase.metadata

    async def upsert_owner(self, user: UserInfo) -> int:
        """Get the id of the owner from the database."""
        # TODO: Follow https://github.com/DIRACGrid/diracx/issues/49
        stmt = sqlalchemy.select(sb_Owners.OwnerID).where(
            sb_Owners.Owner == user.preferred_username,
            sb_Owners.OwnerGroup == user.dirac_group,
            sb_Owners.VO == user.vo,
        )
        result = await self.conn.execute(stmt)
        if owner_id := result.scalar_one_or_none():
            return owner_id

        stmt = sqlalchemy.insert(sb_Owners).values(
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
        self, se_name: str, user: UserInfo, pfn: str, size: int
    ) -> None:
        """Add a new sandbox in SandboxMetadataDB."""
        # TODO: Follow https://github.com/DIRACGrid/diracx/issues/49
        owner_id = await self.upsert_owner(user)
        stmt = sqlalchemy.insert(sb_SandBoxes).values(
            OwnerId=owner_id,
            SEName=se_name,
            SEPFN=pfn,
            Bytes=size,
            RegistrationTime=utcnow(),
            LastAccessTime=utcnow(),
        )
        try:
            result = await self.conn.execute(stmt)
        except sqlalchemy.exc.IntegrityError:
            await self.update_sandbox_last_access_time(se_name, pfn)
        else:
            assert result.rowcount == 1

    async def update_sandbox_last_access_time(self, se_name: str, pfn: str) -> None:
        stmt = (
            sqlalchemy.update(sb_SandBoxes)
            .where(sb_SandBoxes.SEName == se_name, sb_SandBoxes.SEPFN == pfn)
            .values(LastAccessTime=utcnow())
        )
        result = await self.conn.execute(stmt)
        assert result.rowcount == 1

    async def sandbox_is_assigned(self, pfn: str, se_name: str) -> bool:
        """Checks if a sandbox exists and has been assigned."""
        stmt: sqlalchemy.Executable = sqlalchemy.select(sb_SandBoxes.Assigned).where(
            sb_SandBoxes.SEName == se_name, sb_SandBoxes.SEPFN == pfn
        )
        result = await self.conn.execute(stmt)
        is_assigned = result.scalar_one()
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
            sqlalchemy.select(sb_SandBoxes.SEPFN)
            .where(sb_SandBoxes.SBId == sb_EntityMapping.SBId)
            .where(
                sb_EntityMapping.EntityId == entity_id,
                sb_EntityMapping.Type == sb_type,
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
        """Mapp sandbox and jobs."""
        for job_id in jobs_ids:
            # Define the entity id as 'Entity:entity_id' due to the DB definition:
            entity_id = self.jobid_to_entity_id(job_id)
            select_sb_id = sqlalchemy.select(
                sb_SandBoxes.SBId,
                sqlalchemy.literal(entity_id).label("EntityId"),
                sqlalchemy.literal(sb_type).label("Type"),
            ).where(
                sb_SandBoxes.SEName == se_name,
                sb_SandBoxes.SEPFN == pfn,
            )
            stmt = sqlalchemy.insert(sb_EntityMapping).from_select(
                ["SBId", "EntityId", "Type"], select_sb_id
            )
            await self.conn.execute(stmt)

            stmt = (
                sqlalchemy.update(sb_SandBoxes)
                .where(sb_SandBoxes.SEPFN == pfn)
                .values(Assigned=True)
            )
            result = await self.conn.execute(stmt)
            assert result.rowcount == 1

    async def unassign_sandboxes_to_jobs(self, jobs_ids: list[int]) -> None:
        """Delete mapping between jobs and sandboxes."""
        for job_id in jobs_ids:
            entity_id = self.jobid_to_entity_id(job_id)
            sb_sel_stmt = sqlalchemy.select(sb_SandBoxes.SBId)
            sb_sel_stmt = sb_sel_stmt.join(
                sb_EntityMapping, sb_EntityMapping.SBId == sb_SandBoxes.SBId
            )
            sb_sel_stmt = sb_sel_stmt.where(sb_EntityMapping.EntityId == entity_id)

            result = await self.conn.execute(sb_sel_stmt)
            sb_ids = [row.SBId for row in result]

            del_stmt = sqlalchemy.delete(sb_EntityMapping).where(
                sb_EntityMapping.EntityId == entity_id
            )
            await self.conn.execute(del_stmt)

            sb_entity_sel_stmt = sqlalchemy.select(sb_EntityMapping.SBId).where(
                sb_EntityMapping.SBId.in_(sb_ids)
            )
            result = await self.conn.execute(sb_entity_sel_stmt)
            remaining_sb_ids = [row.SBId for row in result]
            if not remaining_sb_ids:
                unassign_stmt = (
                    sqlalchemy.update(sb_SandBoxes)
                    .where(sb_SandBoxes.SBId.in_(sb_ids))
                    .values(Assigned=False)
                )
                await self.conn.execute(unassign_stmt)
