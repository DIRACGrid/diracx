from __future__ import annotations

import sqlalchemy
from sqlalchemy import delete

from diracx.core.models import SandboxInfo, UserInfo
from diracx.db.sql.utils import BaseSQLDB, utcnow

from .schema import Base as SandboxMetadataDBBase
from .schema import sb_EntityMapping, sb_Owners, sb_SandBoxes


class SandboxMetadataDB(BaseSQLDB):
    metadata = SandboxMetadataDBBase.metadata

    async def upsert_owner(self, user: UserInfo) -> int:
        """Get the id of the owner from the database"""
        # TODO: Follow https://github.com/DIRACGrid/diracx/issues/49
        stmt = sqlalchemy.select(sb_Owners.OwnerID).where(
            sb_Owners.Owner == user.preferred_username,
            sb_Owners.OwnerGroup == user.dirac_group,
            # TODO: Add VO
        )
        result = await self.conn.execute(stmt)
        if owner_id := result.scalar_one_or_none():
            return owner_id

        stmt = sqlalchemy.insert(sb_Owners).values(
            Owner=user.preferred_username,
            OwnerGroup=user.dirac_group,
        )
        result = await self.conn.execute(stmt)
        return result.lastrowid

    @staticmethod
    def get_pfn(bucket_name: str, user: UserInfo, sandbox_info: SandboxInfo) -> str:
        """Get the sandbox's user namespaced and content addressed PFN"""
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
        """Add a new sandbox in SandboxMetadataDB"""
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

    async def sandbox_is_assigned(self, se_name: str, pfn: str) -> bool:
        """Checks if a sandbox exists and has been assigned."""
        stmt: sqlalchemy.Executable = sqlalchemy.select(sb_SandBoxes.Assigned).where(
            sb_SandBoxes.SEName == se_name, sb_SandBoxes.SEPFN == pfn
        )
        result = await self.conn.execute(stmt)
        is_assigned = result.scalar_one()
        return is_assigned
        return True

    async def unassign_sandbox_from_jobs(self, job_ids: list[int]):
        """
        Unassign sandbox from jobs
        """
        stmt = delete(sb_EntityMapping).where(
            sb_EntityMapping.EntityId.in_(f"Job:{job_id}" for job_id in job_ids)
        )
        await self.conn.execute(stmt)
