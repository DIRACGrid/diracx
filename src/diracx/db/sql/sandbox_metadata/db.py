""" SandboxMetadataDB frontend
"""

from __future__ import annotations

import datetime

import sqlalchemy

from diracx.db.sql.utils import BaseSQLDB

from .schema import Base as SandboxMetadataDBBase
from .schema import sb_Owners, sb_SandBoxes


class SandboxMetadataDB(BaseSQLDB):
    metadata = SandboxMetadataDBBase.metadata

    async def _get_put_owner(self, owner: str, owner_group: str) -> int:
        """adds a new owner/ownerGroup pairs, while returning their ID if already existing

        Args:
            owner (str): user name
            owner_group (str): group of the owner
        """
        stmt = sqlalchemy.select(sb_Owners.OwnerID).where(
            sb_Owners.Owner == owner, sb_Owners.OwnerGroup == owner_group
        )
        result = await self.conn.execute(stmt)
        if owner_id := result.scalar_one_or_none():
            return owner_id

        stmt = sqlalchemy.insert(sb_Owners).values(Owner=owner, OwnerGroup=owner_group)
        result = await self.conn.execute(stmt)
        return result.lastrowid

    async def insert(
        self, owner: str, owner_group: str, sb_SE: str, se_PFN: str, size: int = 0
    ) -> tuple[int, bool]:
        """inserts a new sandbox in SandboxMetadataDB
        this is "equivalent" of DIRAC registerAndGetSandbox

        Args:
            owner (str): user name_
            owner_group (str): groupd of the owner
            sb_SE (str): _description_
            sb_PFN (str): _description_
            size (int, optional): _description_. Defaults to 0.
        """
        owner_id = await self._get_put_owner(owner, owner_group)
        stmt = sqlalchemy.insert(sb_SandBoxes).values(
            OwnerId=owner_id, SEName=sb_SE, SEPFN=se_PFN, Bytes=size
        )
        try:
            result = await self.conn.execute(stmt)
            return result.lastrowid
        except sqlalchemy.exc.IntegrityError:
            # it is a duplicate, try to retrieve SBiD
            stmt: sqlalchemy.Executable = sqlalchemy.select(sb_SandBoxes.SBId).where(  # type: ignore[no-redef]
                sb_SandBoxes.SEPFN == se_PFN,
                sb_SandBoxes.SEName == sb_SE,
                sb_SandBoxes.OwnerId == owner_id,
            )
            result = await self.conn.execute(stmt)
            sb_ID = result.scalar_one()
            stmt: sqlalchemy.Executable = (  # type: ignore[no-redef]
                sqlalchemy.update(sb_SandBoxes)
                .where(sb_SandBoxes.SBId == sb_ID)
                .values(LastAccessTime=datetime.datetime.utcnow())
            )
            await self.conn.execute(stmt)
            return sb_ID

    async def delete(self, sandbox_ids: list[int]) -> bool:
        stmt: sqlalchemy.Executable = sqlalchemy.delete(sb_SandBoxes).where(
            sb_SandBoxes.SBId.in_(sandbox_ids)
        )
        await self.conn.execute(stmt)

        return True
