from __future__ import annotations

from sqlalchemy import insert, select

from ..utils import BaseDB
from .schema import Base as JobDBBase
from .schema import JobJDLs


class JobDB(BaseDB):
    # This needs to be here for the BaseDB to create the engine
    metadata = JobDBBase.metadata

    async def list(self):
        stmt = select(JobJDLs)
        res = [row._mapping async for row in (await self.conn.stream(stmt))]
        # return
        return res
        # result = await self.conn.execute(stmt)
        # return result.fetchall()

    async def insert(self, JDL, JobRequirements, OriginalJDL):
        stmt = insert(JobJDLs).values(
            JDL=JDL, JobRequirements=JobRequirements, OriginalJDL=OriginalJDL
        )
        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid


async def get_job_db():
    async with JobDB() as job_db:
        yield job_db
