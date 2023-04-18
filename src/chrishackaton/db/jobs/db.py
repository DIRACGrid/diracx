from __future__ import annotations

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import create_async_engine

from .schema import Base as JobDBBase
from .schema import JobJDLs


class BaseDB:
    def __init__(self):
        self._conn = None

    @classmethod
    async def make_engine(cls, db_url: str):
        cls.engine = create_async_engine(
            db_url,
            echo=True,
        )
        async with cls.engine.begin() as conn:
            await conn.run_sync(JobDBBase.metadata.create_all)

    @classmethod
    async def destroy_engine(cls):
        await cls.engine.dispose()

    @property
    def conn(self):
        if self._conn is None:
            raise RuntimeError(f"{self.__class__} was used before entering")
        return self._conn

    async def __aenter__(self):
        self._conn = await self.engine.connect().__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self._conn.commit()
        await self._conn.__aexit__(exc_type, exc, tb)
        self._conn = None


class JobDB(BaseDB):
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
