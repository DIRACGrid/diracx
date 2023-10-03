from diracx.db.sql.job.db import JobDB
from sqlalchemy import insert, select

from .schema import GubbinsInfo, JobDBBase


class GubbinsJobDB(JobDB):
    """
    This DB extends the diracx JobDB.
    All methods from the parent DB are accessible

    """

    metadata = JobDBBase.metadata

    async def insert_gubbins_info(self, job_id: int, info: str):
        """
        This is a new method that makes use of a new table.
        """
        stmt = insert(GubbinsInfo).values(JobID=job_id, Info=info)
        await self.conn.execute(stmt)

    async def getJobJDL(  # type: ignore[override]
        self, job_id: int, original: bool = False, with_info=False
    ) -> str | dict[str, str]:
        """
        This method modifes the one in the parent class:
        * adds an extra argument
        * changes the return type

        Note that this requires to disable mypy error with
        # type: ignore[override]
        """
        jdl = await super().getJobJDL(job_id, original=original)
        if not with_info:
            return jdl

        stmt = select(GubbinsInfo.Info).where(GubbinsInfo.JobID == job_id)

        info = (await self.conn.execute(stmt)).scalar_one()
        return {"JDL": jdl, "Info": info}

    async def setJobAttributes(self, job_id, jobData):
        """
        This method modified the one in the parent class,
        without changing the argument nor the return type

        Also, this method is called by the router via the status_utility
        so we can test in test_gubbins_job_router that the behavior
        is altered without even redefining a gubbins specific router
        """
        # We do nothing
        ...
