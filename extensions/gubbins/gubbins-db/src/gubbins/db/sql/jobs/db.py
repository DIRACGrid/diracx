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
        stmt = insert(GubbinsInfo).values(job_id=job_id, info=info)
        await self.conn.execute(stmt)

    async def get_job_jdls(  # type: ignore[override]
        self, job_ids, original: bool = False, with_info=False
    ) -> dict:
        """
        This method modifes the one in the parent class:
        * adds an extra argument
        * changes the return type

        Note that this requires to disable mypy error with
        # type: ignore[override]
        """
        jdl = await super().get_job_jdls(job_ids, original=original)
        if not with_info:
            return jdl

        stmt = select(GubbinsInfo.job_id, GubbinsInfo.info).where(
            GubbinsInfo.job_id.in_(job_ids)
        )

        rows = await self.conn.execute(stmt)
        info = {row[0]: row[1] for row in rows.fetchall()}

        result = {}
        for job_id, jdl_details in jdl.items():
            result[job_id] = {"JDL": jdl_details, "Info": info.get(job_id, "")}
        return result

    async def set_job_attributes(self, job_data):
        """
        This method modified the one in the parent class,
        without changing the argument nor the return type

        Also, this method is called by the router via the status_utility
        so we can test in test_gubbins_job_router that the behavior
        is altered without even redefining a gubbins specific router
        """
        # We do nothing
        ...
