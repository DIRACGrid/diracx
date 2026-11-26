"""Gubbins extension of the DiracX JobDB."""

from __future__ import annotations

import logging

from diracx.db.sql.job.db import JobDB
from sqlalchemy import insert, select

from .schema import GubbinsInfo, JobDBBase

logger = logging.getLogger(__name__)


class GubbinsJobDB(JobDB):
    """This DB extends the diracx JobDB.

    All methods from the parent DB are accessible.
    """

    metadata = JobDBBase.metadata

    async def insert_gubbins_info(self, job_id: int, info: str):
        """This is a new method that makes use of a new table.

        Args:
            job_id: ID of the job.
            info: Gubbins-specific information to store for the job.
        """
        stmt = insert(GubbinsInfo).values(job_id=job_id, info=info)
        await self.conn.execute(stmt)

    async def get_job_jdls(  # type: ignore[override]
        self, job_ids, original: bool = False, with_info=False
    ) -> dict:
        """This method modifies the one in the parent class.

        * adds an extra argument
        * changes the return type

        Note that this requires to disable mypy error with
        # type: ignore[override]

        Args:
            job_ids: IDs of the jobs.
            original: Passed to the parent method.
            with_info: Whether to also return the Gubbins-specific information.

        Returns:
            The result of the parent method if ``with_info`` is false, else a
            dict of ``{"JDL": ..., "Info": ...}`` per job ID.
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
        """This method modified the one in the parent class.

        It changes neither the argument nor the return type.

        Also, this method is called by the router via the status_utility
        so we can test in test_gubbins_job_router that the behavior
        is altered without even redefining a gubbins specific router.

        Args:
            job_data: Passed to the parent method.

        Returns:
            The result of the parent method.
        """
        logger.info("GubbinsJobDB: setting attributes for jobs %s", job_data)
        return await super().set_job_attributes(job_data)
