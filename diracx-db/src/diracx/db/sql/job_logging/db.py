from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, insert, select

if TYPE_CHECKING:
    pass

from diracx.core.exceptions import JobNotFound
from diracx.core.models import (
    JobStatus,
    JobStatusReturn,
)

from ..utils import BaseSQLDB
from .schema import (
    JobLoggingDBBase,
    LoggingInfo,
)

MAGIC_EPOC_NUMBER = 1270000000


class JobLoggingDB(BaseSQLDB):
    """Frontend for the JobLoggingDB. Provides the ability to store changes with timestamps."""

    metadata = JobLoggingDBBase.metadata

    async def insert_record(
        self,
        job_id: int,
        status: JobStatus,
        minor_status: str,
        application_status: str,
        date: datetime,
        source: str,
    ):
        """Add a new entry to the JobLoggingDB table. One, two or all the three status
        components (status, minorStatus, applicationStatus) can be specified.
        Optionally the time stamp of the status can
        be provided in a form of a string in a format '%Y-%m-%d %H:%M:%S' or
        as datetime.datetime object. If the time stamp is not provided the current
        UTC time is used.
        """
        # First, fetch the maximum SeqNum for the given job_id
        seqnum_stmt = select(func.coalesce(func.max(LoggingInfo.SeqNum) + 1, 1)).where(
            LoggingInfo.JobID == job_id
        )
        seqnum = await self.conn.scalar(seqnum_stmt)

        epoc = (
            time.mktime(date.timetuple())
            + date.microsecond / 1000000.0
            - MAGIC_EPOC_NUMBER
        )

        stmt = insert(LoggingInfo).values(
            JobID=int(job_id),
            SeqNum=seqnum,
            Status=status,
            MinorStatus=minor_status,
            ApplicationStatus=application_status[:255],
            StatusTime=date,
            StatusTimeOrder=epoc,
            Source=source[:32],
        )
        await self.conn.execute(stmt)

    async def get_records(self, job_id: int) -> list[JobStatusReturn]:
        """Returns a Status,MinorStatus,ApplicationStatus,StatusTime,Source tuple
        for each record found for job specified by its jobID in historical order.
        """
        stmt = (
            select(
                LoggingInfo.Status,
                LoggingInfo.MinorStatus,
                LoggingInfo.ApplicationStatus,
                LoggingInfo.StatusTime,
                LoggingInfo.Source,
            )
            .where(LoggingInfo.JobID == int(job_id))
            .order_by(LoggingInfo.StatusTimeOrder, LoggingInfo.StatusTime)
        )
        rows = await self.conn.execute(stmt)

        values = []
        for (
            status,
            minor_status,
            application_status,
            status_time,
            status_source,
        ) in rows:
            values.append(
                [
                    status,
                    minor_status,
                    application_status,
                    status_time.replace(tzinfo=timezone.utc),
                    status_source,
                ]
            )

        # If no value has been set for the application status in the first place,
        # We put this status to unknown
        res = []
        if values:
            if values[0][2] == "idem":
                values[0][2] = "Unknown"

            # We replace "idem" values by the value previously stated
            for i in range(1, len(values)):
                for j in range(3):
                    if values[i][j] == "idem":
                        values[i][j] = values[i - 1][j]

            # And we replace arrays with tuples
            for (
                status,
                minor_status,
                application_status,
                status_time,
                status_source,
            ) in values:
                res.append(
                    JobStatusReturn(
                        Status=status,
                        MinorStatus=minor_status,
                        ApplicationStatus=application_status,
                        StatusTime=status_time,
                        Source=status_source,
                    )
                )

        return res

    async def delete_records(self, job_ids: list[int]):
        """Delete logging records for given jobs."""
        stmt = delete(LoggingInfo).where(LoggingInfo.JobID.in_(job_ids))
        await self.conn.execute(stmt)

    async def get_wms_time_stamps(self, job_id):
        """Get TimeStamps for job MajorState transitions
        return a {State:timestamp} dictionary.
        """
        result = {}
        stmt = select(
            LoggingInfo.Status,
            LoggingInfo.StatusTimeOrder,
        ).where(LoggingInfo.JobID == job_id)
        rows = await self.conn.execute(stmt)
        if not rows.rowcount:
            raise JobNotFound(job_id) from None

        for event, etime in rows:
            result[event] = str(etime + MAGIC_EPOC_NUMBER)

        return result
