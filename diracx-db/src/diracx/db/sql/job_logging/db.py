from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import delete, func, insert, select

if TYPE_CHECKING:
    pass

from collections import defaultdict

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


class JobLoggingRecord(BaseModel):
    job_id: int
    status: JobStatus
    minor_status: str
    application_status: str
    date: datetime
    source: str


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

    async def bulk_insert_record(
        self,
        records: list[JobLoggingRecord],
    ):
        """Bulk insert entries to the JobLoggingDB table."""

        def get_epoc(date):
            return (
                time.mktime(date.timetuple())
                + date.microsecond / 1000000.0
                - MAGIC_EPOC_NUMBER
            )

        # First, fetch the maximum SeqNums for the given job_ids
        seqnum_stmt = (
            select(
                LoggingInfo.JobID, func.coalesce(func.max(LoggingInfo.SeqNum) + 1, 1)
            )
            .where(LoggingInfo.JobID.in_([record.job_id for record in records]))
            .group_by(LoggingInfo.JobID)
        )

        seqnum = {jid: seqnum for jid, seqnum in (await self.conn.execute(seqnum_stmt))}
        # IF a seqnum is not found, then assume it does not exist and the first sequence number is 1.
        # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#orm-bulk-insert-statements
        await self.conn.execute(
            LoggingInfo.__table__.insert(),
            [
                {
                    "JobID": record.job_id,
                    "SeqNum": seqnum.get(record.job_id, 1),
                    "Status": record.status,
                    "MinorStatus": record.minor_status,
                    "ApplicationStatus": record.application_status[:255],
                    "StatusTime": record.date,
                    "StatusTimeOrder": get_epoc(record.date),
                    "StatusSource": record.source[:32],
                }
                for record in records
            ],
        )

    async def get_records(self, job_ids: list[int]) -> dict[int, JobStatusReturn]:
        """Returns a Status,MinorStatus,ApplicationStatus,StatusTime,Source tuple
        for each record found for job specified by its jobID in historical order.
        """
        # We could potentially use a group_by here, but we need to post-process the
        # results later.
        stmt = (
            select(
                LoggingInfo.JobID,
                LoggingInfo.Status,
                LoggingInfo.MinorStatus,
                LoggingInfo.ApplicationStatus,
                LoggingInfo.StatusTime,
                LoggingInfo.Source,
            )
            .where(LoggingInfo.JobID.in_(job_ids))
            .order_by(LoggingInfo.StatusTimeOrder, LoggingInfo.StatusTime)
        )
        rows = await self.conn.execute(stmt)

        values = defaultdict(list)
        for (
            job_id,
            status,
            minor_status,
            application_status,
            status_time,
            status_source,
        ) in rows:

            values[job_id].append(
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
        res: dict = defaultdict(list)
        for job_id, history in values.items():
            if history[0][2] == "idem":
                history[0][2] = "Unknown"

            # We replace "idem" values by the value previously stated
            for i in range(1, len(history)):
                for j in range(3):
                    if history[i][j] == "idem":
                        history[i][j] = history[i - 1][j]

            # And we replace arrays with tuples
            for (
                status,
                minor_status,
                application_status,
                status_time,
                status_source,
            ) in history:
                res[job_id].append(
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

    async def get_wms_time_stamps_bulk(self, job_ids):
        """Get TimeStamps for job MajorState transitions for multiple jobs at once
        return a {JobID: {State:timestamp}} dictionary.
        """
        result = defaultdict(dict)
        stmt = select(
            LoggingInfo.JobID,
            LoggingInfo.Status,
            LoggingInfo.StatusTimeOrder,
        ).where(LoggingInfo.JobID.in_(job_ids))
        rows = await self.conn.execute(stmt)
        if not rows.rowcount:
            return {}

        for job_id, event, etime in rows:
            result[job_id][event] = str(etime + MAGIC_EPOC_NUMBER)

        return result
