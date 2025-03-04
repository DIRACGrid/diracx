from __future__ import annotations

import time
from datetime import timezone
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select

if TYPE_CHECKING:
    pass

from collections import defaultdict

from diracx.core.models import (
    JobLoggingRecord,
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

    async def insert_records(
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
                LoggingInfo.job_id, func.coalesce(func.max(LoggingInfo.seq_num) + 1, 1)
            )
            .where(LoggingInfo.job_id.in_([record.job_id for record in records]))
            .group_by(LoggingInfo.job_id)
        )

        seqnums = {
            jid: seqnum for jid, seqnum in (await self.conn.execute(seqnum_stmt))
        }
        # IF a seqnum is not found, then assume it does not exist and the first sequence number is 1.
        # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#orm-bulk-insert-statements
        values = []
        for record in records:
            if record.job_id not in seqnums:
                seqnums[record.job_id] = 1

            values.append(
                {
                    "JobID": record.job_id,
                    "SeqNum": seqnums[record.job_id],
                    "Status": record.status,
                    "MinorStatus": record.minor_status,
                    "ApplicationStatus": record.application_status[:255],
                    "StatusTime": record.date,
                    "StatusTimeOrder": get_epoc(record.date),
                    "StatusSource": record.source[:32],
                }
            )
            seqnums[record.job_id] = seqnums[record.job_id] + 1

        await self.conn.execute(
            LoggingInfo.__table__.insert(),
            values,
        )

    async def get_records(self, job_ids: list[int]) -> dict[int, JobStatusReturn]:
        """Returns a Status,MinorStatus,ApplicationStatus,StatusTime,Source tuple
        for each record found for job specified by its jobID in historical order.
        """
        # We could potentially use a group_by here, but we need to post-process the
        # results later.
        stmt = (
            select(
                LoggingInfo.job_id,
                LoggingInfo.status,
                LoggingInfo.minor_status,
                LoggingInfo.application_status,
                LoggingInfo.status_time,
                LoggingInfo.source,
            )
            .where(LoggingInfo.job_id.in_(job_ids))
            .order_by(LoggingInfo.status_time_order, LoggingInfo.status_time)
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
        stmt = delete(LoggingInfo).where(LoggingInfo.job_id.in_(job_ids))
        await self.conn.execute(stmt)

    async def get_wms_time_stamps(self, job_ids):
        """Get TimeStamps for job MajorState transitions for multiple jobs at once
        return a {JobID: {State:timestamp}} dictionary.
        """
        result = defaultdict(dict)
        stmt = select(
            LoggingInfo.job_id,
            LoggingInfo.status,
            LoggingInfo.status_time_order,
        ).where(LoggingInfo.job_id.in_(job_ids))
        rows = await self.conn.execute(stmt)
        if not rows.rowcount:
            return {}

        for job_id, event, etime in rows:
            result[job_id][event] = str(etime + MAGIC_EPOC_NUMBER)

        return result
