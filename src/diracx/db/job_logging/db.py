from __future__ import annotations

import time
from datetime import datetime

import sqlalchemy

from ..utils import BaseDB
from .schema import Base as JobLoggingDBBase
from .schema import LoggingInfo

MAGIC_EPOC_NUMBER = 1270000000


class JobLoggingDB(BaseDB):
    # This needs to be here for the BaseDB to create the engine
    metadata = JobLoggingDBBase.metadata

    async def insert(
        self,
        jobID: int,
        status: str = "idem",
        minorStatus: str = "idem",
        applicationStatus: str = "idem",
        date: str | datetime = "",
        source: str = "Unknown",
    ):
        """ "equivalent" of DIRAC's addLoggingRecord

        Args:
            jobID (int): _description_
            status (str, optional): _description_. Defaults to "idem".
            minorStatus (str, optional): _description_. Defaults to "idem".
            applicationStatus (str, optional): _description_. Defaults to "idem".
            date (str | datetime, optional): _description_. Defaults to None.
            source (str, optional): _description_. Defaults to "Unknown".
        """
        # self.log.info("Adding record for job ", str(jobID) + ": '" + event + "' from " + source)

        try:
            if not date:
                # Make the UTC datetime string and float
                _date = datetime.utcnow()
            elif isinstance(date, str):
                # The date is provided as a string in UTC
                from DIRAC.Core.Utilities import TimeUtilities

                _date = TimeUtilities.fromString(date)
            elif isinstance(date, datetime):
                _date = date
            else:
                # self.log.error("Incorrect date for the logging record")
                _date = datetime.utcnow()
        except Exception:
            # self.log.exception("Exception while date evaluation")
            _date = datetime.utcnow()

        epoc = (
            time.mktime(_date.timetuple())
            + _date.microsecond / 1000000.0
            - MAGIC_EPOC_NUMBER
        )

        stmt = sqlalchemy.insert(LoggingInfo).values(
            JobID=jobID,
            Status=status,
            MinorStatus=minorStatus,
            ApplicationStatus=applicationStatus,
            StatusTime=_date,
            StatusTimeOrder=epoc,
            StatusSource=source,
        )
        result = await self.conn.execute(stmt)
        return result.lastrowid
