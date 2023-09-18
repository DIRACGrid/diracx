from datetime import datetime, timezone
from unittest.mock import MagicMock

from sqlalchemy.exc import NoResultFound

from diracx.core.models import (
    JobStatus,
    JobStatusUpdate,
    ScalarSearchOperator,
    SetJobStatusReturn,
)
from diracx.db.sql.jobs.db import JobDB, JobLoggingDB


async def set_job_status(
    job_id: int,
    status: dict[datetime, JobStatusUpdate],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    force: bool = False,
) -> SetJobStatusReturn:
    """Set various status fields for job specified by its jobId.
    Set only the last status in the JobDB, updating all the status
    logging information in the JobLoggingDB. The statusDict has datetime
    as a key and status information dictionary as values
    """

    from DIRAC.Core.Utilities import TimeUtilities
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import (
        getNewStatus,
        getStartAndEndTime,
    )

    # transform JobStateUpdate objects into dicts
    statusDict = {}
    for key, value in status.items():
        statusDict[key] = value.dict(by_alias=True)

    res = await job_db.search(
        parameters=["Status", "StartExecTime", "EndExecTime"],
        search=[
            {
                "parameter": "JobID",
                "operator": ScalarSearchOperator.EQUAL,
                "value": str(job_id),
            }
        ],
        sorts=[],
    )
    if not res:
        raise NoResultFound(f"Job {job_id} not found")

    currentStatus = res[0]["Status"]
    startTime = res[0]["StartExecTime"]
    endTime = res[0]["EndExecTime"]

    # If the current status is Stalled and we get an update, it should probably be "Running"
    if currentStatus == JobStatus.STALLED:
        currentStatus = JobStatus.RUNNING

    # Get the latest time stamps of major status updates
    try:
        result = await job_logging_db.get_wms_time_stamps(job_id)
    except NoResultFound as e:
        raise e

    #####################################################################################################

    # This is more precise than "LastTime". timeStamps is a sorted list of tuples...
    timeStamps = sorted((float(t), s) for s, t in result.items())
    lastTime = TimeUtilities.fromEpoch(timeStamps[-1][0]).replace(tzinfo=timezone.utc)

    # Get chronological order of new updates
    updateTimes = sorted(statusDict)

    newStartTime, newEndTime = getStartAndEndTime(
        startTime, endTime, updateTimes, timeStamps, statusDict
    )

    job_data = {}
    if updateTimes[-1] >= lastTime:
        new_status, new_minor, new_application = returnValueOrRaise(
            getNewStatus(
                job_id,
                updateTimes,
                lastTime,
                statusDict,
                currentStatus,
                force,
                MagicMock(),
            )
        )

        if new_status:
            job_data["Status"] = new_status
            job_data["LastUpdateTime"] = datetime.now(timezone.utc)
        if new_minor:
            job_data["MinorStatus"] = new_minor
        if new_application:
            job_data["ApplicationStatus"] = new_application

        # TODO: implement elasticJobParametersDB ?
        # if cls.elasticJobParametersDB:
        #     result = cls.elasticJobParametersDB.setJobParameter(int(jobID), "Status", status)
        #     if not result["OK"]:
        #         return result

    for updTime in updateTimes:
        if statusDict[updTime]["StatusSource"].startswith("Job"):
            job_data["HeartBeatTime"] = updTime

    if not startTime and newStartTime:
        job_data["StartExecTime"] = newStartTime

    if not endTime and newEndTime:
        job_data["EndExecTime"] = newEndTime

    if job_data:
        await job_db.setJobAttributes(job_id, job_data)

    # Update the JobLoggingDB records
    # TODO: Because I really didn't liked the fact that the input field is called "Source"
    # and the output field is called "StatusSource"
    # I changed the name of the input field to "StatusSource"
    # Meaning this change must be added to the transformation layer for DIRAC.

    for updTime in updateTimes:
        sDict = statusDict[updTime]
        if not sDict["Status"]:
            sDict["Status"] = "idem"
        if not sDict["MinorStatus"]:
            sDict["MinorStatus"] = "idem"
        if not sDict["ApplicationStatus"]:
            sDict["ApplicationStatus"] = "idem"
        if not sDict["StatusSource"]:
            sDict["StatusSource"] = "Unknown"

        await job_logging_db.insert_record(
            job_id,
            sDict["Status"],
            sDict["MinorStatus"],
            sDict["ApplicationStatus"],
            updTime,
            sDict["StatusSource"],
        )

    return SetJobStatusReturn(**job_data)
