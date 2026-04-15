"""All classes related to job reports."""

from __future__ import annotations

from datetime import datetime, timezone

from diracx.client.aio import AsyncDiracClient  # type: ignore[attr-defined]
from diracx.core.models.job import (
    HeartbeatData,
    JobCommand,
    JobMinorStatus,
    JobStatus,
    JobStatusUpdate,
)


class JobReport:
    """JobReport."""

    def __init__(self, job_id: int, source: str, client: AsyncDiracClient) -> None:
        """Initialize Job Report.

        :param job_id: the job ID
        :param source: source for the reports
        :param client: DiracX client instance
        """
        self.job_status_info: dict[
            str, dict[str, str]
        ] = {}  # where job status updates are cumulated
        self.job_id = job_id
        self.source = source
        self._client = client

    def set_job_status(
        self,
        status: JobStatus | None = None,
        minor_status: JobMinorStatus | None = None,
        application_status: str | None = None,
    ) -> None:
        """Add a new job status to the job report.

        :param status: job status
        :param minor_status: job minor status
        :param application_status: application status
        """
        timestamp = str(datetime.now(timezone.utc))
        # add job status record
        self.job_status_info.update(
            {
                timestamp: JobStatusUpdate(
                    Status=status,
                    MinorStatus=minor_status,
                    ApplicationStatus=application_status,
                    Source=self.source,
                ).model_dump()
            }
        )

    async def send_heartbeat(self, metrics: HeartbeatData) -> list[JobCommand]:
        """Send a heartbeat with metrics and return any pending commands.

        :param metrics: Resource metrics to report.
        :return: List of commands from the server (e.g. Kill).
        """
        return await self._client.jobs.add_heartbeat(
            {str(self.job_id): metrics.model_dump()}
        )

    async def send_stored_status_info(self):
        """Send all the accumulated job status information."""
        if not self.job_status_info:
            return
        body = {self.job_id: self.job_status_info}
        ret = await self._client.jobs.set_job_statuses(body)
        if ret.success:
            self.job_status_info = {}
        else:
            raise RuntimeError(f"Could not set job statuses: {ret}")

    async def commit(self):
        """Send all the accumulated information."""
        await self.send_stored_status_info()
