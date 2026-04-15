"""Job monitor: prmon metrics, heartbeats, peek, stall/kill handling."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from collections import deque
from pathlib import Path

from diracx.api.job_report import JobReport
from diracx.api.prmon_reader import PrmonFifoReader
from diracx.core.models.job import HeartbeatData

logger = logging.getLogger(__name__)

# TODO: replace with CS config options
PEEK_LINES = 800


def build_heartbeat_data(
    *,
    prmon_row: dict[str, int],
    job_path: Path,
    peek_content: str,
) -> HeartbeatData:
    """Build a HeartbeatData from a prmon TSV row.

    Metric mapping (prmon TSV columns to HeartbeatData fields):
    - CPUConsumed = utime + stime (seconds)
    - MemoryUsed = pss / 1024 (KB to MB)
    - Vsize = vmem / 1024 (KB to MB)
    - WallClockTime = wtime (seconds)
    - AvailableDiskSpace = free disk in job_path (bytes to MB)
    - LoadAverage = 1-minute load average
    - StandardOutput = peek content string
    """
    cpu = float(prmon_row.get("utime", 0) + prmon_row.get("stime", 0))
    pss_kb = prmon_row.get("pss", 0)
    vmem_kb = prmon_row.get("vmem", 0)
    wtime = float(prmon_row.get("wtime", 0))

    try:
        st = os.statvfs(job_path)
        disk_mb = (st.f_bavail * st.f_frsize) / (1024 * 1024)
    except OSError:
        disk_mb = None

    try:
        load_avg = os.getloadavg()[0]
    except OSError:
        load_avg = None

    return HeartbeatData(
        CPUConsumed=cpu,
        MemoryUsed=pss_kb / 1024,
        Vsize=vmem_kb / 1024,
        WallClockTime=wtime,
        AvailableDiskSpace=disk_mb,
        LoadAverage=load_avg,
        StandardOutput=peek_content,
    )


def build_peek_content(
    cwltool_stderr: deque[str],
    *,
    max_lines: int = PEEK_LINES,
) -> str:
    """Build peek content for Watchdog display.

    Returns the last *max_lines* cwltool stderr lines from the shared deque.
    Application stdout/stderr go into the output sandbox and are not
    duplicated here.
    """
    return "\n".join(list(cwltool_stderr)[-max_lines:])


async def send_final_heartbeat(
    *,
    job_path: Path,
    job_report: JobReport,
    cwltool_stderr: deque[str],
    fifo_reader: PrmonFifoReader | None = None,
) -> None:
    """Send one final heartbeat with exit metrics."""
    if fifo_reader is None or fifo_reader.latest_row is None:
        logger.info("No prmon data -- skipping final heartbeat")
        return

    peek = build_peek_content(cwltool_stderr)
    data = build_heartbeat_data(
        prmon_row=fifo_reader.latest_row,
        job_path=job_path,
        peek_content=peek,
    )
    try:
        await job_report.send_heartbeat(data)
    except Exception:
        logger.warning("Failed to send final heartbeat", exc_info=True)


class StallDetector:
    """Detect stalled jobs via CPU/wall-clock ratio over a rolling window.

    Reads cumulative CPU time (utime + stime) and wall clock time (wtime)
    from prmon metrics each heartbeat cycle. A job is stalled when the
    ratio stays below *threshold* for at least *window_seconds*.
    """

    def __init__(
        self,
        window_seconds: float = 1800,
        threshold: float = 0.05,
    ) -> None:
        self._window = window_seconds
        self._threshold = threshold
        self._first_cpu: float | None = None
        self._first_wall: float | None = None
        self._stalled_since: float | None = None

    def check(self, *, cpu_seconds: float, wall_seconds: float) -> bool:
        """Record a sample and return True if the job is stalled.

        :param cpu_seconds: Cumulative CPU time (prmon utime + stime).
        :param wall_seconds: Cumulative wall clock time (prmon wtime).
        """
        if self._first_cpu is None:
            self._first_cpu = cpu_seconds
            self._first_wall = wall_seconds
            return False

        assert self._first_wall is not None
        delta_wall = wall_seconds - self._first_wall
        if delta_wall <= 0:
            return False

        delta_cpu = cpu_seconds - self._first_cpu
        ratio = delta_cpu / delta_wall

        if ratio >= self._threshold:
            # Healthy — reset window start
            self._first_cpu = cpu_seconds
            self._first_wall = wall_seconds
            self._stalled_since = None
            return False

        # Below threshold
        if self._stalled_since is None:
            self._stalled_since = self._first_wall

        return (wall_seconds - self._stalled_since) >= self._window


class KillCommandReceived(Exception):  # noqa: N818
    """Raised when the server sends a Kill command via heartbeat."""


class JobMonitor:
    """Monitor a running job: heartbeats, peek, kill handling, stall detection.

    Start with ``asyncio.create_task(monitor.run())`` after launching the
    subprocess. Cancel the task when the subprocess exits.

    Note: prmon is launched as a wrapper around the command (not as a sidecar),
    so this class does not manage the prmon process. It reads metrics from a
    PrmonFifoReader that streams data from the prmon FIFO pipe.

    :param pid: PID of the subprocess (the prmon wrapper process).
    :param job_path: Working directory of the job.
    :param job_report: JobReport instance for sending heartbeats.
    :param cwltool_stderr: Shared deque of cwltool stderr lines.
    :param heartbeat_interval: Seconds between heartbeat cycles.
    :param fifo_reader: PrmonFifoReader instance for reading prmon metrics.
    :param stall_window: Stall detection window in seconds (default 1800).
    :param stall_threshold: CPU/wall ratio below which a job is stalled.
    :param kill_grace_period: Seconds between SIGTERM and SIGKILL.
    """

    def __init__(
        self,
        *,
        pid: int,
        job_path: Path,
        job_report: JobReport,
        cwltool_stderr: deque[str],
        heartbeat_interval: float = 60.0,
        fifo_reader: PrmonFifoReader | None = None,
        stall_window: float = 1800.0,
        stall_threshold: float = 0.05,
        kill_grace_period: float = 30.0,
    ) -> None:
        self._pid = pid
        self._job_path = job_path
        self._job_report = job_report
        self._cwltool_stderr = cwltool_stderr
        self._interval = heartbeat_interval
        self._fifo_reader = fifo_reader
        self._stall_detector = StallDetector(
            window_seconds=stall_window, threshold=stall_threshold
        )
        self._kill_grace = kill_grace_period

    def _kill_subprocess(self, sig: int = signal.SIGTERM) -> None:
        """Send a signal to the subprocess's process group."""
        try:
            pgid = os.getpgid(self._pid)
            os.killpg(pgid, sig)
        except OSError:
            logger.debug("Could not signal process %d", self._pid)

    async def run(self) -> None:
        """Run the monitor loop until cancelled or a kill/stall triggers.

        Raises KillCommandReceived if the server sends a Kill command or
        a stall is detected.
        """
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self._heartbeat_cycle()
            except KillCommandReceived:
                raise
            except Exception:
                logger.warning("Heartbeat cycle failed", exc_info=True)

    async def _heartbeat_cycle(self) -> None:
        """One iteration: collect metrics from prmon FIFO reader, send, check."""
        # 1. Read prmon metrics from FIFO reader
        if self._fifo_reader is None or self._fifo_reader.latest_row is None:
            logger.debug("No prmon data yet -- skipping heartbeat")
            return
        prmon_row = self._fifo_reader.latest_row

        # 2. Build peek content
        peek = build_peek_content(self._cwltool_stderr)

        # 3. Build HeartbeatData
        data = build_heartbeat_data(
            prmon_row=prmon_row,
            job_path=self._job_path,
            peek_content=peek,
        )

        # 4. Send heartbeat
        commands = await self._job_report.send_heartbeat(data)

        # 5. Check for Kill command
        for cmd in commands:
            if cmd.command == "Kill":
                logger.warning("Kill command received for job")
                self._kill_subprocess(signal.SIGTERM)
                await asyncio.sleep(self._kill_grace)
                self._kill_subprocess(signal.SIGKILL)
                raise KillCommandReceived("Server sent Kill command")

        # 6. Stall detection (using prmon CPU/wall metrics)
        cpu = float(prmon_row.get("utime", 0) + prmon_row.get("stime", 0))
        wall = float(prmon_row.get("wtime", 0))
        if self._stall_detector.check(cpu_seconds=cpu, wall_seconds=wall):
            logger.warning("Job stalled: CPU/wall ratio below threshold")
            self._job_report.set_job_status(
                application_status="Stalled: low CPU/wall-clock ratio"
            )
            await self._job_report.commit()
            self._kill_subprocess(signal.SIGTERM)
            await asyncio.sleep(self._kill_grace)
            self._kill_subprocess(signal.SIGKILL)
            raise KillCommandReceived("Job stalled — killed by monitor")
