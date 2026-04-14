"""Job monitor: prmon metrics, heartbeats, peek, stall/kill handling."""

from __future__ import annotations

import logging
import os
from collections import deque
from pathlib import Path

from diracx.core.models.job import HeartbeatData

logger = logging.getLogger(__name__)

# TODO: replace with CS config options
PEEK_LINES = 800


def parse_prmon_tsv(path: Path) -> dict[str, int] | None:
    """Parse the latest row from a prmon TSV time-series file.

    Returns a dict mapping column names to integer values, or None if the
    file is missing or has no data rows.
    """
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None

    lines = text.strip().splitlines()
    if len(lines) < 2:
        return None

    headers = lines[0].split("\t")
    values = lines[-1].split("\t")
    return {h: int(v) for h, v in zip(headers, values)}


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
