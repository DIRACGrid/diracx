from __future__ import annotations

import os
import re
from enum import Enum


class JobStatus(str, Enum):
    SUBMITTING = "Submitting"
    RECEIVED = "Received"
    CHECKING = "Checking"
    STAGING = "Staging"
    WAITING = "Waiting"
    MATCHED = "Matched"
    RUNNING = "Running"
    STALLED = "Stalled"
    COMPLETING = "Completing"
    DONE = "Done"
    COMPLETED = "Completed"
    FAILED = "Failed"
    DELETED = "Deleted"
    KILLED = "Killed"
    RESCHEDULED = "Rescheduled"


class MinorStatus(str, Enum):
    MAX_RESCHEDULING = "Maximum of reschedulings reached"
    RESCHEDULED = "job Rescheduled"


def dotenv_files_from_environment(prefix: str) -> list[str]:
    """Get the sorted list of .env files to use for configuration"""
    env_files = {}
    for key, value in os.environ.items():
        if match := re.fullmatch(rf"{prefix}(?:_(\d+))?", key):
            env_files[int(match.group(1) or -1)] = value
    return [v for _, v in sorted(env_files.items())]
