from enum import Enum


class JobStatus(str, Enum):
    Running = "Running"
    Stalled = "Stalled"
    Killed = "Killed"
    Failed = "Failed"
    RECEIVED = "RECEIVED"
    SUBMITTING = "Submitting"
