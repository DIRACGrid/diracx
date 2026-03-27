from __future__ import annotations

__all__ = ["Priority", "Size"]

from enum import StrEnum


class Priority(StrEnum):
    """Task execution priority.

    Workers drain REALTIME before NORMAL before BACKGROUND.
    """

    REALTIME = "realtime"
    NORMAL = "normal"
    BACKGROUND = "background"


class Size(StrEnum):
    """Expected resource footprint of a task.

    Workers of a given size listen to the three priority streams
    for that size.
    """

    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
