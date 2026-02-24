from __future__ import annotations

__all__ = ["LollygagDB", "GubbinsJobDB"]

from .jobs.db import GubbinsJobDB
from .lollygag.db import LollygagDB

# --8<-- [start:my_pilots_db_init]
from .my_pilot_db.db import MyPilotDB  # noqa: F401

__all__ += ("MyPilotDB",)  # type: ignore[assignment]
# --8<-- [end:my_pilots_db_init]
