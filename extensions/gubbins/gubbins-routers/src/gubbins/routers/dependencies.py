from __future__ import annotations

__all__ = ("Config", "LollygagDB")

from typing import Annotated

from diracx.core.config import ConfigSource
from fastapi import Depends

from gubbins.core.config.schema import Config as _Config

# Re-export DI types from gubbins-tasks (mirrors diracx.routers.dependencies)
from gubbins.tasks.depends import LollygagDB as LollygagDB  # noqa: F401

# --8<-- [start:my_pilots_router_depends]
from gubbins.tasks.depends import MyPilotDB as MyPilotDB  # noqa: F401, E402

__all__ += ("MyPilotDB",)  # type: ignore[assignment]
# --8<-- [end:my_pilots_router_depends]

# Overwrite the Config dependency such that gubbins routers
# can use it
Config = Annotated[_Config, Depends(ConfigSource.create)]
