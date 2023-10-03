from __future__ import annotations

__all__ = ("Config",)

from typing import Annotated

from diracx.core.config import ConfigSource
from fastapi import Depends

from gubbins.core.config.schema import Config as _Config

# Overwrite the Config dependency such that gubbins routers
# can use it
Config = Annotated[_Config, Depends(ConfigSource.create)]
