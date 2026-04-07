from __future__ import annotations

__all__ = ("app",)

from ..utils import AsyncTyper

app = AsyncTyper(help="Job operations.")

# Import submodules to register commands
from . import search as _search  # noqa: F401, E402
