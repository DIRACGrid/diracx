from __future__ import annotations

__all__ = ("app",)

from ...utils import AsyncTyper

app = AsyncTyper(help="Submit jobs to the grid.")

# Import submodules to register commands
from . import cmd as _cmd  # noqa: F401, E402
from . import cwl as _cwl  # noqa: F401, E402
from . import jdl as _jdl  # noqa: F401, E402
