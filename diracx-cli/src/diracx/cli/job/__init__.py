from __future__ import annotations

__all__ = ("app",)

from ..utils import AsyncTyper
from .submit import app as submit_app

app = AsyncTyper(help="Job operations.")
app.add_typer(submit_app, name="submit")

# Import submodules to register commands
from . import search as _search  # noqa: F401, E402
