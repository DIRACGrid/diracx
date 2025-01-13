from __future__ import annotations

from . import legacy
from .config import app

__all__ = ("app",)

app.add_typer(legacy.app, name="legacy")
