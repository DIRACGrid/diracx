from __future__ import annotations

__all__ = ["app"]

from . import legacy
from .config import app

app.add_typer(legacy.app, name="legacy")
