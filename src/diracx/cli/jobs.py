from __future__ import annotations

__all__ = ("app",)

from pathlib import Path

import typer

app = typer.Typer()


@app.command()
def list():
    raise NotImplementedError()


@app.command()
def submit(jdl: Path):
    raise NotImplementedError()
