from __future__ import annotations

__all__ = ("app",)

from ..utils import AsyncTyper

app = AsyncTyper(help="CWL workflow operations.", no_args_is_help=True)


@app.callback()
def callback() -> None:
    """CWL workflow operations."""


# Import submodules to register commands
from . import submit as _submit  # noqa: F401, E402
