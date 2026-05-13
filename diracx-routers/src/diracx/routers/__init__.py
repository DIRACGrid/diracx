"""# Startup sequence.

uvicorn is called with `create_app` as a factory

create_app loads the environment configuration
"""

from __future__ import annotations

# Special case due to a dependency in diracx-charts and DIRAC
__all__ = ["create_app"]

from .factory import create_app
