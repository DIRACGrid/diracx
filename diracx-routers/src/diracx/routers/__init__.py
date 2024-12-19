"""# Startup sequence.

uvicorn is called with `create_app` as a factory

create_app loads the environment configuration
"""

from __future__ import annotations

from .factory import DIRACX_MIN_CLIENT_VERSION, create_app, create_app_inner

__all__ = ("create_app", "create_app_inner", "DIRACX_MIN_CLIENT_VERSION")
