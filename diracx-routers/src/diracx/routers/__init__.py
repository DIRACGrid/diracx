"""# Startup sequence.

uvicorn is called with `create_app` as a factory

create_app loads the environment configuration
"""

from __future__ import annotations

__all__ = ["DIRACX_MIN_CLIENT_VERSION", "create_app", "create_app_inner"]

from .factory import DIRACX_MIN_CLIENT_VERSION, create_app, create_app_inner
