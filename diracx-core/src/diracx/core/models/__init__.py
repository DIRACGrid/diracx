"""Models used to define the data structure of the requests and responses for the DiracX API.

Shared between the client components (cli, api) and services components (db, logic, routers).
"""

# in order to avoid DIRAC from failing to import TokenResponse
# TODO: remove after DIRACGrid/DIRAC#8433
from __future__ import annotations

from .auth import TokenResponse

__all__ = ["TokenResponse"]
