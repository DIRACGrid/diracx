from __future__ import annotations

"""Database exceptions used by DiracX.

This module defines exception types raised by DiracX database subsystems.
Keep exceptions small and focused so callers can catch specific failure
conditions (for example, when the database is unavailable).
"""

__all__ = ["DBUnavailableError"]


class DBUnavailableError(Exception):
    """Error raised when the database cannot be reached or used.

    This exception indicates that a connection or operation failed due to the
    database being unreachable, misconfigured, or returning an operational
    error. Database helper code raises this to signal callers that the
    underlying storage is unavailable.
    """

    pass
