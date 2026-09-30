"""Database exception types used by DiracX database backends."""

from __future__ import annotations

__all__ = ["DBUnavailableError"]


class DBUnavailableError(Exception):
    """Raised when a database backend is unavailable."""
