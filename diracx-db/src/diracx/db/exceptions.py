"""Database exception types used by DiracX database backends."""

from __future__ import annotations

__all__ = ["DBUnavailableError", "MissingDBURLError"]

from diracx.core.exceptions import ConfigurationError


class DBUnavailableError(Exception):
    """Raised when a database backend is unavailable."""


class MissingDBURLError(ConfigurationError):
    """Raised at startup when DBs used by enabled components have no URL.

    Attributes:
        env_vars: Names of the environment variables to set.
    """

    def __init__(self, detail: str, env_vars: list[str]):
        self.env_vars = env_vars
        super().__init__(detail)

    def __reduce__(self):
        # Keep env_vars when unpickled
        return (type(self), (self.detail, self.env_vars))
