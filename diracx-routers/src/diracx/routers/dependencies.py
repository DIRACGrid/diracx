"""Re-export dependency injection types for router modules.

The canonical definitions live in ``diracx.tasks.plumbing``; this module
provides a stable import location for routers.

Attributes:
    AvailableSecurityProperties: Dependency providing security properties available to a request.
    CallbackSpawner: Dependency for scheduling a callback after task completion.
    Config: Application configuration dependency.
    NoTransaction: Marker dependency for code that must not run in a transaction.
    auto_inject: Decorator that injects dependencies into a callable.
    auto_inject_depends: Helper that builds injected dependency parameters.
"""

from __future__ import annotations

__all__ = [
    "AvailableSecurityProperties",
    "CallbackSpawner",
    "Config",
    "NoTransaction",
    "auto_inject",
    "auto_inject_depends",
]

from diracx.tasks.plumbing import (
    AvailableSecurityProperties,
    CallbackSpawner,
    Config,
    NoTransaction,
    auto_inject,
    auto_inject_depends,
)
