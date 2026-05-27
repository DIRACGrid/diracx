"""Router dependency injection types — re-exported from the canonical definitions in diracx-tasks."""

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
