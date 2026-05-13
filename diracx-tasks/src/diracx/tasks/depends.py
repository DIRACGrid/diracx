"""Public dependency injection markers for task authors."""

from __future__ import annotations

__all__ = ["CallbackSpawner", "NoTransaction"]

from diracx.tasks.plumbing.depends import CallbackSpawner, NoTransaction
