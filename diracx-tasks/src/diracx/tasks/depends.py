"""Public dependency injection markers for task authors."""

from __future__ import annotations

from diracx.tasks.plumbing.depends import CallbackSpawner, NoTransaction

__all__ = ("CallbackSpawner", "NoTransaction")
