"""Custom locked-object types for my_pilot tasks.

Loaded via the ``diracx.lock_object_types`` entry point so that
``LockedObjectType("my_pilot")`` is valid system-wide.
"""

from __future__ import annotations

__all__ = ["MY_PILOT"]

from diracx.tasks.plumbing.lock_registry import register_locked_object_type

MY_PILOT = register_locked_object_type("my_pilot")
