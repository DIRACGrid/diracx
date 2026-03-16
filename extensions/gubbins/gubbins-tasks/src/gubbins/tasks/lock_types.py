"""Custom locked-object types for gubbins tasks.

Loaded via the ``diracx.lock_object_types`` entry point so that
``LockedObjectType("lollygag")`` is valid system-wide.
"""

from __future__ import annotations

from diracx.tasks.plumbing.lock_registry import register_locked_object_type

LOLLYGAG = register_locked_object_type("lollygag")
