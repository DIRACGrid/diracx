"""Tests for task discovery with the real gubbins entry points."""

from __future__ import annotations

from importlib.metadata import entry_points

from diracx.core.extensions import DiracEntryPoint
from diracx.tasks.plumbing.factory import load_task_registry

from gubbins.tasks.lollygag import SyncOwnersTask
from gubbins.tasks.my_pilots import MySubmitPilotsTask


def test_lock_object_types_not_loaded_as_tasks():
    """Lock object type modules must not be registered as task classes."""

    # Select lock-types entrypoints
    # Precondition: gubbins installs lock object types under ``diracx.tasks.``,
    # otherwise this test would pass without exercising the filtering at all.
    lock_eps = entry_points(group=DiracEntryPoint.LOCK_OBJECT_TYPES)
    assert {ep.name for ep in lock_eps} >= {"lollygag", "my_pilot"}

    # Load the tasks from the entrypoints
    registry = load_task_registry()

    # Real gubbins tasks are correctly discovered
    assert registry["my-pilots:MySubmitPilotsTask"] is MySubmitPilotsTask
    assert registry["lollygag:SyncOwnersTask"] is SyncOwnersTask

    # Lock object type modules are not registered as tasks
    assert not any(key.startswith("lock-object-types:") for key in registry)
