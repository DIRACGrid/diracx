from __future__ import annotations

__all__ = ["LockedObjectType", "register_locked_object_type", "validate_registry"]

import inspect

# String-based extensible registry for locked object types.
# Core types are registered here; extensions add their own via the
# ``diracx.lock_object_types`` entry-point group.

# If an extension registers a type with the same name as a core type, that's a
# potential bug due to a new object type being added to diracx which conflicts
# with an extension's custom type. To help catch this, we track the file where
# each type was registered and throw an error if a duplicate registration is
# attempted.
_REGISTRY: dict[str, str] = {}


class LockedObjectType(str):
    """A validated locked-object type string.

    Acts like a plain ``str`` but raises ``ValueError`` at construction
    time if the value was not previously registered.
    """

    def __new__(cls, value: str):
        if value not in _REGISTRY:
            raise ValueError(
                f"Unknown LockedObjectType {value!r}. "
                f"Registered types: {sorted(_REGISTRY)}"
            )
        return super().__new__(cls, value)


def register_locked_object_type(name: str) -> LockedObjectType:
    """Register a new locked-object type and return it as a ``LockedObjectType``."""
    if name in _REGISTRY:
        raise ValueError(
            f"LockedObjectType {name!r} is already registered at {_REGISTRY[name]!r}"
        )
    _REGISTRY[name] = inspect.stack()[1].filename
    return LockedObjectType(name)


def validate_registry() -> None:
    """Validate that all entry-point-registered types are loaded.

    Called at startup to catch misconfigurations early.
    """
    from diracx.core.extensions import DiracEntryPoint, select_from_extension

    for ep in select_from_extension(group=DiracEntryPoint.LOCK_OBJECT_TYPES):
        ep.load()  # Side-effect: calls register_locked_object_type


# Register built-in types
TASK = register_locked_object_type("task")
JOB = register_locked_object_type("job")
TRANSFORMATION = register_locked_object_type("transformation")
