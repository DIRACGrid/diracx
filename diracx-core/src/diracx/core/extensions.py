"""Helpers for discovering and selecting DiracX extension entry points."""

from __future__ import annotations

__all__ = [
    "DiracEntryPoint",
    "extensions_by_priority",
    "select_from_extension",
    "supports_extending",
]

from collections import defaultdict
from enum import StrEnum
from importlib.metadata import EntryPoint, entry_points
from typing import Callable, ParamSpec, TypeVar, cast

from cachetools import LRUCache, cached

T = TypeVar("T")
P = ParamSpec("P")


class DiracEntryPoint(StrEnum):
    """Available entrypoint group values.

    Naming conventions for entry-point groups:
      * Always start with `diracx`, using `.` to separate levels.
      * Namespace by either the system (e.g. `diracx.transformation.*`),
        or by the python package if no specific system is defined
        (e.g. `diracx.routers.access-policies`).
      * The system is singular (`transformation`, `resource-status`);
        the objects registered under it are plural (`feeders`, `sources`),
        i.e. `diracx.<system singular>.<components plural>`.
      * Use hyphens instead of underscores (``access-policies``).
    """

    CORE = "diracx"
    ACCESS_POLICY = "diracx.routers.access-policies"
    RESOURCE_STATUS_SOURCES = "diracx.resource-status.sources"
    CLI = "diracx.cli"
    HIDDEN_CLI = "diracx.cli.internal"
    OS_DB = "diracx.db.os"
    SQL_DB = "diracx.db.sql"
    MIN_CLIENT_VERSION = "diracx.routers.min-client-version"
    RESOURCES = "diracx.core.resources"
    SERVICES = "diracx.routers.services"
    LOCK_OBJECT_TYPES = "diracx.tasks.lock-object-types"
    FEEDERS = "diracx.transformation.feeders"
    PACKERS = "diracx.transformation.packers"


@cached(cache=LRUCache(maxsize=1))
def extensions_by_priority() -> list[str]:
    """Yield extension module names in order of priority.

    NOTE: This function is duplicated in diracx._client_importer to avoid
    importing diracx in the MetaPathFinder as part of unrelated imports
    (e.g. http.client).

    Returns:
        Extension module names ordered from lowest to highest priority.
    """
    selected = entry_points().select(group=DiracEntryPoint.CORE)
    if selected is None:
        raise NotImplementedError(
            f"No entry points found for group {DiracEntryPoint.CORE}. Do you have it installed?"
        )
    extensions = set()
    for entry_point in selected.select(name="extension"):
        extensions.add(entry_point.module)
    if len(extensions) not in {1, 2}:
        raise NotImplementedError(
            f"Expect to find either diracx or diracx + 1 extension: {extensions=}"
        )
    # DiracX will always be there so force it to be the lowest priority
    return sorted(extensions, key=lambda x: x == "diracx")


@cached(cache=LRUCache(maxsize=1024))
def select_from_extension(*, group: str, name: str | None = None) -> list[EntryPoint]:
    """Select entry points by group and name, in order of priority.

    Args:
        group: Entry point group to search.
        name: Optional entry point name to filter by.

    Returns:
        Matching entry points ordered by extension priority.
    """
    selected = entry_points().select(group=group)
    if name is not None:
        selected = selected.select(name=name)

    matches = defaultdict(list)
    for entry_point in selected:
        # The parent module of the entry point is the name of the extension
        module_name = entry_point.module.split(".")[0]
        matches[module_name].append(entry_point)

    return [
        x
        for module_name in extensions_by_priority()
        for x in matches.get(module_name, [])
    ]


def supports_extending(
    group: str, name: str
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Replace a function with an extension implementation.

    This decorator looks for an entry point in the specified group and name,
    and if found, replaces the decorated function with the extension's implementation.

    Args:
        group: The entry point group to search in
        name: The entry point name to search for

    Returns:
        A decorator that selects an extension implementation for a function.

    Example:
        @supports_extending(DiracEntryPoint.RESOURCES, "find_compatible_platforms")
        def my_function():
            return "default implementation"

    """

    def decorator(f: Callable[P, T]) -> Callable[P, T]:
        candidates = select_from_extension(group=group, name=name)
        assert len(candidates) > 0, f"No extension found for {group=} {name=}"
        # Try to find an extension implementation
        for entry_point in candidates[:-1]:
            extension_func = cast(Callable[P, T], entry_point.load())
            return extension_func
        # Fall back to the original function
        assert candidates[-1].value == f"{f.__module__}:{f.__qualname__}"
        return f

    return decorator
