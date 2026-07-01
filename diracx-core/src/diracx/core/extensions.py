"""Helpers for discovering and using DIRACX extension entry points.

This module exposes utilities for locating extension implementations from the
installed Python entry points and for decorating functions with extension
fallback behavior.
"""

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
    """Available entrypoint group values."""

    CORE = "diracx"
    ACCESS_POLICY = "diracx.access_policies"
    CLI = "diracx.cli"
    HIDDEN_CLI = "diracx.cli.hidden"
    OS_DB = "diracx.dbs.os"
    SQL_DB = "diracx.dbs.sql"
    MIN_CLIENT_VERSION = "diracx.min_client_version"
    RESOURCES = "diracx.resources"
    SERVICES = "diracx.services"
    LOCK_OBJECT_TYPES = "diracx.lock_object_types"


@cached(cache=LRUCache(maxsize=1))
def extensions_by_priority() -> list[str]:
    """Return extension module names in priority order.

    The base DIRACX package is always included with the lowest priority, while
    any installed extension package is ranked above it.

    Note:
        This function is duplicated in diracx._client_importer to avoid
        importing diracx in the MetaPathFinder as part of unrelated imports
        (for example, http.client).

    Returns:
        list[str]: The sorted module names of the active extension packages.
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
    """Select extension entry points by group and name.

    Args:
        group (str): The entry point group to search.
        name (str | None): Optional entry point name filter.

    Returns:
        list[EntryPoint]: Matching entry points ordered by extension priority.
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
    """Decorate a function with an extension-aware fallback implementation.

    This decorator looks for an entry point in the specified group and name.
    When one is found, the decorated function is replaced by the extension's
    implementation; otherwise the original function is used.

    Args:
        group (str): The entry point group to search.
        name (str): The entry point name to search for.

    Returns:
        Callable[[Callable[P, T]], Callable[P, T]]: A decorator that wires in
            the extension implementation when available.

    Example:
        >>> @supports_extending(DiracEntryPoint.RESOURCES, "find_compatible_platforms")
        ... def my_function():
        ...     return "default implementation"
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
