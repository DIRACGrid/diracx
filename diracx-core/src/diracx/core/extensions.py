from __future__ import annotations

__all__ = [
    "extensions_by_priority",
    "select_from_extension",
    "supports_extending",
]

from collections import defaultdict
from importlib.metadata import EntryPoint, entry_points
from typing import Callable, ParamSpec, TypeVar, cast

from cachetools import LRUCache, cached

T = TypeVar("T")
P = ParamSpec("P")


@cached(cache=LRUCache(maxsize=1))
def extensions_by_priority() -> list[str]:
    """Yield extension module names in order of priority.

    NOTE: This function is duplicated in diracx._client_importer to avoid
    importing diracx in the MetaPathFinder as part of unrelated imports
    (e.g. http.client).
    """
    selected = entry_points().select(group="diracx")
    if selected is None:
        raise NotImplementedError(
            "No entry points found for group 'diracx'. Do you have it installed?"
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
    """Select entry points by group and name, in order of priority."""
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
    """Decorator to replace a function with an extension implementation.

    This decorator looks for an entry point in the specified group and name,
    and if found, replaces the decorated function with the extension's implementation.

    Args:
        group: The entry point group to search in
        name: The entry point name to search for

    Example:
        @supports_extending("diracx.resources", "find_compatible_platforms")
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
