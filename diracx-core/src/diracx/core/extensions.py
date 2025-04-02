from __future__ import annotations

__all__ = [
    "extensions_by_priority",
    "select_from_extension",
]

from collections import defaultdict
from importlib.metadata import EntryPoint, entry_points

from cachetools import LRUCache, cached


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
