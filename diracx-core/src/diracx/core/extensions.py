__all__ = ("select_from_extension",)

import os
from collections import defaultdict
from importlib.metadata import EntryPoint, entry_points
from importlib.util import find_spec

from cachetools import LRUCache, cached


@cached(cache=LRUCache(maxsize=1024))
def extensions_by_priority() -> list[str]:
    """Yield extension module names in order of priority."""
    results = []
    for module_name in os.environ.get("DIRACX_EXTENSIONS", "diracx").split(","):
        if find_spec(module_name) is None:
            raise RuntimeError(f"Could not find extension module {module_name=}")
        results.append(module_name)
    return results


@cached(cache=LRUCache(maxsize=1024))
def select_from_extension(*, group: str, name: str | None = None) -> list[EntryPoint]:
    """Select entry points by group and name, in order of priority.

    Similar to ``importlib.metadata.entry_points.select`` except only modules
    found in ``DIRACX_EXTENSIONS`` are considered and return order is sorted
    from highest to lowest priority.
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
