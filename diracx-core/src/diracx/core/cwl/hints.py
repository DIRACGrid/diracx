"""DiracX's own hints in a submitted CWL document (DX-ADR-007).

``dirac:`` hints and fields say how DiracX schedules the work rather than what
runs, so the services take them out of the packed document before it is
identified: editing a transformation's packer does not make its process
another one.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator
from typing import Any

# cwltool --pack writes the dirac: prefix expanded
DIRAC_NAMESPACE = "https://diracgrid.org/cwl#"


def split_dirac_hints(
    document: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Separate a packed document's ``dirac:`` hints and fields from its processes.

    Args:
        document: A packed CWL document, as ``cwltool --pack`` writes it.

    Returns:
        A copy of the document without ``dirac:`` hints and fields, and those
        hints and fields, by the id of the object that held them and then by
        class or field name, e.g.
        ``{"#main/MCMerge": {"Transformation": {...}}}``. Ids and names inside
        a hint are given as written in the CWL file, not as pack scoped them.
    """
    document = copy.deepcopy(document)
    taken: dict[str, dict[str, Any]] = {}
    for owner in _objects(document):
        owner_id = str(owner.get("id", "#main"))
        found = _take_hints(owner)
        for key in [k for k in owner if str(k).startswith(DIRAC_NAMESPACE)]:
            found[key.removeprefix(DIRAC_NAMESPACE)] = owner.pop(key)
        if found:
            taken[owner_id] = _unscope(found, owner_id)
    return document, taken


def _take_hints(owner: dict[str, Any]) -> dict[str, Any]:
    """Remove the ``dirac:`` hints of an object, in list or map form."""
    hints = owner.get("hints")
    if isinstance(hints, dict):
        items = [{**body, "class": cls} for cls, body in hints.items()]
    elif isinstance(hints, list):
        items = hints
    else:
        return {}
    found, kept = {}, []
    for hint in items:
        cls = str(hint.get("class", "")) if isinstance(hint, dict) else ""
        if cls.startswith(DIRAC_NAMESPACE):
            body = {k: v for k, v in hint.items() if k != "class"}
            found[cls.removeprefix(DIRAC_NAMESPACE)] = body
        else:
            kept.append(hint)
    if not found:
        return {}
    if not kept:
        del owner["hints"]
    elif isinstance(hints, dict):
        owner["hints"] = {
            k: v for k, v in hints.items() if not k.startswith(DIRAC_NAMESPACE)
        }
    else:
        owner["hints"] = kept
    return found


def _unscope(value: Any, scope: str) -> Any:
    """Undo pack writing a hint's ``id`` and ``name`` fields as ids in their scope.

    Pack resolves them as identifiers, so ``PerInput`` in step
    ``#main/MCSimulation`` comes out as ``#main/MCSimulation/PerInput``, and
    an ``id`` opens a scope for the names under it.
    """
    if isinstance(value, list):
        return [_unscope(v, scope) for v in value]
    if not isinstance(value, dict):
        return value
    own = value.get("id")
    inner = own if isinstance(own, str) and own.startswith(f"{scope}/") else scope
    result = {}
    for key, item in value.items():
        if key in ("id", "name") and isinstance(item, str):
            result[key] = item.removeprefix(f"{scope if key == 'id' else inner}/")
        else:
            result[key] = _unscope(item, inner)
    return result


def _objects(value: Any) -> Iterator[dict[str, Any]]:
    """Every JSON object in ``value``, outermost first."""
    if isinstance(value, dict):
        yield value
        for item in list(value.values()):
            yield from _objects(item)
    elif isinstance(value, list):
        for item in value:
            yield from _objects(item)
