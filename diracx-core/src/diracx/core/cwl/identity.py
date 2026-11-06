"""Content identity of packed CWL processes (DX-ADR-004, DX-ADR-007).

The client submits a document packed by ``cwltool --pack``: a ``$graph`` of
processes, each step's ``run`` naming another entry (``#gauss.cwl``) or
holding an inline body. A process is identified by :func:`hash_json` of the
process as submitted, with each ``run`` replaced by its child's identity, so
a workflow's identity covers its children's (a Merkle tree). Nothing is
normalised: the same files packed by another cwltool version, or with an
edited ``doc``, get another identity, which only costs a duplicate row.

:func:`check_self_contained` makes sure that a document needs nothing from
outside itself, so that neither the services nor a runner fetch anything.
See ``docs/dev/explanations/cwl-process-store.md`` for a worked example.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from rfc8785 import CanonicalizationError

from diracx.core.hashing import hash_json

REFERENCE_PREFIX = "proc:sha256-"
CWL_VERSION = "v1.2"
PROCESS_CLASSES = frozenset(
    {"CommandLineTool", "Workflow", "ExpressionTool", "Operation"}
)
# The only "$" keys a document may carry, at its top level. $schemas is
# refused: a runner fetches the schemas it names.
TOP_LEVEL_KEYS = frozenset({"$graph", "$namespaces", "cwlVersion"})


class NotSelfContainedError(ValueError):
    """A document refers to something outside itself."""


@dataclass(frozen=True)
class Component:
    """One process of a packed document.

    Attributes:
        process_id: Identity of the process.
        entry_id: Its ``$graph`` id, or ``None`` for an inline ``run`` body.
        children: Identities of the processes its steps run, sorted.
    """

    process_id: bytes
    entry_id: str | None
    children: tuple[bytes, ...]


def format_reference(process_id: bytes) -> str:
    """Return the reference that stands for a child in its parent's identity.

    Args:
        process_id: Identity of the child.

    Returns:
        The reference, e.g. ``proc:sha256-<hex>``.
    """
    return REFERENCE_PREFIX + process_id.hex()


def check_self_contained(document: dict[str, Any]) -> None:
    """Refuse a packed document that needs anything from outside itself.

    Args:
        document: A packed CWL document, as made by ``cwltool --pack``.

    Raises:
        NotSelfContainedError: Naming everything found: a ``$`` key other than
            the document's ``$graph`` and ``$namespaces``; a ``run`` that is
            not an entry of the ``$graph`` or an inline body; a ``File`` or
            ``Directory`` given by ``path`` or ``location``, which
            ``cwltool --pack`` writes as the submitter's absolute path or
            keeps as a URL, instead of inline ``contents`` or ``listing``.
        ValueError: If it is not a CWL v1.2 document.
    """
    graph = _graph(document)
    problems = [
        f"top-level {key} is not allowed"
        for key in document
        if key.startswith("$") and key not in TOP_LEVEL_KEYS
    ]
    for entry_id, process in graph.items():
        for path, key in _dollar_keys(process):
            problems.append(f"{entry_id}: {key} at {path} is not allowed")
        for path, run in _runs(process):
            if isinstance(run, str) and run not in graph:
                problems.append(f"{entry_id}: run {run!r} at {path} is not in $graph")
            elif not isinstance(run, (str, dict)):
                problems.append(f"{entry_id}: run at {path} is not a process")
        for path, literal in _file_literals(process):
            if "path" in literal or "location" in literal:
                problems.append(
                    f"{entry_id}: {literal['class']} at {path} names a path or "
                    "location; give its contents inline"
                )
    if problems:
        raise NotSelfContainedError("; ".join(problems))


def decompose(document: dict[str, Any]) -> dict[bytes, Component]:
    """Compute the identity of every process of a packed document.

    Args:
        document: A packed CWL document that passes
            :func:`check_self_contained`.

    Returns:
        Every distinct process, keyed by identity; :func:`entry_identity`
        picks out one entry.

    Raises:
        ValueError: If a ``run`` refers back to a process running it, or a
            number lies outside the range canonical JSON can represent.
    """
    graph = _graph(document)
    components: dict[bytes, Component] = {}
    by_entry: dict[str, bytes] = {}

    def identify(process: dict[str, Any], entry_id: str | None, chain: tuple) -> bytes:
        if entry_id is not None and entry_id in by_entry:
            return by_entry[entry_id]
        if entry_id in chain:
            raise ValueError(f"{entry_id} runs itself: {' -> '.join(chain)}")
        chain = (*chain, entry_id) if entry_id is not None else chain
        children = set()

        def child(run: Any) -> str:
            process_id = (
                identify(graph[run], run, chain)
                if isinstance(run, str)
                else identify(run, None, chain)
            )
            children.add(process_id)
            return format_reference(process_id)

        # The version is hashed once, whether a body repeats it or not
        body = {
            k: v for k, v in _replace_runs(process, child).items() if k != "cwlVersion"
        }
        try:
            process_id = hash_json({"cwlVersion": CWL_VERSION, "process": body})
        except CanonicalizationError as e:
            raise ValueError(f"{entry_id or 'An inline process'}: {e}") from None
        components[process_id] = Component(
            process_id, entry_id, tuple(sorted(children))
        )
        if entry_id is not None:
            by_entry[entry_id] = process_id
        return process_id

    for entry_id, process in graph.items():
        identify(process, entry_id, ())
    return components


def entry_identity(document: dict[str, Any], entry_id: str = "#main") -> bytes:
    """Return the identity of one entry of a packed document.

    Args:
        document: A packed CWL document.
        entry_id: The ``$graph`` entry, ``#main`` by default.

    Returns:
        Its identity.

    Raises:
        KeyError: If the document has no such entry.
    """
    for component in decompose(document).values():
        if component.entry_id == entry_id:
            return component.process_id
    raise KeyError(entry_id)


def subgraph(document: dict[str, Any], entry_id: str) -> dict[str, Any]:
    """Return the document one entry runs: that entry and what it reaches.

    Entries are kept as submitted, so the result is what a stock runner
    executes (``<file>#<entry_id>``) and re-hashes to the same identities.

    Args:
        document: A packed CWL document.
        entry_id: The entry, e.g. a workgraph step's ``run``.

    Returns:
        A packed document holding ``entry_id`` and every entry it runs.
    """
    graph = _graph(document)
    seen: list[str] = []
    stack = [entry_id]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.append(current)
        stack.extend(run for _, run in _runs(graph[current]) if isinstance(run, str))
    result: dict[str, Any] = {"cwlVersion": document["cwlVersion"]}
    if "$namespaces" in document:
        result["$namespaces"] = document["$namespaces"]
    result["$graph"] = [graph[e] for e in seen]
    return result


def _graph(document: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if document.get("cwlVersion") != CWL_VERSION:
        raise ValueError(
            f"Only CWL {CWL_VERSION} is supported, got "
            f"{document.get('cwlVersion')!r}: upgrade the document with cwl-upgrader"
        )
    if "$graph" in document:
        entries = document["$graph"]
    else:
        # cwltool --pack writes a process that runs nothing as itself
        entries = [{k: v for k, v in document.items() if k not in TOP_LEVEL_KEYS}]
    graph = {}
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("class") not in PROCESS_CLASSES:
            raise ValueError(f"Not a CWL process in $graph: {entry!r:.80}")
        graph[str(entry.get("id", "#main"))] = entry
    return graph


def _steps(process: dict[str, Any]) -> list[dict[str, Any]]:
    steps = process.get("steps") or []
    return list(steps.values()) if isinstance(steps, dict) else list(steps)


def _runs(process: dict[str, Any], path: str = "") -> Iterator[tuple[str, Any]]:
    """Every step ``run`` of a process, including those of inline bodies."""
    for step in _steps(process):
        where = f"{path}/{step.get('id', '?')}"
        run = step.get("run")
        yield where, run
        if isinstance(run, dict):
            yield from _runs(run, where)


def _replace_runs(process: dict[str, Any], child) -> dict[str, Any]:
    """A copy of ``process`` with each step's ``run`` replaced by ``child(run)``."""
    if process.get("class") != "Workflow":
        return process
    steps = process.get("steps") or []
    if isinstance(steps, dict):
        steps = {k: {**s, "run": child(s["run"])} for k, s in steps.items()}
    else:
        steps = [{**s, "run": child(s["run"])} for s in steps]
    return {**process, "steps": steps}


def _dollar_keys(value: Any, path: str = "") -> Iterator[tuple[str, str]]:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).startswith("$"):
                yield path or "/", key
            yield from _dollar_keys(item, f"{path}/{key}")
    elif isinstance(value, list):
        for i, item in enumerate(value):
            yield from _dollar_keys(item, f"{path}/{i}")


def _file_literals(value: Any, path: str = "") -> Iterator[tuple[str, dict]]:
    if isinstance(value, dict):
        if value.get("class") in ("File", "Directory"):
            yield path or "/", value
        for key, item in value.items():
            yield from _file_literals(item, f"{path}/{key}")
    elif isinstance(value, list):
        for i, item in enumerate(value):
            yield from _file_literals(item, f"{path}/{i}")
