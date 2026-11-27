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
See ``docs/dev/explanations/cwl-process-identity.md`` for a worked example.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from diracx.core.hashing import hash_json

REFERENCE_PREFIX = "proc:sha256-"
CWL_VERSION = "v1.2"
PROCESS_CLASSES = frozenset(
    {"CommandLineTool", "Workflow", "ExpressionTool", "Operation"}
)
# CWL's own prefixes, bound in every document without $namespaces, and the
# namespaces of its names: a runner reads cwl:File, or File's IRI, as File.
# cwltool --pack writes CWL names bare, so only that spelling is accepted, and
# a check made on a bare name cannot be passed by another spelling of it.
BUILTIN_PREFIXES = frozenset({"cwl", "sld", "dct", "rdf", "rdfs", "xsd"})
CWL_NAMESPACES = ("https://w3id.org/cwl/cwl#", "https://w3id.org/cwl/salad#")
# The only "$" keys a document may carry, at its top level, and all a $graph
# document may carry there: what cwltool --pack writes. $schemas is refused:
# a runner fetches the schemas it names.
TOP_LEVEL_KEYS = frozenset({"$graph", "$namespaces", "cwlVersion"})
# How deep inline run bodies may nest. cwltool --pack makes an entry of every
# process a step runs, so real documents nest none or a handful; without a
# bound, a small document could nest enough of them to exhaust the stack of
# whatever walks it.
MAX_INLINE_DEPTH = 32


class NotSelfContainedError(ValueError):
    """A document refers to something outside itself."""


@dataclass(frozen=True)
class Component:
    """One process of a packed document.

    Attributes:
        process_id: Identity of the process, a 32-byte SHA-256 digest.
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
        NotSelfContainedError: Naming everything found: a ``$graph`` entry
            without an id, or with a repeated one; a ``$`` key other than
            the document's ``$graph`` and ``$namespaces``, or in a ``$graph``
            document any other top-level key; a key or string starting with a
            prefix of ``$namespaces`` and a colon, the only strings a binding
            can change, which ``cwltool --pack`` expands wherever CWL resolves
            them and an identity does not cover; a CWL name spelled other
            than bare, with a built-in prefix (``cwl:File``) or as an IRI,
            which a check on the bare name would miss; a ``run`` that is not
            an entry of the ``$graph`` or an inline body;
            a ``File`` or ``Directory`` given by ``path`` or ``location``,
            which ``cwltool --pack`` writes as the submitter's absolute path
            or keeps as a URL, instead of inline ``contents`` or ``listing``.
        ValueError: If it is not a CWL v1.2 document, or a process in it
            declares another version; if it does not have the shape of a
            packed document: ``$graph`` is not a list of processes,
            ``$namespaces`` is not a map, or ``steps`` are not a list or map
            of objects; or if it is nested too deeply to be read, inline
            ``run`` bodies more than :data:`MAX_INLINE_DEPTH` deep.
    """
    try:
        problems = _problems(document)
    except RecursionError:
        # Whatever is nested, nothing below could walk it either
        raise ValueError("The document is nested too deeply to be read") from None
    if problems:
        raise NotSelfContainedError("; ".join(problems))


def _problems(document: dict[str, Any]) -> list[str]:
    """What a packed document needs from outside itself."""
    problems: list[str] = []
    graph = _graph(document, problems)
    prefixes = document.get("$namespaces") or {}
    if not isinstance(prefixes, dict):
        raise ValueError(f"$namespaces is not a map of prefixes: {prefixes!r:.80}")
    # A single process document's other top-level keys are the process,
    # scanned below; a $graph document's would be scanned by nothing
    problems += [
        f"top-level {key} is not allowed"
        for key in document
        if key not in TOP_LEVEL_KEYS and (key.startswith("$") or "$graph" in document)
    ]
    for entry_id, process in graph.items():
        _check_steps(process, entry_id)
        for path, key in _dollar_keys(process):
            problems.append(f"{entry_id}: {key} at {path} is not allowed")
        for path, name in _names(process):
            # A binding only changes a name starting "<prefix>:": schema-salad
            # splits a name at its first colon to expand it
            prefix, colon, _ = name.partition(":")
            if colon and prefix in prefixes:
                problems.append(
                    f"{entry_id}: {name!r} at {path} uses the $namespaces prefix "
                    f"{prefix!r}; write the full IRI, as cwltool --pack does"
                )
            elif colon and prefix in BUILTIN_PREFIXES:
                problems.append(
                    f"{entry_id}: {name!r} at {path} spells a CWL name with the "
                    f"built-in prefix {prefix!r}; write it bare, as cwltool "
                    "--pack does"
                )
            elif name.startswith(CWL_NAMESPACES):
                problems.append(
                    f"{entry_id}: {name!r} at {path} spells a CWL name as an IRI; "
                    "write it bare, as cwltool --pack does"
                )
        for path, run in _runs(process):
            if isinstance(run, dict):
                _check_version(run, f"{entry_id}: the inline process at {path}")
            if isinstance(run, str):
                if run not in graph:
                    problems.append(
                        f"{entry_id}: run {run!r} at {path} is not in $graph"
                    )
            # A dict is a process embedded in the step instead of an entry
            elif not _is_process(run):
                problems.append(f"{entry_id}: run at {path} is not a process")
        for path, literal in _file_literals(process):
            if "path" in literal or "location" in literal:
                problems.append(
                    f"{entry_id}: {literal['class']} at {path} names a path or "
                    "location; give its contents inline"
                )
    return problems


def decompose(document: dict[str, Any]) -> dict[bytes, Component]:
    """Compute the identity of every process of a packed document.

    Args:
        document: A packed CWL document that passes
            :func:`check_self_contained`.

    Returns:
        Every distinct process, keyed by identity; :func:`entry_identity`
        picks out one entry.

    Raises:
        ValueError: If a ``run`` refers back to a process running it, a
            number lies outside the range canonical JSON can represent, or
            the document is nested too deeply to be hashed.
    """
    graph = _graph(document)
    components: dict[bytes, Component] = {}
    by_entry: dict[str, bytes] = {}

    def identify(process: dict[str, Any], entry_id: str | None, chain: tuple) -> bytes:
        if entry_id is not None and entry_id in by_entry:
            return by_entry[entry_id]
        if entry_id in chain:
            raise ValueError(f"{entry_id} runs itself: {' -> '.join(chain)}")
        # _graph checks the entries; an inline body is checked here
        if not _is_process(process):
            raise ValueError(f"Not a CWL process inline in a step: {process!r:.80}")
        _check_version(process, entry_id or "An inline process")
        chain = (*chain, entry_id) if entry_id is not None else chain
        children = []
        if process.get("class") == "Workflow":
            for step in _steps(process):
                run = step["run"]
                children.append(
                    identify(graph[run], run, chain)
                    if isinstance(run, str)
                    else identify(run, None, chain)
                )
            process = _replace_runs(process, [format_reference(c) for c in children])
        # The version is hashed once, whether a body repeats it or not
        body = {k: v for k, v in process.items() if k != "cwlVersion"}
        try:
            process_id = hash_json({"cwlVersion": CWL_VERSION, "process": body})
        except ValueError as e:
            raise ValueError(f"{entry_id or 'An inline process'}: {e}") from None
        # An entry and an identical inline body share an identity: keep the
        # entry id whichever comes last ($graph ids are unique and hashed)
        if entry_id is None and process_id in components:
            entry_id = components[process_id].entry_id
        components[process_id] = Component(
            process_id, entry_id, tuple(sorted(set(children)))
        )
        if entry_id is not None:
            by_entry[entry_id] = process_id
        return process_id

    try:
        for entry_id, process in graph.items():
            identify(process, entry_id, ())
    except RecursionError:
        # Only inline bodies are bounded: not what a process nests in its
        # fields, nor how many entries run one another in a row
        raise ValueError("The document is nested too deeply to be hashed") from None
    return components


def entry_identity(document: dict[str, Any], entry_id: str = "#main") -> bytes:
    """Return the identity of one entry of a packed document.

    Args:
        document: A packed CWL document.
        entry_id: The ``$graph`` entry, ``#main`` by default.

    Returns:
        Its identity, a 32-byte SHA-256 digest.

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
        A packed document holding ``entry_id`` and every entry it runs; a
        single process document is returned as it is, being its own entry.
    """
    graph = _graph(document)
    if "$graph" not in document:
        # Wrapping it would need an id it may not have, changing its identity
        if entry_id not in graph:
            raise KeyError(entry_id)
        return document
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


def _graph(
    document: dict[str, Any], problems: list[str] | None = None
) -> dict[str, dict[str, Any]]:
    """The ``$graph`` entries by id.

    An entry without an id, or with a repeated one, raises a ``ValueError``;
    given ``problems``, it is listed there instead and left out. A document
    or ``$graph`` of another type always raises one: nothing has validated
    the document as CWL yet.
    """
    if not isinstance(document, dict):
        raise ValueError(f"Not a CWL document: {document!r:.80}")
    if document.get("cwlVersion") != CWL_VERSION:
        raise ValueError(
            f"Only CWL {CWL_VERSION} is supported, got "
            f"{document.get('cwlVersion')!r}: upgrade the document with cwl-upgrader"
        )
    if "$graph" in document:
        entries, default_id = document["$graph"], None
    else:
        # cwltool --pack writes a process that runs nothing as itself
        entries = [{k: v for k, v in document.items() if k not in TOP_LEVEL_KEYS}]
        default_id = "#main"
    if not isinstance(entries, list):
        raise ValueError(f"$graph is not a list of processes: {entries!r:.80}")
    graph = {}
    for entry in entries:
        if not _is_process(entry):
            raise ValueError(f"Not a CWL process in $graph: {entry!r:.80}")
        # Entries are found by id: one without, or a repeated one, would be
        # silently dropped
        entry_id = entry.get("id", default_id)
        problem = None
        if entry_id is None:
            problem = f"A $graph entry has no id: {entry!r:.80}"
        elif not isinstance(entry_id, str):
            problem = f"A $graph entry has an id that is not a string: {entry!r:.80}"
        elif entry_id in graph:
            problem = f"Two $graph entries have the id {entry_id!r}"
        if problem is not None:
            if problems is None:
                raise ValueError(problem)
            problems.append(problem)
            continue
        _check_version(entry, str(entry_id))
        graph[str(entry_id)] = entry
    return graph


def _is_process(value: Any) -> bool:
    # A class of another type, which may not hash, is not one of them either
    if not isinstance(value, dict) or not isinstance(value.get("class"), str):
        return False
    return value["class"] in PROCESS_CLASSES


def _check_steps(
    process: dict[str, Any], entry_id: str, path: str = "", depth: int = 0
) -> None:
    """Refuse steps :func:`_runs` cannot walk, down through inline bodies."""
    steps = process.get("steps") or []
    if isinstance(steps, dict):
        steps = list(steps.values())
    if not isinstance(steps, list) or not all(isinstance(s, dict) for s in steps):
        raise ValueError(
            f"{entry_id}: steps at {path or '/'} are not a list or map of objects"
        )
    for step in steps:
        if not isinstance(step.get("run"), dict):
            continue
        if depth == MAX_INLINE_DEPTH:
            raise ValueError(
                f"{entry_id}: inline processes are nested more than "
                f"{MAX_INLINE_DEPTH} deep; make them $graph entries and name "
                "them in run"
            )
        where = f"{path}/{step.get('id', '?')}"
        _check_steps(step["run"], entry_id, where, depth + 1)


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


def _replace_runs(process: dict[str, Any], runs: list[str]) -> dict[str, Any]:
    """A copy of a workflow whose steps run ``runs``, in step order."""
    steps = process.get("steps")
    if not steps:
        # Nothing runs: keep the steps as submitted, {} and [] alike
        return process
    if isinstance(steps, dict):
        steps = {
            k: {**s, "run": r} for (k, s), r in zip(steps.items(), runs, strict=True)
        }
    else:
        steps = [{**s, "run": r} for s, r in zip(steps, runs, strict=True)]
    return {**process, "steps": steps}


def _check_version(process: dict[str, Any], where: str) -> None:
    # A body may repeat the document's version, which is hashed once; another
    # one would be dropped from the hash and pass for v1.2
    version = process.get("cwlVersion", CWL_VERSION)
    if version != CWL_VERSION:
        raise ValueError(
            f"{where} declares cwlVersion {version!r} in a {CWL_VERSION} document"
        )


def _names(value: Any, path: str = "") -> Iterator[tuple[str, str]]:
    """Every key and string of a value, with where it is."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield from _names(key, path)
            yield from _names(item, f"{path}/{key}")
    elif isinstance(value, list):
        for i, item in enumerate(value):
            yield from _names(item, f"{path}/{i}")
    elif isinstance(value, str):
        yield path or "/", value


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
