"""Load a CWL document from files into one self-contained JSON value.

CWL is written in YAML, but the services only ever receive JSON: the CLI
resolves a document here and submits the result. Before a process can be
canonicalised (:mod:`diracx.core.cwl.canonical`), everything it refers
to on disk has to be inlined: ``run`` paths, ``$import`` and ``$include``
directives, and ``#fragment`` references into a ``$graph``. References to
stored processes (``proc:sha256-...``) are kept as they are.

Files are parsed as YAML 1.2, as CWL runners do, so a value such as ``yes``
stays the string it is in the document a user tested.
"""

from __future__ import annotations

__all__ = ["DocumentLoadError", "load_document"]

import copy
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from ruamel.yaml import YAML
from ruamel.yaml.constructor import SafeConstructor

from diracx.core.cwl.canonical import REFERENCE_PREFIX

MAX_DEPTH = 16
MAX_FILE_BYTES = 4 * 1024 * 1024


class _NoTimestampConstructor(SafeConstructor):
    """Keep timestamps as the strings they were written as, like schema-salad."""


_NoTimestampConstructor.add_constructor(
    "tag:yaml.org,2002:timestamp", SafeConstructor.construct_yaml_str
)


class DocumentLoadError(ValueError):
    """A CWL document, or a file it refers to, cannot be loaded."""


def load_document(
    path: str | Path,
    *,
    max_depth: int = MAX_DEPTH,
    max_file_bytes: int = MAX_FILE_BYTES,
) -> dict[str, Any]:
    """Load a CWL document and inline everything it refers to on disk.

    Args:
        path: Path (or ``file://`` URI) of the document.
        max_depth: Deepest chain of ``run``/``$import`` references followed.
        max_file_bytes: Largest file read.

    Returns:
        The process, with every ``run`` an inline body or a stored-process
        reference. For a packed document, its ``#main`` process.

    Raises:
        DocumentLoadError: If a file is missing, too large, not a mapping,
            refers to itself, or nests too deeply.
    """
    loader = _Loader(max_depth, max_file_bytes)
    return loader.process(_path(str(path), Path.cwd()), stack=())


class _Loader:
    def __init__(self, max_depth: int, max_file_bytes: int):
        self.max_depth = max_depth
        self.max_file_bytes = max_file_bytes
        self.yaml = YAML(typ="safe", pure=True)
        self.yaml.version = (1, 2)
        self.yaml.Constructor = _NoTimestampConstructor

    def read(self, path: Path) -> str:
        try:
            size = path.stat().st_size
        except OSError as e:
            raise DocumentLoadError(f"Cannot read {path}: {e.strerror}") from e
        if size > self.max_file_bytes:
            raise DocumentLoadError(
                f"{path} is larger than {self.max_file_bytes} bytes"
            )
        return path.read_text()

    def parse(self, path: Path) -> Any:
        return self.yaml.load(self.read(path))

    def process(self, path: Path, stack: tuple[Path, ...], fragment: str = "") -> dict:
        if path in stack:
            chain = " -> ".join(str(p) for p in (*stack, path))
            raise DocumentLoadError(f"Reference cycle: {chain}")
        if len(stack) >= self.max_depth:
            raise DocumentLoadError(f"References nest deeper than {self.max_depth}")
        stack = (*stack, path)
        document = self.parse(path)
        if not isinstance(document, dict):
            raise DocumentLoadError(f"{path} is not a CWL document")
        graph = _graph(document)
        if graph is not None:
            name = fragment or "main"
            if name not in graph:
                raise DocumentLoadError(f"{path} has no process #{name}")
            process = copy.deepcopy(graph[name])
            process.setdefault("cwlVersion", document.get("cwlVersion"))
            if "$namespaces" in document:
                process["$namespaces"] = {
                    **document["$namespaces"],
                    **process.get("$namespaces", {}),
                }
        else:
            if fragment and str(document.get("id", "")).lstrip("#") != fragment:
                raise DocumentLoadError(f"{path} has no process #{fragment}")
            process = document
        return self.resolve(process, path.parent, stack, graph, path)

    def resolve(self, value: Any, base: Path, stack, graph, path: Path) -> Any:
        if isinstance(value, list):
            return [self.resolve(v, base, stack, graph, path) for v in value]
        if not isinstance(value, dict):
            return value
        if set(value) == {"$include"}:
            return self.read(_path(value["$include"], base))
        if set(value) == {"$import"}:
            return self.imported(value, base, stack, path, self.resolve)
        result = {}
        for key, item in value.items():
            if key == "steps" and value.get("class") == "Workflow":
                result[key] = self.steps(item, base, stack, graph, path)
            else:
                result[key] = self.resolve(item, base, stack, graph, path)
        return result

    def imported(self, directive: dict, base: Path, stack, path: Path, then) -> Any:
        """Parse the file an ``$import`` names, and resolve it with ``then``."""
        target = _path(directive["$import"], base)
        if target in stack or len(stack) >= self.max_depth:
            raise DocumentLoadError(f"Cannot $import {target} from {path}")
        return then(self.parse(target), target.parent, (*stack, target), None, target)

    def steps(self, steps: Any, base: Path, stack, graph, path: Path) -> Any:
        """Resolve a workflow's steps: ``run`` is a reference only on a step."""
        if isinstance(steps, dict) and set(steps) == {"$import"}:
            return self.imported(steps, base, stack, path, self.steps)
        if isinstance(steps, list):
            return [self.step(step, base, stack, graph, path) for step in steps]
        if isinstance(steps, dict) and not _is_directive(steps):
            return {
                name: self.step(step, base, stack, graph, path)
                for name, step in steps.items()
            }
        return self.resolve(steps, base, stack, graph, path)

    def step(self, step: Any, base: Path, stack, graph, path: Path) -> Any:
        if isinstance(step, dict) and set(step) == {"$import"}:
            return self.imported(step, base, stack, path, self.step)
        if not isinstance(step, dict) or _is_directive(step):
            return self.resolve(step, base, stack, graph, path)
        return {
            key: (
                self.run(item, base, stack, graph, path)
                if key == "run"
                else self.resolve(item, base, stack, graph, path)
            )
            for key, item in step.items()
        }

    def run(self, run: Any, base: Path, stack, graph, path: Path) -> Any:
        if isinstance(run, dict):
            return self.resolve(run, base, stack, graph, path)
        if not isinstance(run, str):
            raise DocumentLoadError(f"Invalid run in {path}: {run!r}")
        if run.startswith(REFERENCE_PREFIX):
            return run
        if run.startswith("#"):
            if graph is None or run[1:] not in graph:
                raise DocumentLoadError(f"{path} has no process {run}")
            if len(stack) >= self.max_depth:
                raise DocumentLoadError(f"References nest deeper than {self.max_depth}")
            body = copy.deepcopy(graph[run[1:]])
            return self.resolve(body, base, (*stack, Path(f"{path}{run}")), graph, path)
        target, _, fragment = run.partition("#")
        return self.process(_path(target, base), stack, fragment)


def _is_directive(value: dict) -> bool:
    return set(value) in ({"$include"}, {"$import"})


def _graph(document: dict) -> dict[str, dict] | None:
    if "$graph" not in document:
        return None
    return {str(entry.get("id", "")).lstrip("#"): entry for entry in document["$graph"]}


def _path(reference: str, base: Path) -> Path:
    if reference.startswith("file://"):
        return Path(unquote(urlparse(reference).path)).resolve()
    if "://" in reference:
        raise DocumentLoadError(f"Only local files can be loaded, not {reference}")
    return (base / reference).resolve()
