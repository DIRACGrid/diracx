"""Turn CWL files into what is submitted to DiracX (DX-ADR-007).

CWL is written as YAML files, but services only receive JSON: the CLI packs
the files with cwltool into one document, a ``$graph`` with one entry per
file, and takes DiracX's own ``dirac:`` hints out of it, since they say how
DiracX schedules the work rather than what runs. The services compute the
identity of what is left (``diracx.core.cwl``), so every client must pack with
cwltool for the same files to get the same identities.
"""

from __future__ import annotations

__all__ = ["DIRAC_NAMESPACE", "PackedSubmission", "pack"]

import contextlib
import json
import logging
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import urlsplit

from cwltool.context import LoadingContext
from cwltool.load_tool import fetch_document, resolve_and_validate_document
from cwltool.pack import pack as cwltool_pack
from schema_salad.exceptions import ValidationException
from schema_salad.fetcher import DefaultFetcher
from schema_salad.utils import json_dumps

from diracx.core.cwl import check_self_contained

# cwltool --pack writes the dirac: prefix expanded
DIRAC_NAMESPACE = "https://diracgrid.org/cwl#"

# cwltool names the file a stdout or stderr shorthand captures by a SHA-1 of
# the loaded document, which holds absolute paths
GENERATED_STREAM_NAME = re.compile(r"[0-9a-f]{40}")


class PackedSubmission(NamedTuple):
    """CWL files packed for submission.

    Attributes:
        document: The packed CWL document, without ``dirac:`` hints.
        hints: The ``dirac:`` hints and fields taken out of it, by the id of
            the object that held them and then by class or field name, e.g.
            ``{"#main/MCMerge": {"Transformation": {...}}}``.
    """

    document: dict[str, Any]
    hints: dict[str, dict[str, Any]]


def pack(path: Path) -> PackedSubmission:
    """Pack a CWL file, and the files it runs, for submission.

    Args:
        path: The CWL file to submit, e.g. a workgraph.

    Returns:
        The packed document and the ``dirac:`` hints taken out of it.

    Raises:
        schema_salad.exceptions.ValidationException: If cwltool cannot load
            the files, or they refer to anything but local files.
        diracx.core.cwl.NotSelfContainedError: If the packed document still
            needs something from outside itself, such as a ``File`` given by
            path.
    """
    document = _cwltool_pack(Path(path))
    _name_captured_streams(document)
    hints = _take_dirac_hints(document)
    check_self_contained(document)
    return PackedSubmission(document, hints)


def _cwltool_pack(path: Path) -> dict[str, Any]:
    """``cwltool --pack``, reading only local files."""
    loading_context = LoadingContext(
        {"do_update": False, "fetcher_constructor": _LocalOnly}
    )
    with _quiet("cwltool", "salad"):
        loading_context, document, uri = fetch_document(
            path.resolve().as_uri(), loading_context
        )
        loading_context, uri = resolve_and_validate_document(
            loading_context, document, uri, preprocess_only=True
        )
        packed = cwltool_pack(loading_context, uri)
    # As cwltool --pack prints it: a single process as itself
    graph = packed["$graph"]
    target = packed if len(graph) > 1 else graph[0]
    return json.loads(json_dumps(target, default=str))


class _LocalOnly(DefaultFetcher):
    """A fetcher that refuses anything but local files."""

    def fetch_text(self, url: str, content_types: list[str] | None = None) -> str:
        if urlsplit(url).scheme != "file":
            raise ValidationException(f"Refusing to fetch {url}: use a local file")
        return super().fetch_text(url, content_types)

    def check_exists(self, url: str) -> bool:
        if urlsplit(url).scheme not in ("", "file"):
            return False
        return super().check_exists(url)


@contextlib.contextmanager
def _quiet(*names: str) -> Iterator[None]:
    """Hide warnings, such as cwltool's about dirac: classes it does not know."""
    loggers = [logging.getLogger(name) for name in names]
    levels = [logger.level for logger in loggers]
    for logger in loggers:
        logger.setLevel(logging.ERROR)
    try:
        yield
    finally:
        for logger, level in zip(loggers, levels, strict=True):
            logger.setLevel(level)


def _name_captured_streams(document: dict[str, Any]) -> None:
    """Replace the names cwltool makes up for captured streams with fixed ones.

    CWL leaves the name of a ``stdout``/``stderr`` shorthand's file to the
    runner, and cwltool derives it from where the files were, which would
    give the same process another identity in another directory.
    """
    for process in _objects(document):
        if process.get("class") != "CommandLineTool":
            continue
        renames = {}
        for stream in ("stdout", "stderr"):
            name = process.get(stream)
            if isinstance(name, str) and GENERATED_STREAM_NAME.fullmatch(name):
                renames[name] = process[stream] = f"cwl.{stream}"
        for output in process.get("outputs") or []:
            binding = output.get("outputBinding") or {}
            if binding.get("glob") in renames:
                binding["glob"] = renames[binding["glob"]]


def _take_dirac_hints(document: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Remove ``dirac:`` hints and fields from a packed document, and return them."""
    taken: dict[str, dict[str, Any]] = {}
    for owner in _objects(document):
        owner_id = str(owner.get("id", "#main"))
        found = {}
        if isinstance(owner.get("hints"), list):
            kept = []
            for hint in owner["hints"]:
                cls = str(hint.get("class", ""))
                if cls.startswith(DIRAC_NAMESPACE):
                    body = {k: v for k, v in hint.items() if k != "class"}
                    found[cls.removeprefix(DIRAC_NAMESPACE)] = body
                else:
                    kept.append(hint)
            if kept:
                owner["hints"] = kept
            else:
                del owner["hints"]
        for key in [k for k in owner if str(k).startswith(DIRAC_NAMESPACE)]:
            found[key.removeprefix(DIRAC_NAMESPACE)] = owner.pop(key)
        if found:
            taken[owner_id] = _unscope(found, owner_id)
    return taken


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
