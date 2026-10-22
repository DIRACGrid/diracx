"""Canonical form and content identity of CWL processes (DX-ADR-004, DX-ADR-007).

A process is identified by the SHA-256 of its canonical byte form. The form is
defined over the parsed document (a plain JSON value), not over the output of
any CWL library, so that it is deterministic, stable across library upgrades
and reproducible by any tool. Version 1 of the rules:

1. Only CWL v1.2 is accepted.
2. On every process, ``id``, ``$namespaces`` and ``$schemas`` are dropped: the
   identity of a process is its content, not its name or where it was loaded
   from. Before that, every key, ``class`` and ``format`` written with a bound
   prefix (``edam:format_2572``) is expanded to its full URI, using the
   bindings of the process and of the processes enclosing it, so the identity
   covers what a prefix means and not which prefix was chosen. The ``dirac``
   prefix is never expanded.
3. ``doc`` and ``label`` are dropped from processes, parameters, steps and
   schemas, but kept as names a user chose (a record field, an environment
   variable), inside ``default`` values and inside requirements and hints
   other than ``SchemaDefRequirement``, where they are data. ``dirac:`` fields,
   ``dirac:`` hints and anonymous schema names (``name: _:<uuid>``) are dropped.
   ``dirac:`` data is extracted into transformation columns at submission, so
   it is not part of what a parcel runs.
4. ``inputs``, ``outputs``, ``steps`` and a step's ``in`` are written in map
   form keyed by their short id; ``requirements`` and ``hints`` in map form
   keyed by class. Shorthand values (``x: File``, ``in: {x: source}``) are
   expanded to objects. A step's ``out`` is a sorted list of short ids.
5. A fully scoped identifier or reference (``file:///p/x.cwl#main/x``,
   ``#main/x``) is shortened to the part after ``#``, without the enclosing
   scope, so that absolute ``file://`` URIs never enter the identity. A
   relative one is already relative to that scope, as a CWL runner reads it,
   and is kept as written.
6. A child ``run`` body is replaced by a reference to its own identity, so the
   identity of a workflow covers those of its children (a Merkle tree). Child
   bodies are therefore canonicalised before their parents.
7. Type shorthands are expanded, as a CWL runner reads them: ``File[]`` is
   ``{type: array, items: File}`` and ``File?`` is ``[null, File]``. A named
   type (``SchemaDefRequirement``) and every reference to it are written by
   its short name, as are enum symbols and record field names. A
   process may only use named types it defines itself, so that its identity
   covers their definitions; :func:`pack` scopes them by component
   (``#main/Seed``), as a CWL runner requires.
8. The result is serialised as JSON with sorted keys and no whitespace, then
   hashed. Numbers use Python's ``json`` rendering, which matches RFC 8785
   for the integers and short decimals CWL documents contain in practice.

Documents must be resolved before canonicalisation: a schema-salad directive
left in one (``$import``, ``$include``, ``$mixin``, or any other ``$`` key
except a process's ``$namespaces`` and ``$schemas``) is refused, since the
identity would not cover what it refers to and a CWL runner would fetch it.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from typing import Any

CANONICAL_VERSION = 1

REFERENCE_PREFIX = "proc:sha256-"
PACKED_PREFIX = "#sha256-"

PROCESS_CLASSES = frozenset(
    {"CommandLineTool", "Workflow", "ExpressionTool", "Operation"}
)
PRIMITIVE_TYPES = frozenset(
    {
        "null",
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "string",
        "File",
        "Directory",
        "Any",
        "stdin",
        "stdout",
        "stderr",
    }
)
PROCESS_ONLY_FIELDS = frozenset({"id", "$namespaces", "$schemas"})
METADATA_FIELDS = frozenset({"doc", "label"})
DIRAC_PREFIX = "dirac:"
# Fields whose value is a reference to a parameter or step output
REFERENCE_FIELDS = frozenset({"source", "outputSource"})


@dataclass(frozen=True)
class CanonicalProcess:
    """One process of a canonicalised tree.

    Attributes:
        process_id: SHA-256 of ``body``.
        body: Canonical bytes, child ``run`` bodies replaced by references.
        children: Identities of the processes ``body`` references, sorted.
    """

    process_id: bytes
    body: bytes
    children: tuple[bytes, ...]


def canonical_bytes(value: Any) -> bytes:
    """Serialise a JSON value in canonical form (rule 7).

    Args:
        value: Plain JSON value.

    Returns:
        UTF-8 JSON with sorted keys and no insignificant whitespace.
    """
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def format_reference(process_id: bytes) -> str:
    """Return the ``run`` reference to a stored process.

    Args:
        process_id: Identity of the process.

    Returns:
        The reference, e.g. ``proc:sha256-<hex>``.
    """
    return REFERENCE_PREFIX + process_id.hex()


def parse_reference(reference: str) -> bytes:
    """Return the identity a ``run`` reference points to.

    Args:
        reference: A reference made by :func:`format_reference`.

    Returns:
        The 32-byte identity.

    Raises:
        ValueError: If ``reference`` is not a process reference.
    """
    if not reference.startswith(REFERENCE_PREFIX):
        raise ValueError(f"Not a process reference: {reference!r}")
    process_id = bytes.fromhex(reference.removeprefix(REFERENCE_PREFIX))
    if len(process_id) != 32:
        raise ValueError(f"Not a SHA-256 process reference: {reference!r}")
    return process_id


def decompose(process: dict[str, Any]) -> tuple[bytes, dict[bytes, CanonicalProcess]]:
    """Canonicalise a process and every process nested in it.

    Args:
        process: A CWL process as a plain JSON value, with every ``run`` either
            an inline body or a process reference.

    Returns:
        The identity of ``process`` and every distinct process of the tree,
        keyed by identity.

    Raises:
        ValueError: If the document is not a CWL v1.2 process, or a ``run``
            path or schema-salad directive has not been resolved.
    """
    _check_resolved(process)
    nodes: dict[bytes, CanonicalProcess] = {}
    root = _decompose(process, nodes, process.get("cwlVersion"), {})
    return root, nodes


def pack(root: bytes, nodes: dict[bytes, CanonicalProcess]) -> dict[str, Any]:
    """Assemble a tree into one self-contained packed document.

    The root is ``#main`` and every other process is ``#sha256-<hex>``, so the
    document runs with a stock CWL runner and still names each component by
    its identity.

    Args:
        root: Identity of the root process.
        nodes: Processes of the tree, at least every one reachable from root.

    Returns:
        A CWL document with a ``$graph``.
    """
    graph = []
    for process_id in _reachable(root, nodes):
        body = json.loads(nodes[process_id].body)
        body.pop("cwlVersion", None)
        body["id"] = "#main" if process_id == root else PACKED_PREFIX + process_id.hex()
        _scope_types(body, body["id"])
        for step in (body.get("steps") or {}).values():
            if isinstance(step.get("run"), str):
                step["run"] = PACKED_PREFIX + parse_reference(step["run"]).hex()
        graph.append(body)
    return {"cwlVersion": "v1.2", "$graph": graph}


def unpack(packed: dict[str, Any]) -> tuple[bytes, dict[bytes, CanonicalProcess]]:
    """Recover the canonical tree from a document made by :func:`pack`.

    Args:
        packed: The packed document.

    Returns:
        The identity of the root and the processes of the tree.

    Raises:
        ValueError: If the document was not made by :func:`pack` or a
            component does not match the identity it is filed under.
    """
    by_id = {entry["id"]: entry for entry in packed["$graph"]}
    nodes: dict[bytes, CanonicalProcess] = {}
    # Each entry is verified once, however many steps share it
    resolved: dict[str, bytes] = {}
    in_progress: set[str] = set()

    def resolve(entry_id: str) -> bytes:
        if entry_id in resolved:
            return resolved[entry_id]
        if entry_id in in_progress:
            raise ValueError(f"{entry_id} runs itself")
        in_progress.add(entry_id)
        body = copy.deepcopy(by_id[entry_id])
        body.pop("id")
        _unscope_types(body, entry_id)
        body["cwlVersion"] = packed["cwlVersion"]
        children = set()
        steps = {}
        for name, step in (body.get("steps") or {}).items():
            step = dict(step)
            if isinstance(step.get("run"), str):
                child = resolve(step["run"])
                children.add(child)
                step["run"] = format_reference(child)
            steps[name] = step
        if steps:
            body["steps"] = steps
        data = canonical_bytes(body)
        process_id = hashlib.sha256(data).digest()
        if entry_id != "#main" and entry_id != PACKED_PREFIX + process_id.hex():
            raise ValueError(f"{entry_id} does not match its content")
        nodes[process_id] = CanonicalProcess(process_id, data, tuple(sorted(children)))
        in_progress.discard(entry_id)
        resolved[entry_id] = process_id
        return process_id

    return resolve("#main"), nodes


def _reachable(root: bytes, nodes: dict[bytes, CanonicalProcess]) -> list[bytes]:
    order, seen, stack = [], set(), [root]
    while stack:
        process_id = stack.pop()
        if process_id in seen:
            continue
        seen.add(process_id)
        order.append(process_id)
        stack.extend(reversed(nodes[process_id].children))
    return order


def _check_resolved(value: Any) -> None:
    """Refuse schema-salad directives, which would make a runner fetch files."""
    if isinstance(value, dict):
        is_process = value.get("class") in PROCESS_CLASSES
        for key, item in value.items():
            if str(key).startswith("$") and not (
                is_process and key in PROCESS_ONLY_FIELDS
            ):
                raise ValueError(
                    f"Unresolved {key} directive: resolve the document before "
                    "submitting it"
                )
            _check_resolved(item)
    elif isinstance(value, list):
        for item in value:
            _check_resolved(item)


def _decompose(
    process: dict[str, Any],
    nodes: dict[bytes, CanonicalProcess],
    cwl_version: Any,
    namespaces: dict[str, str],
) -> bytes:
    if process.get("class") not in PROCESS_CLASSES:
        raise ValueError(f"Not a CWL process: class={process.get('class')!r}")
    version = process.get("cwlVersion", cwl_version)
    if version != "v1.2":
        raise ValueError(
            f"Only CWL v1.2 is supported, got {version!r}: upgrade the document "
            "with cwl-upgrader"
        )

    namespaces = {**namespaces, **(process.get("$namespaces") or {})}
    namespaces.pop(DIRAC_PREFIX.rstrip(":"), None)
    process = _qualify(process, namespaces, top=True)

    own_name = _short(str(process.get("id", "")), None) or None
    body: dict[str, Any] = {}
    children: set[bytes] = set()
    for key, value in process.items():
        if key in PROCESS_ONLY_FIELDS or _is_dropped(key):
            continue
        if key in ("inputs", "outputs"):
            body[key] = _parameters(value, own_name)
        elif key in ("requirements", "hints"):
            classes = _by_class(value, drop_dirac=key == "hints")
            if classes:
                body[key] = classes
        elif key == "steps":
            body[key] = {
                name: _step(
                    step,
                    own_name,
                    f"{own_name}/{name}" if own_name else name,
                    nodes,
                    version,
                    namespaces,
                    children,
                )
                for name, step in _by_id(value, own_name).items()
            }
        else:
            body[key] = _strip(value)
    _name_types(body, own_name)
    body["cwlVersion"] = version

    data = canonical_bytes(body)
    process_id = hashlib.sha256(data).digest()
    nodes[process_id] = CanonicalProcess(process_id, data, tuple(sorted(children)))
    return process_id


def _step(
    step: dict[str, Any],
    own_name: str | None,
    step_scope: str,
    nodes: dict[bytes, CanonicalProcess],
    version: Any,
    namespaces: dict[str, str],
    children: set[bytes],
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in step.items():
        if _is_dropped(key):
            continue
        if key == "run":
            if isinstance(value, dict):
                child = _decompose(value, nodes, version, namespaces)
            elif isinstance(value, str) and value.startswith(REFERENCE_PREFIX):
                child = parse_reference(value)
            else:
                raise ValueError(f"Unresolved run reference: {value!r}")
            children.add(child)
            out[key] = format_reference(child)
        elif key == "in":
            out[key] = {
                name: _strip(_refs(spec, own_name))
                for name, spec in _by_id(value, step_scope, shorthand="source").items()
            }
        elif key == "out":
            out[key] = sorted(
                _short(item if isinstance(item, str) else item["id"], step_scope)
                for item in value
            )
        elif key in ("requirements", "hints"):
            classes = _by_class(value, drop_dirac=key == "hints")
            if classes:
                out[key] = classes
        else:
            out[key] = _strip(
                _refs(value, own_name) if key in REFERENCE_FIELDS else value
            )
    return out


def _type_definitions(body: dict[str, Any]) -> list[dict[str, Any]]:
    schema = (body.get("requirements") or {}).get("SchemaDefRequirement") or {}
    return [d for d in schema.get("types") or [] if isinstance(d, dict)]


def _map_types(body: dict[str, Any], rename) -> None:
    """Apply ``rename`` to every type name a process's types refer to."""
    schema = (body.get("requirements") or {}).get("SchemaDefRequirement")
    if schema and schema.get("types"):
        schema["types"] = [_map_type(d, rename) for d in schema["types"]]
    for side in ("inputs", "outputs"):
        for spec in (body.get(side) or {}).values():
            if isinstance(spec, dict) and "type" in spec:
                spec["type"] = _map_type(spec["type"], rename)


def _map_type(value: Any, rename) -> Any:
    """Expand shorthands and rename type names in a type or record field."""
    if isinstance(value, str):
        return _expand_type(value, rename)
    if isinstance(value, list):
        union: list[Any] = []
        for member in value:
            mapped = _map_type(member, rename)
            # File? in a union is null plus File: a union cannot nest
            for item in (
                mapped
                if isinstance(member, str) and isinstance(mapped, list)
                else [mapped]
            ):
                if item not in union:
                    union.append(item)
        return union
    if not isinstance(value, dict):
        return value
    value = dict(value)
    if value.get("type") not in ("record", "enum", "array") and "type" in value:
        value["type"] = _map_type(value["type"], rename)
    if "items" in value:
        value["items"] = _map_type(value["items"], rename)
    fields = value.get("fields")
    if isinstance(fields, dict):
        value["fields"] = {n: _map_type(f, rename) for n, f in fields.items()}
    elif isinstance(fields, list):
        value["fields"] = [
            {**_map_type(f, rename), "name": _leaf(f["name"])}
            if isinstance(f, dict) and "name" in f
            else _map_type(f, rename)
            for f in fields
        ]
    if isinstance(value.get("symbols"), list):
        value["symbols"] = [_leaf(s) for s in value["symbols"]]
    return value


def _expand_type(value: str, rename) -> Any:
    """``X?`` is ``[null, X]`` and ``X[]`` is ``{type: array, items: X}``."""
    if value.endswith("?"):
        return ["null", _expand_type(value.removesuffix("?"), rename)]
    if value.endswith("[]"):
        return {
            "type": "array",
            "items": _expand_type(value.removesuffix("[]"), rename),
        }
    return rename(value)


def _leaf(name: Any) -> Any:
    """``red`` for an enum symbol or field written ``#main/Colour/red``."""
    if isinstance(name, str) and "#" in name:
        return name.split("#", 1)[1].rsplit("/", 1)[-1]
    return name


def _name_types(body: dict[str, Any], own_name: str | None) -> None:
    """Write named types and references to them by short name (rule 7)."""
    definitions = _type_definitions(body)
    names = set()
    for definition in definitions:
        definition["name"] = _short(str(definition["name"]), own_name)
        names.add(definition["name"])

    def rename(name: str) -> str:
        if name in PRIMITIVE_TYPES:
            return name
        short = _short(name, own_name)
        if short not in names:
            raise ValueError(
                f"Type {name!r} is not defined by the process using it: define "
                "it in that process's own SchemaDefRequirement"
            )
        return short

    _map_types(body, rename)


def _scope_types(body: dict[str, Any], entry_id: str) -> None:
    """Scope a packed component's named types by its id, as runners require."""
    definitions = _type_definitions(body)
    names = {definition["name"] for definition in definitions}
    if not names:
        return
    for definition in definitions:
        definition["name"] = f"{entry_id}/{definition['name']}"
    _map_types(body, lambda n: f"{entry_id}/{n}" if n in names else n)


def _unscope_types(body: dict[str, Any], entry_id: str) -> None:
    prefix = f"{entry_id}/"
    for definition in _type_definitions(body):
        definition["name"] = str(definition["name"]).removeprefix(prefix)
    _map_types(body, lambda n: n.removeprefix(prefix))


def _qualify(value: Any, namespaces: dict[str, str], *, top: bool = False) -> Any:
    """Expand prefixed keys, classes and formats to full URIs (rule 2).

    Nested processes are left alone: they are expanded with their own
    bindings, which may rebind a prefix, when they are canonicalised.
    """
    if not namespaces:
        return value
    if isinstance(value, list):
        return [_qualify(v, namespaces) for v in value]
    if not isinstance(value, dict):
        return value
    if not top and value.get("class") in PROCESS_CLASSES:
        return value
    result = {}
    for key, item in value.items():
        if key in ("default", "$namespaces"):
            result[key] = item
        elif key in ("class", "format"):
            result[key] = (
                [_expand_name(v, namespaces) for v in item]
                if isinstance(item, list)
                else _expand_name(item, namespaces)
            )
        else:
            result[_expand_name(key, namespaces)] = _qualify(item, namespaces)
    return result


def _expand_name(name: Any, namespaces: dict[str, str]) -> Any:
    if not isinstance(name, str):
        return name
    prefix, sep, rest = name.partition(":")
    if sep and prefix in namespaces and not rest.startswith("//"):
        return namespaces[prefix] + rest
    return name


def _parameters(value: Any, own_name: str | None) -> dict[str, Any]:
    return {
        name: _strip(_refs(spec, own_name))
        for name, spec in _by_id(value, own_name, shorthand="type").items()
    }


def _by_id(
    value: Any, own_name: str | None, shorthand: str | None = None
) -> dict[str, Any]:
    """Map form of an id-keyed list (rule 4), with shorthand values expanded."""
    if isinstance(value, dict):
        items = [{"id": k, **_expand(v, shorthand)} for k, v in value.items()]
    else:
        items = [{"id": v} if isinstance(v, str) else v for v in value]
    result = {}
    for item in items:
        item = dict(item)
        name = _short(str(item.pop("id")), own_name)
        if name in result:
            raise ValueError(f"Duplicate id {name!r}")
        result[name] = item
    return result


def _expand(value: Any, shorthand: str | None) -> dict[str, Any]:
    # A map-form value that is an object is the parameter (or step input) itself
    if isinstance(value, dict) or shorthand is None:
        return value
    return {shorthand: value}


def _by_class(value: Any, *, drop_dirac: bool) -> dict[str, Any]:
    """Map form of a requirements or hints list keyed by class (rule 4).

    ``dirac:`` hints are dropped (rule 3). A ``dirac:`` requirement is kept,
    so that the policy refuses it: a runner must honour a requirement, and no
    CWL runner knows that class.
    """
    if isinstance(value, dict):
        items = [{"class": k, **(v or {})} for k, v in value.items()]
    else:
        items = list(value)
    result = {}
    for item in items:
        item = dict(item)
        cls = item.pop("class")
        if drop_dirac and str(cls).startswith(DIRAC_PREFIX):
            continue
        result[cls] = _strip(item, metadata=cls == "SchemaDefRequirement")
    return result


def _refs(value: Any, own_name: str | None) -> Any:
    """Shorten the references inside a parameter or step input (rule 5)."""
    if isinstance(value, dict):
        return {
            k: (_shorten_refs(v, own_name) if k in REFERENCE_FIELDS else v)
            for k, v in value.items()
        }
    return _shorten_refs(value, own_name)


def _shorten_refs(value: Any, own_name: str | None) -> Any:
    if isinstance(value, str):
        return _short(value, own_name)
    if isinstance(value, list):
        return [_short(v, own_name) if isinstance(v, str) else v for v in value]
    return value


def _short(identifier: str, scope: str | None) -> str:
    """Shorten an identifier relative to its enclosing scope (rule 5).

    Only a fully scoped identifier loses the scope: ``s0/out`` in a workflow
    whose own name is ``s0`` names the output of its step ``s0``.
    """
    if "#" in identifier:
        identifier = identifier.split("#", 1)[1]
        if scope and identifier.startswith(scope + "/"):
            identifier = identifier[len(scope) + 1 :]
    elif "://" in identifier or identifier.startswith("_:"):
        identifier = identifier.rsplit("/", 1)[-1]
    return identifier


def _is_dropped(key: str) -> bool:
    return key in METADATA_FIELDS or key.startswith(DIRAC_PREFIX)


def _strip(value: Any, *, metadata: bool = True) -> Any:
    """Drop ``dirac:`` fields, and metadata if asked, at every level (rule 3).

    ``default`` values are user data and kept verbatim, as are the names of a
    record's fields in map form. Anonymous schema names (``name: _:<uuid>``),
    which some CWL libraries mint afresh on every load, are dropped.
    """
    if isinstance(value, dict):
        result = {}
        for k, v in value.items():
            if k.startswith(DIRAC_PREFIX) or (metadata and k in METADATA_FIELDS):
                continue
            if k == "name" and isinstance(v, str) and v.startswith("_:"):
                continue
            if k == "default":
                result[k] = v
            elif k == "fields" and isinstance(v, dict):
                result[k] = {n: _strip(f, metadata=metadata) for n, f in v.items()}
            else:
                result[k] = _strip(v, metadata=metadata)
        return result
    if isinstance(value, list):
        return [_strip(v, metadata=metadata) for v in value]
    return value
