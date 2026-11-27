"""Workgraph submission: compile a CWL document into the schema (DX-ADR-007).

A workgraph is a CWL v1.2 ``Workflow`` carrying ``dirac:Workgraph`` whose
steps carry ``dirac:Transformation`` and become transformations. Deliverable 1
(DIRACGrid/diracx#1081) supports a workgraph made of a single compute
transformation: :func:`compile_workgraph` checks the document has exactly one
step and maps its hints to the columns of DX-ADR-004, :func:`submit_workgraph`
stores the rows ``New``, and :func:`start_workgraph` moves the workgraph and
its transformation to ``Active``, which is when the feeder starts.

The document is the one ``cwltool --pack`` makes: a ``$graph`` whose ``#main``
entry is the workgraph, its step's ``run`` naming another entry (``#gauss``)
or holding an inline body, and ``dirac:`` names written as full IRIs. It must
pass :func:`~diracx.core.cwl.check_self_contained`. Two pieces of the process
ingestion of DIRACGrid/diracx#1080 are not merged yet and have placeholders
here: reading the ``dirac:`` hints (``split_dirac_hints``, #1120) and storing
the process (``store_process``, #1118).
"""

from __future__ import annotations

import copy
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ValidationError
from uuid_utils import UUID, uuid7

from diracx.core.cwl import (
    NotSelfContainedError,
    check_self_contained,
    entry_identity,
    format_reference,
    subgraph,
)
from diracx.core.exceptions import (
    InvalidWorkgraphError,
    InvalidWorkgraphTransitionError,
)
from diracx.core.hashing import hash_json
from diracx.core.models.transformation import (
    TRANSFORMATION_TRANSITIONS,
    WORKGRAPH_TRANSITIONS,
    FeederHint,
    TransformationHint,
    TransformationStatus,
    WorkgraphHint,
    WorkgraphStatus,
    WorkgraphSubmissionResult,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.db import AnyUUID, NewTransformation

# The dirac: namespace; cwltool --pack writes its names as full IRIs
DIRAC_NAMESPACE = "https://diracgrid.org/cwl#"
WORKGRAPH_HINT = f"{DIRAC_NAMESPACE}Workgraph"
TRANSFORMATION_HINT = f"{DIRAC_NAMESPACE}Transformation"
FEEDER_HINT = f"{DIRAC_NAMESPACE}Feeder"


def _name(iri: str) -> str:
    """The prefixed name of a dirac: IRI, for messages."""
    return "dirac:" + iri.removeprefix(DIRAC_NAMESPACE)


# The entry of a packed document that is the workgraph
MAIN_ENTRY = "#main"


@dataclass
class CompiledWorkgraph:
    """The definition-side rows of a workgraph, ready to be inserted.

    Attributes:
        workgraph_id: UUIDv7 identifier minted for the workgraph.
        type: VO-defined label selecting default behaviour.
        stripped_spec: The document with the run body replaced by its hash.
        annotations: Human-facing labels of the workgraph.
        transformation: Its single compute transformation.
        processes: Process documents keyed by their content hash.
        requirements_templates: Requirement sets keyed by their content hash.
    """

    workgraph_id: UUID
    type: str
    stripped_spec: dict[str, Any]
    annotations: dict[str, Any] | None
    transformation: NewTransformation
    processes: dict[bytes, dict[str, Any]] = field(default_factory=dict)
    requirements_templates: dict[bytes, dict[str, Any]] = field(default_factory=dict)


def ingest_process(
    document: dict[str, Any], run: str | dict[str, Any]
) -> tuple[bytes, dict[str, Any]]:
    """Return the identity a step's process is stored under, and what is stored.

    Placeholder for ``store_process`` (DIRACGrid/diracx#1118), which also
    checks the process against the VO policy and validates it with
    ``cwl-utils`` before storing it. The identity and the stored document are
    already those of DIRACGrid/diracx#1114: what is stored is the document the
    step runs, which a runner can start.

    Args:
        document: The packed workgraph document; it is self-contained.
        run: The step's ``run``: a ``$graph`` entry id, or an inline body.

    Returns:
        The identity of the process, and the document to store under it.
    """
    # TODO(#1118): replace with store_process
    if isinstance(run, str):
        return entry_identity(document, run), subgraph(document, run)
    # An inline body is its own document; its identity is the same as inside
    # the workgraph, the version being hashed apart from the body
    process = {"cwlVersion": document["cwlVersion"], **run}
    if "$namespaces" in document:
        process["$namespaces"] = document["$namespaces"]
    return entry_identity(process, run.get("id", MAIN_ENTRY)), process


async def submit_workgraph(
    document: Any,
    *,
    transformation_db: TransformationDB,
    vo: str,
    sub: str,
) -> WorkgraphSubmissionResult:
    """Compile a workgraph document and store it, ``New``.

    Everything is written on the connection of ``transformation_db``, so the
    workgraph is stored in full or not at all.

    Args:
        document: The CWL workgraph document, as JSON.
        transformation_db: Where to store the workgraph.
        vo: Virtual organisation of the submitter.
        sub: OIDC subject of the submitter.

    Returns:
        The identifiers of the workgraph and of its transformation.

    Raises:
        InvalidWorkgraphError: If the document is not a valid workgraph.
    """
    compiled = compile_workgraph(document)
    await transformation_db.insert_processes(compiled.processes)
    for requirements_id, requirements in compiled.requirements_templates.items():
        await transformation_db.insert_requirements(requirements_id, requirements)
    await transformation_db.insert_workgraph(
        workgraph_id=compiled.workgraph_id,
        vo=vo,
        sub=sub,
        type=compiled.type,
        stripped_spec=compiled.stripped_spec,
        annotations=compiled.annotations,
        transformations=[compiled.transformation],
    )
    return WorkgraphSubmissionResult(
        workgraph_id=uuid.UUID(str(compiled.workgraph_id)),
        transformation_id=uuid.UUID(str(compiled.transformation.transformation_id)),
    )


async def start_workgraph(
    workgraph_id: AnyUUID, *, transformation_db: TransformationDB, author: str
) -> None:
    """Move a ``New`` workgraph and its transformations to ``Active``.

    Once ``Active``, the feeder of each transformation starts running. The
    moves are recorded in the workgraph and transformation audit logs.

    Args:
        workgraph_id: The workgraph to start.
        transformation_db: Where the workgraph is stored.
        author: Who started it, recorded in the audit logs.

    Raises:
        WorkgraphNotFoundError: If the workgraph does not exist.
        InvalidWorkgraphTransitionError: If the workgraph is not ``New``, or
            none of its transformations is.
    """
    from_status, to_status = WorkgraphStatus.NEW, WorkgraphStatus.ACTIVE
    assert to_status in WORKGRAPH_TRANSITIONS[from_status]
    assert (
        TransformationStatus.ACTIVE
        in TRANSFORMATION_TRANSITIONS[TransformationStatus.NEW]
    )

    status = (await transformation_db.get_workgraph(workgraph_id))["status"]
    if status != from_status or not await transformation_db.set_workgraph_status(
        workgraph_id, from_status, to_status, author
    ):
        raise InvalidWorkgraphTransitionError(str(status), to_status.value)
    moved = await transformation_db.set_workgraph_transformations_status(
        workgraph_id, TransformationStatus.NEW, TransformationStatus.ACTIVE, author
    )
    if moved == 0:
        # A New workgraph has only New members (WORKGRAPH_MEMBER_STATUSES):
        # raising rolls back the workgraph's own move with the transaction
        raise InvalidWorkgraphTransitionError(
            str(status), to_status.value, "none of its transformations is New"
        )


# --------------------------------------------------------------------------
# Compilation
# --------------------------------------------------------------------------


def _hints(obj: Mapping[str, Any], where: str, errors: list[str]) -> dict[str, Any]:
    """Return the hints of a process or step, keyed by class.

    CWL writes hints either as a mapping keyed by class or as a list of
    objects carrying ``class``; both mean the same (DX-ADR-007).
    """
    # TODO(#1120): replace with diracx.core.cwl.split_dirac_hints
    hints = obj.get("hints")
    if hints is None:
        return {}
    if isinstance(hints, Mapping):
        return dict(hints)
    if not isinstance(hints, list):
        errors.append(f"{where}: hints must be a mapping or a list")
        return {}
    by_class: dict[str, Any] = {}
    for hint in hints:
        if not isinstance(hint, Mapping) or "class" not in hint:
            errors.append(f"{where}: every hint in a list needs a class")
            continue
        cls = hint["class"]
        if not isinstance(cls, str):
            errors.append(f"{where}: a hint class must be a string, not {cls!r}")
            continue
        # A class appears at most once (DX-ADR-007)
        if cls in by_class:
            shown = _name(cls) if cls.startswith(DIRAC_NAMESPACE) else cls
            errors.append(f"{where}: hint {shown} appears more than once")
            continue
        by_class[cls] = {k: v for k, v in hint.items() if k != "class"}
    return by_class


def _entries(
    value: Any, where: str, shorthand: str, errors: list[str]
) -> list[tuple[str, dict[str, Any]]]:
    """Return the ``(id, body)`` entries of a CWL map-or-list field.

    ``shorthand`` is the key a non-object mapping value stands for, such as
    ``type`` for an input or ``source`` for a step input.
    """
    if value is None:
        return []
    if isinstance(value, Mapping):
        items = list(value.items())
    elif isinstance(value, list):
        items = []
        for entry in value:
            if isinstance(entry, Mapping) and "id" in entry:
                items.append((entry["id"], entry))
            else:
                errors.append(f"{where}: every entry needs an id")
    else:
        errors.append(f"{where}: must be a mapping or a list")
        return []
    entries: list[tuple[str, dict[str, Any]]] = []
    seen: set[str] = set()
    for key, body in items:
        if not isinstance(key, str):
            errors.append(f"{where}: an id must be a string, not {key!r}")
            continue
        key = key.lstrip("#")
        # A list cannot repeat an id, as a mapping cannot repeat a key
        if key in seen:
            errors.append(f"{where}: id {key} appears more than once")
            continue
        seen.add(key)
        entries.append(
            (key, dict(body) if isinstance(body, Mapping) else {shorthand: body})
        )
    return entries


def _validate(model: type[BaseModel], value: Any, where: str, errors: list[str]) -> Any:
    """Validate a hint against its model, recording any errors."""
    try:
        return model.model_validate(value)
    except ValidationError as exc:
        for error in exc.errors():
            loc = ".".join(str(part) for part in error["loc"])
            errors.append(f"{where}{'.' + loc if loc else ''}: {error['msg']}")
        return None


def _labels(body: Mapping[str, Any], name: str | None) -> dict[str, Any]:
    """Collect the human-facing labels of a process or step (DX-ADR-004)."""
    labels: dict[str, Any] = {} if name is None else {"name": name}
    for key in ("label", "doc"):
        if isinstance(body.get(key), str):
            labels[key] = body[key]
    return labels


def _local(ref: Any, *scopes: str) -> str:
    """Strip the ``#`` and the enclosing scopes from a CWL identifier.

    ``cwltool --pack`` writes ``#main/MCSimulation/seed`` where the unpacked
    document writes ``seed``.
    """
    ref = str(ref).lstrip("#")
    for scope in scopes:
        if scope and ref.startswith(f"{scope}/"):
            ref = ref[len(scope) + 1 :]
    return ref


def compile_workgraph(document: Any) -> CompiledWorkgraph:
    """Decompose a single-transformation workgraph into the rows of DX-ADR-004.

    Mints the identifiers of the workgraph and its transformation, but
    touches no database.

    Args:
        document: The packed CWL workgraph document, as JSON.

    Returns:
        The rows to insert.

    Raises:
        InvalidWorkgraphError: Listing every problem found.
    """
    if not isinstance(document, Mapping):
        raise InvalidWorkgraphError(["the document must be a JSON object"])
    document = dict(document)
    try:
        check_self_contained(document)
    except (NotSelfContainedError, ValueError) as exc:
        raise InvalidWorkgraphError([str(exc)]) from exc

    if "$graph" in document:
        graph = {entry["id"]: entry for entry in document["$graph"]}
        if MAIN_ENTRY not in graph:
            raise InvalidWorkgraphError(
                [f"a packed workgraph needs a {MAIN_ENTRY} entry"]
            )
        main = graph[MAIN_ENTRY]
    else:
        main = document
    main_id = _local(main.get("id", MAIN_ENTRY))

    errors: list[str] = []
    if main.get("class") != "Workflow":
        errors.append(f"a workgraph must be a Workflow, not {main.get('class')!r}")
    hints = _hints(main, "workflow", errors)
    wg_hint: WorkgraphHint | None = None
    if WORKGRAPH_HINT in hints:
        wg_hint = _validate(
            WorkgraphHint, hints[WORKGRAPH_HINT], _name(WORKGRAPH_HINT), errors
        )
    else:
        errors.append(f"the workflow must carry the {_name(WORKGRAPH_HINT)} hint")

    # Workflow inputs, with the feeder of those that carry one
    feeders: dict[str, FeederHint | None] = {}
    for raw_name, body in _entries(main.get("inputs"), "inputs", "type", errors):
        name = _local(raw_name, main_id)
        feeders[name] = None
        if FEEDER_HINT in body:
            feeders[name] = _validate(
                FeederHint,
                body[FEEDER_HINT],
                f"inputs.{name}.{_name(FEEDER_HINT)}",
                errors,
            )

    steps = _entries(main.get("steps"), "steps", "run", errors)
    if len(steps) != 1:
        errors.append(
            f"a workgraph must have exactly one step for now, not {len(steps)}"
        )
        raise InvalidWorkgraphError(errors)
    raw_name, step = steps[0]
    name = _local(raw_name, main_id)
    where = f"steps.{name}"

    step_hints = _hints(step, where, errors)
    hint: TransformationHint | None = None
    if TRANSFORMATION_HINT in step_hints:
        hint = _validate(
            TransformationHint,
            step_hints[TRANSFORMATION_HINT],
            f"{where}.{_name(TRANSFORMATION_HINT)}",
            errors,
        )
    else:
        errors.append(f"{where}: the step must carry {_name(TRANSFORMATION_HINT)}")

    # check_self_contained has made sure it is a $graph entry or a process
    run = step.get("run")
    if isinstance(run, Mapping) and any(
        isinstance(s.get("run"), str)
        for s in (
            run.get("steps", {}).values()
            if isinstance(run.get("steps"), Mapping)
            else run.get("steps", [])
        )
    ):
        errors.append(
            f"{where}.run: an inline process running $graph entries is not "
            "supported yet; name it by its $graph id instead"
        )

    # The step input fed from a workflow input carrying dirac:Feeder
    fed: list[tuple[str, FeederHint]] = []
    for raw_in, in_body in _entries(step.get("in"), f"{where}.in", "source", errors):
        in_name = _local(raw_in, main_id, name)
        source = in_body.get("source")
        sources = source if isinstance(source, list) else [source]
        for src in sources:
            src = _local(src, main_id)
            if src not in feeders:
                errors.append(
                    f"{where}.in.{in_name}: source {src} is not a workflow input"
                )
            elif (feeder := feeders[src]) is not None:
                if len(sources) != 1:
                    errors.append(
                        f"{where}.in.{in_name}: a fed input reads exactly one source"
                    )
                fed.append((in_name, feeder))
    if len(fed) > 1:
        errors.append(
            f"{where}: inputs {', '.join(n for n, _ in fed)} are all fed; "
            "only one may be for now"
        )

    if errors:
        raise InvalidWorkgraphError(errors)
    assert wg_hint is not None and hint is not None and run is not None

    process_id, process = ingest_process(
        document, run if isinstance(run, str) else dict(run)
    )
    requirements_id = None
    compiled_requirements: dict[bytes, dict[str, Any]] = {}
    if hint.requirements_template is not None:
        # TODO(#1213): hash with the Requirements model once it exists
        requirements_id = hash_json(hint.requirements_template)
        compiled_requirements[requirements_id] = hint.requirements_template
    feeder = fed[0][1] if fed else None

    # The workgraph's own entry, its step running the stored process
    stripped = copy.deepcopy(dict(main))
    for key in ("cwlVersion", "$namespaces"):
        if key in document:
            stripped[key] = copy.deepcopy(document[key])
    stripped_steps = stripped["steps"]
    stripped_step = (
        stripped_steps[next(iter(stripped_steps))]
        if isinstance(stripped_steps, Mapping)
        else stripped_steps[0]
    )
    stripped_step["run"] = format_reference(process_id)

    workgraph_labels = _labels(main, None)
    if isinstance(main.get("id"), str):
        workgraph_labels["id"] = main["id"]
    return CompiledWorkgraph(
        workgraph_id=uuid7(),
        type=wg_hint.type,
        stripped_spec=stripped,
        annotations=workgraph_labels or None,
        transformation=NewTransformation(
            transformation_id=uuid7(),
            type=wg_hint.type,
            packer=hint.packer.name,
            packer_args=hint.packer.args,
            feeder=feeder.name if feeder else None,
            feeder_args=feeder.args if feeder else None,
            annotations=_labels(step, name),
            process_id=process_id,
            requirements_template_id=requirements_id,
        ),
        processes={process_id: process},
        requirements_templates=compiled_requirements,
    )
