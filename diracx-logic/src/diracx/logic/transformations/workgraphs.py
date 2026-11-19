"""Workgraph submission: compile a CWL document into the schema (DX-ADR-007).

A workgraph is a CWL v1.2 ``Workflow`` carrying ``dirac:Workgraph``, in which
every step carries ``dirac:Transformation`` and becomes a compute
transformation, and each data-management declaration of the ``dirac:Workgraph``
hint becomes a data transformation. :func:`compile_workgraph` decomposes the
document into the definition-side rows of DX-ADR-004 without touching the
database, and :func:`submit_workgraph` stores them in one transaction.

The document is accepted as JSON, either as a single ``Workflow`` or packed
(``cwltool --pack``) as a ``$graph`` whose entry point is ``#main``. A step's
``run`` is an inline process or a ``#fragment`` into that graph.

Process ingestion is not here: validating the processes as CWL with
``cwl-utils``, canonicalising them for content identity, deduplicating nested
references and accepting references to already-stored processes are the job
of DIRACGrid/diracx#1080. Until it lands, :func:`ingest_process` is a
placeholder that hashes each ``run`` body as it was submitted.

Defaults from the configuration service are not resolved either, so the
fields they would fill (the workgraph ``type``, each transformation's
``packer``) are required for now.
"""

from __future__ import annotations

import copy
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ValidationError
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import InvalidWorkgraphError
from diracx.core.hashing import hash_json
from diracx.core.models.transformation import (
    ActionSpec,
    DataManagementHint,
    FeederHint,
    SubmittedTransformation,
    TransformationHint,
    TransformationKind,
    TransformationStatus,
    WorkgraphHint,
    WorkgraphStatus,
    WorkgraphSubmissionResult,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.db import (
    AnyUUID,
    NewAction,
    NewTransformation,
    NewTransformationOutput,
)

logger = logging.getLogger(__name__)

# The feeder serving an internal edge from the ParcelOutputs of the declared
# output named in its FeederArgs (DX-ADR-006). Provisional name: the edge
# feeder plugin is not implemented yet.
EDGE_FEEDER = "Edge"

# How the stripped spec refers to a stored process in place of its run body
PROCESS_URI_PREFIX = "diracx://"

# The longest names the definition columns hold (TransformationOutputs.Name)
MAX_NAME_LENGTH = 64

WORKGRAPH_HINT = "dirac:Workgraph"
TRANSFORMATION_HINT = "dirac:Transformation"
FEEDER_HINT = "dirac:Feeder"


@dataclass
class CompiledWorkgraph:
    """The definition-side rows of a workgraph, ready to be inserted.

    Attributes:
        workgraph_id: UUIDv7 identifier minted for the workgraph.
        type: VO-defined label selecting default behaviour.
        stripped_spec: The document with run bodies replaced by hashes.
        annotations: Human-facing labels of the workgraph.
        hooks: Single-hook bindings of the workgraph.
        processes: Process documents keyed by their content hash.
        requirements_templates: Requirement sets keyed by their content hash.
        transformations: The member transformations, steps first, in
            document order.
        names: Name of each member transformation, by identifier.
        outputs: Declared outputs of the compute members.
        actions: Lifecycle actions of the workgraph and its members.
    """

    workgraph_id: UUID
    type: str
    stripped_spec: dict[str, Any]
    annotations: dict[str, Any] | None
    hooks: dict[str, Any] | None
    processes: dict[bytes, dict[str, Any]] = field(default_factory=dict)
    requirements_templates: dict[bytes, dict[str, Any]] = field(default_factory=dict)
    transformations: list[NewTransformation] = field(default_factory=list)
    names: dict[AnyUUID, str] = field(default_factory=dict)
    outputs: list[NewTransformationOutput] = field(default_factory=list)
    actions: list[NewAction] = field(default_factory=list)


def ingest_process(process: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
    """Return the content hash a process is stored under, and what is stored.

    Placeholder for the process ingestion of DIRACGrid/diracx#1080, which
    validates the process with ``cwl-utils``, canonicalises it (``doc`` and
    ``label``, ``$import``) and stores nested references as references. Here
    the body is hashed as submitted, so two processes differing only in
    formatting-neutral fields are stored twice.

    Args:
        process: A CWL process, inline.

    Returns:
        The SHA-256 digest of its JCS form, and the document to store.
    """
    # TODO(#1080): canonicalise and validate before hashing
    return hash_json(process), process


async def submit_workgraph(
    document: dict[str, Any],
    *,
    transformation_db: TransformationDB,
    vo: str,
    sub: str,
    author: str,
) -> WorkgraphSubmissionResult:
    """Compile a workgraph document and store it, with its transformations.

    Everything is written on the connection of ``transformation_db``, so the
    workgraph is stored in full or not at all.

    Args:
        document: The CWL workgraph document, as JSON.
        transformation_db: Where to store the workgraph.
        vo: Virtual organisation of the submitter.
        sub: OIDC subject of the submitter.
        author: Who submitted, recorded in the audit logs.

    Returns:
        The identifiers of the workgraph and of its transformations.

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
        hooks=compiled.hooks,
        transformations=compiled.transformations,
        outputs=compiled.outputs,
        actions=compiled.actions,
        author=author,
    )
    logger.info(
        "Workgraph %s submitted by %s with %d transformations",
        compiled.workgraph_id,
        author,
        len(compiled.transformations),
    )
    return WorkgraphSubmissionResult(
        workgraph_id=uuid.UUID(bytes=compiled.workgraph_id.bytes),
        transformations=[
            SubmittedTransformation(
                transformation_id=uuid.UUID(bytes=t.transformation_id.bytes),
                name=compiled.names[t.transformation_id],
                kind=t.kind,
            )
            for t in compiled.transformations
        ],
    )


# --------------------------------------------------------------------------
# Compilation
# --------------------------------------------------------------------------


@dataclass
class _Step:
    """A step of the workflow, as the compiler needs it."""

    name: str
    body: dict[str, Any]
    hint: TransformationHint
    # Step input name -> the sources it reads, as local ids
    inputs: dict[str, list[str]]
    outputs: list[str]
    run: dict[str, Any]


class _Compiler:
    """One compilation: accumulates errors so that all are reported at once."""

    def __init__(self, document: Any):
        self.document = document
        self.errors: list[str] = []
        self.graph: dict[str, dict[str, Any]] = {}

    def fail(self) -> None:
        """Raise the errors found so far, if any."""
        if self.errors:
            raise InvalidWorkgraphError(self.errors)

    def validate(self, model: type[BaseModel], value: Any, where: str) -> Any:
        """Validate a hint against its model, recording any errors."""
        try:
            return model.model_validate(value)
        except ValidationError as exc:
            for error in exc.errors():
                loc = ".".join(str(part) for part in error["loc"])
                self.errors.append(f"{where}{'.' + loc if loc else ''}: {error['msg']}")
            return None

    # -- document shape ----------------------------------------------------

    def select_main(self) -> dict[str, Any]:
        """Find the workflow to compile, indexing a packed ``$graph``."""
        document = self.document
        if not isinstance(document, Mapping):
            self.errors.append("the document must be a JSON object")
            self.fail()
        if document.get("cwlVersion") != "v1.2":
            self.errors.append(
                f"cwlVersion must be v1.2, not {document.get('cwlVersion')!r}"
            )
        if "$graph" not in document:
            return dict(document)
        graph = document["$graph"]
        if not isinstance(graph, list):
            self.errors.append("$graph must be a list of processes")
            self.fail()
        for process in graph:
            if isinstance(process, Mapping) and isinstance(process.get("id"), str):
                self.graph[process["id"].lstrip("#")] = dict(process)
        if "main" not in self.graph:
            self.errors.append("a packed document must have a #main process")
            self.fail()
        return self.graph["main"]

    def hints(self, obj: Mapping[str, Any], where: str) -> dict[str, Any]:
        """Return the hints of a process or step, keyed by class.

        CWL writes hints either as a mapping keyed by class or as a list of
        objects carrying ``class``; both mean the same (DX-ADR-007).
        """
        hints = obj.get("hints")
        if hints is None:
            return {}
        if isinstance(hints, Mapping):
            return dict(hints)
        if not isinstance(hints, list):
            self.errors.append(f"{where}: hints must be a mapping or a list")
            return {}
        by_class: dict[str, Any] = {}
        for hint in hints:
            if not isinstance(hint, Mapping) or "class" not in hint:
                self.errors.append(f"{where}: every hint in a list needs a class")
                continue
            body = {k: v for k, v in hint.items() if k != "class"}
            if hint["class"] in by_class:
                self.errors.append(f"{where}: hint {hint['class']} appears twice")
            by_class[hint["class"]] = body
        return by_class

    def entries(
        self, value: Any, where: str, shorthand: str
    ) -> list[tuple[str, dict[str, Any]]]:
        """Return the ``(id, body)`` entries of a CWL map-or-list field.

        Args:
            value: The field, a mapping keyed by id or a list of objects
                carrying ``id``.
            where: Where the field is, for error messages.
            shorthand: The key a non-object mapping value stands for, such
                as ``type`` for an input or ``source`` for a step input.
        """
        if value is None:
            return []
        items: list[tuple[Any, Any]]
        if isinstance(value, Mapping):
            items = list(value.items())
        elif isinstance(value, list):
            items = []
            for entry in value:
                if isinstance(entry, str):
                    items.append((entry, {}))
                elif isinstance(entry, Mapping) and "id" in entry:
                    items.append((entry["id"], entry))
                else:
                    self.errors.append(f"{where}: every entry needs an id")
        else:
            self.errors.append(f"{where}: must be a mapping or a list")
            return []
        result = []
        for key, body in items:
            if not isinstance(body, Mapping):
                body = {shorthand: body}
            result.append((str(key), dict(body)))
        return result

    def resolve_run(self, run: Any, where: str, seen: frozenset[str]) -> Any:
        """Inline ``#fragment`` references into the packed graph.

        Other string references, such as a relative path or a reference to an
        already-stored process, need the process store of
        DIRACGrid/diracx#1080 and are rejected for now.
        """
        if isinstance(run, Mapping):
            run = dict(run)
            if "steps" in run:
                steps = run["steps"]
                pairs = (
                    steps.items() if isinstance(steps, Mapping) else enumerate(steps)
                )
                resolved: Any = {} if isinstance(steps, Mapping) else []
                for key, step in pairs:
                    if isinstance(step, Mapping) and "run" in step:
                        step = {
                            **step,
                            "run": self.resolve_run(step["run"], where, seen),
                        }
                    if isinstance(resolved, dict):
                        resolved[key] = step
                    else:
                        resolved.append(step)
                run["steps"] = resolved
            return run
        if isinstance(run, str) and run.startswith("#"):
            target = run[1:]
            if target in seen:
                self.errors.append(f"{where}: run {run} refers to itself")
                return {}
            if target not in self.graph:
                self.errors.append(f"{where}: run {run} is not in the $graph")
                return {}
            return self.resolve_run(self.graph[target], where, seen | {target})
        self.errors.append(
            f"{where}: run must be an inline process or a #fragment of a packed "
            f"document; references such as {run!r} are not supported yet"
        )
        return {}


def _local(ref: Any, *scopes: str) -> str:
    """Strip the ``#`` and the enclosing scopes from a CWL identifier.

    ``#main/MCSimulation/sim-files`` and ``MCSimulation/sim-files`` are the
    same reference, written packed and unpacked.
    """
    ref = str(ref).lstrip("#")
    for scope in scopes:
        if scope and ref.startswith(f"{scope}/"):
            ref = ref[len(scope) + 1 :]
    return ref


def _sources(value: Any) -> list[str]:
    """Return a step input's or workflow output's sources as a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return [str(value)]


def _labels(body: Mapping[str, Any], name: str | None) -> dict[str, Any]:
    """Collect the human-facing labels of a process or step (DX-ADR-004)."""
    labels: dict[str, Any] = {}
    if name is not None:
        labels["name"] = name
    for key in ("label", "doc"):
        if isinstance(body.get(key), str):
            labels[key] = body[key]
    return labels


def compile_workgraph(document: Any) -> CompiledWorkgraph:
    """Decompose a workgraph document into the rows of DX-ADR-004.

    Mints the identifiers of the workgraph, its transformations and their
    declared outputs, but touches no database.

    Args:
        document: The CWL workgraph document, as JSON.

    Returns:
        The rows to insert.

    Raises:
        InvalidWorkgraphError: Listing every problem found.
    """
    c = _Compiler(document)
    main = c.select_main()
    main_id = _local(main.get("id", "main"))

    if main.get("class") != "Workflow":
        c.errors.append(f"a workgraph must be a Workflow, not {main.get('class')!r}")
    hints = c.hints(main, "workflow")
    if WORKGRAPH_HINT not in hints:
        c.errors.append(f"the workflow must carry the {WORKGRAPH_HINT} hint")
    c.fail()
    wg_hint: WorkgraphHint | None = c.validate(
        WorkgraphHint, hints[WORKGRAPH_HINT], WORKGRAPH_HINT
    )
    c.fail()
    assert wg_hint is not None
    if wg_hint.type is None:
        # TODO: default from the configuration service (DX-ADR-007)
        c.errors.append(f"{WORKGRAPH_HINT}.type is required")

    # -- workflow inputs and their feeders ---------------------------------
    feeders: dict[str, FeederHint | None] = {}
    for name, body in c.entries(main.get("inputs"), "inputs", "type"):
        name = _local(name, main_id)
        feeder = None
        if FEEDER_HINT in body:
            feeder = c.validate(FeederHint, body[FEEDER_HINT], f"inputs.{name}")
        feeders[name] = feeder

    # -- steps -------------------------------------------------------------
    steps: dict[str, _Step] = {}
    for raw_name, body in c.entries(main.get("steps"), "steps", "run"):
        name = _local(raw_name, main_id)
        where = f"steps.{name}"
        if len(name) > MAX_NAME_LENGTH:
            c.errors.append(f"{where}: step names are at most 64 characters")
        step_hints = c.hints(body, where)
        if TRANSFORMATION_HINT not in step_hints:
            c.errors.append(
                f"{where}: every step of a workgraph must carry "
                f"{TRANSFORMATION_HINT}; a bare step is an error"
            )
            continue
        hint = c.validate(
            TransformationHint, step_hints[TRANSFORMATION_HINT], f"{where}.hints"
        )
        if hint is None:
            continue
        inputs = {
            _local(in_name, main_id, name): [
                _local(source, main_id) for source in _sources(in_body.get("source"))
            ]
            for in_name, in_body in c.entries(body.get("in"), f"{where}.in", "source")
        }
        outputs = [
            _local(out, main_id, name)
            for out, _ in c.entries(body.get("out"), f"{where}.out", "id")
        ]
        for out in outputs:
            if len(out) > MAX_NAME_LENGTH:
                c.errors.append(f"{where}.out.{out}: at most 64 characters")
        if "run" not in body:
            c.errors.append(f"{where}: a step needs a run")
            continue
        run = c.resolve_run(body["run"], f"{where}.run", frozenset())
        steps[name] = _Step(name, body, hint, inputs, outputs, run)
    if not steps and "steps" not in main:
        c.errors.append("a workgraph needs at least one step")
    c.fail()

    step_outputs = {f"{s.name}/{out}" for s in steps.values() for out in s.outputs}

    def is_step_output(ref: str) -> bool:
        return ref in step_outputs

    # -- workflow outputs ----------------------------------------------------
    declared: dict[str, list[str]] = {}
    for raw_name, body in c.entries(main.get("outputs"), "outputs", "type"):
        name = _local(raw_name, main_id)
        sources = [_local(s, main_id) for s in _sources(body.get("outputSource"))]
        for source in sources:
            if not is_step_output(source):
                c.errors.append(
                    f"outputs.{name}: outputSource {source} is not a step output"
                )
        declared[name] = sources
    if wg_hint.target is not None and wg_hint.target.output not in declared:
        c.errors.append(
            f"{WORKGRAPH_HINT}.target.output {wg_hint.target.output} "
            "is not a workflow output"
        )

    # -- dataflow --------------------------------------------------------------
    consumed: set[str] = {source for sources in declared.values() for source in sources}
    upstream: dict[str, set[str]] = {}
    for step in steps.values():
        upstream[step.name] = set()
        for in_name, sources in step.inputs.items():
            for source in sources:
                if is_step_output(source):
                    consumed.add(source)
                    upstream[step.name].add(source.split("/", 1)[0])
                elif source not in feeders:
                    c.errors.append(
                        f"steps.{step.name}.in.{in_name}: source {source} is "
                        "neither a workflow input nor a step output"
                    )
    _check_acyclic(upstream, c.errors)

    # -- data-management declarations: what each acts on -----------------------
    dm_names: list[str] = []
    dm_sources: list[str | None] = []
    for index, dm in enumerate(wg_hint.data_management):
        name = dm.id or f"{dm.operation}-{index}"
        where = f"{WORKGRAPH_HINT}.data_management.{name}"
        dm_names.append(name)
        dm_source: str | None = None
        files = _local(dm.files, main_id)
        if is_step_output(files):
            dm_source = files
        elif files in declared:
            if len(declared[files]) == 1:
                dm_source = declared[files][0]
            else:
                c.errors.append(f"{where}: files {files} has several sources")
        elif files in feeders and feeders[files] is not None:
            dm_source = files
        else:
            c.errors.append(
                f"{where}: files {files} is neither a step output, a workflow "
                f"output nor a workflow input with {FEEDER_HINT}"
            )
        if dm_source is not None and is_step_output(dm_source):
            consumed.add(dm_source)
        dm_sources.append(dm_source)

    for step in steps.values():
        for out in step.outputs:
            if f"{step.name}/{out}" not in consumed:
                c.errors.append(
                    f"steps.{step.name}.out.{out}: an output must be consumed by "
                    "a step or declared as a workflow output"
                )

    members = [*steps, *dm_names]
    seen_names: set[str] = set()
    for name in members:
        if name in seen_names:
            c.errors.append(f"transformation name {name} is used twice")
        seen_names.add(name)
    for name, after in [(s.name, s.hint.after) for s in steps.values()] + list(
        zip(dm_names, [dm.after for dm in wg_hint.data_management], strict=True)
    ):
        for other in after:
            if other not in seen_names:
                c.errors.append(f"{name}: after {other} is not a transformation")
    c.fail()

    # -- rows ----------------------------------------------------------------
    assert wg_hint.type is not None
    compiled = CompiledWorkgraph(
        workgraph_id=uuid7(),
        type=wg_hint.type,
        stripped_spec={},
        annotations=_labels(main, main_id) or None,
        hooks=wg_hint.hooks,
    )
    output_ids: dict[str, UUID] = {}
    process_ids: dict[str, bytes] = {}

    for step in steps.values():
        transformation_id = uuid7()
        compiled.names[transformation_id] = step.name
        for out in step.outputs:
            output_id = uuid7()
            output_ids[f"{step.name}/{out}"] = output_id
            compiled.outputs.append(
                NewTransformationOutput(
                    transformation_output_id=output_id,
                    transformation_id=transformation_id,
                    name=out,
                    intermediate=not any(
                        f"{step.name}/{out}" in sources for sources in declared.values()
                    ),
                )
            )
        process_id, process = ingest_process(step.run)
        compiled.processes[process_id] = process
        process_ids[step.name] = process_id

    def feeder_of(source: str) -> tuple[str | None, dict[str, Any] | None]:
        if is_step_output(source):
            return EDGE_FEEDER, {"transformation_output_id": str(output_ids[source])}
        feeder = feeders[source]
        assert feeder is not None
        return feeder.name, feeder.args

    transformation_ids = list(compiled.names)
    for transformation_id, step in zip(transformation_ids, steps.values(), strict=True):
        where = f"steps.{step.name}"
        feeder, feeder_args = None, None
        driving = _driving_input(step, feeders, is_step_output, c.errors)
        if driving is not None:
            feeder, feeder_args = feeder_of(step.inputs[driving][0])
        if step.hint.packer is None:
            # TODO: default from the configuration service (DX-ADR-007)
            c.errors.append(f"{where}.hints.{TRANSFORMATION_HINT}.packer is required")
            continue
        requirements_id = None
        if step.hint.requirements_template is not None:
            requirements_id = hash_json(step.hint.requirements_template)
            compiled.requirements_templates[requirements_id] = (
                step.hint.requirements_template
            )
        compiled.transformations.append(
            NewTransformation(
                transformation_id=transformation_id,
                kind=TransformationKind.COMPUTE,
                type=wg_hint.type,
                packer=step.hint.packer.name,
                packer_args=step.hint.packer.args,
                feeder=feeder,
                feeder_args=feeder_args,
                annotations=_labels(step.body, step.name),
                hooks=step.hint.hooks,
                process_id=process_ids[step.name],
                requirements_template_id=requirements_id,
            )
        )
        _actions(compiled, transformation_id, step.hint.actions, where, c.errors)

    for name, dm, source in zip(
        dm_names, wg_hint.data_management, dm_sources, strict=True
    ):
        where = f"{WORKGRAPH_HINT}.data_management.{name}"
        if dm.packer is None:
            # TODO: default from the configuration service (DX-ADR-007)
            c.errors.append(f"{where}.packer is required")
            continue
        assert source is not None
        transformation_id = uuid7()
        compiled.names[transformation_id] = name
        feeder, feeder_args = feeder_of(source)
        compiled.transformations.append(
            NewTransformation(
                transformation_id=transformation_id,
                kind=TransformationKind.DATA,
                type=wg_hint.type,
                packer=dm.packer.name,
                packer_args=dm.packer.args,
                feeder=feeder,
                feeder_args=feeder_args,
                annotations={"name": name, "data_management": _dm_request(dm)},
                hooks=None,
                process_id=None,
                requirements_template_id=None,
            )
        )
        _actions(compiled, transformation_id, dm.actions, where, c.errors)

    for position, spec in enumerate(wg_hint.approving):
        compiled.actions.append(
            NewAction(
                workgraph_id=compiled.workgraph_id,
                transformation_id=None,
                state=WorkgraphStatus.APPROVING.value,
                position=position,
                action=spec.action,
                args=spec.args,
            )
        )
    c.fail()

    compiled.stripped_spec = _strip(document, main, steps, process_ids)
    return compiled


def _check_acyclic(upstream: dict[str, set[str]], errors: list[str]) -> None:
    """Record an error if the step dataflow has a cycle."""
    done: set[str] = set()
    visiting: set[str] = set()

    def visit(name: str) -> bool:
        if name in done:
            return True
        if name in visiting:
            errors.append(f"steps.{name}: the dataflow has a cycle")
            return False
        visiting.add(name)
        ok = all(visit(parent) for parent in upstream.get(name, ()))
        visiting.discard(name)
        done.add(name)
        return ok

    for name in upstream:
        if not visit(name):
            return


def _driving_input(
    step: _Step,
    feeders: Mapping[str, FeederHint | None],
    is_step_output: Any,
    errors: list[str],
) -> str | None:
    """Return the step input that drives the transformation's pool, if any.

    An input is fed when it reads an upstream step output (the edge feeder)
    or a workflow input carrying ``dirac:Feeder``; every other input is a job
    parameter. With several fed inputs, ``driving_input`` must pick one.
    """
    where = f"steps.{step.name}"

    def fed(sources: list[str]) -> bool:
        return any(is_step_output(s) or feeders.get(s) is not None for s in sources)

    fed_inputs = [name for name, sources in step.inputs.items() if fed(sources)]
    driving = step.hint.driving_input
    if driving is not None:
        if driving not in step.inputs:
            errors.append(f"{where}: driving_input {driving} is not a step input")
            return None
        if driving not in fed_inputs:
            errors.append(f"{where}: driving_input {driving} is not fed")
            return None
    elif len(fed_inputs) > 1:
        errors.append(
            f"{where}: inputs {', '.join(fed_inputs)} are all fed; "
            "set driving_input to the one that drives the pool"
        )
        return None
    elif not fed_inputs:
        return None
    else:
        driving = fed_inputs[0]
    if len(step.inputs[driving]) != 1:
        # TODO: several sources merged into one input (MultipleInputFeature)
        errors.append(f"{where}.in.{driving}: a fed input reads exactly one source")
        return None
    return driving


def _actions(
    compiled: CompiledWorkgraph,
    transformation_id: AnyUUID,
    actions: Mapping[str, list[ActionSpec]],
    where: str,
    errors: list[str],
) -> None:
    """Append the lifecycle actions of a transformation, checking the states."""
    states = {status.value for status in TransformationStatus}
    for state, specs in actions.items():
        if state not in states:
            errors.append(f"{where}.actions: {state} is not a transformation state")
            continue
        for position, spec in enumerate(specs):
            compiled.actions.append(
                NewAction(
                    workgraph_id=None,
                    transformation_id=transformation_id,
                    state=state,
                    position=position,
                    action=spec.action,
                    args=spec.args,
                )
            )


def _dm_request(dm: DataManagementHint) -> dict[str, Any]:
    """What a data transformation's packer asks the data backend to do."""
    # Provisional home: the packer that turns this into DataParcel requests
    # (DX-ADR-008) is not implemented yet
    return dm.model_dump(
        include={"operation", "destination", "storage"}, exclude_none=True
    )


def _strip(
    document: Mapping[str, Any],
    main: Mapping[str, Any],
    steps: Mapping[str, _Step],
    process_ids: Mapping[str, bytes],
) -> dict[str, Any]:
    """Return the workflow with each step's run replaced by its hash.

    A packed document is reduced to its ``#main`` workflow, carrying the
    top-level ``cwlVersion`` and ``$namespaces``.
    """
    stripped = copy.deepcopy(dict(main))
    for key in ("cwlVersion", "$namespaces"):
        if key in document:
            stripped[key] = copy.deepcopy(document[key])
    main_id = _local(main.get("id", "main"))
    raw_steps = stripped.get("steps")
    pairs = (
        raw_steps.items()
        if isinstance(raw_steps, Mapping)
        else ((s.get("id"), s) for s in raw_steps or [])
    )
    for raw_name, body in pairs:
        name = _local(raw_name, main_id)
        if name in steps and isinstance(body, dict):
            body["run"] = f"{PROCESS_URI_PREFIX}{process_ids[name].hex()}"
    return stripped
