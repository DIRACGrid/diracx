"""What DiracX accepts in a submitted CWL process, beyond being valid CWL.

Checked on the packed document the client submits, after
:func:`.identity.check_self_contained`, so that every process DiracX stores
can run on any of its backends without fetching or building anything.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from diracx.core.hashing import canonical_json_bytes

from .identity import _graph

# Requirement classes every backend can honour, named as in the CWL v1.2
# specification (https://www.commonwl.org/v1.2/CommandLineTool.html and
# https://www.commonwl.org/v1.2/Workflow.html). Kept deliberately narrow:
# loosening it later is safe, tightening it would break documents already
# accepted.
ALLOWED_REQUIREMENTS = frozenset(
    {
        # Run with Apptainer, which grid sites and compute backends provide
        "DockerRequirement",
        "EnvVarRequirement",
        "InitialWorkDirRequirement",
        "InlineJavascriptRequirement",
        "LoadListingRequirement",
        "MultipleInputFeatureRequirement",
        "NetworkAccess",
        "ResourceRequirement",
        "ScatterFeatureRequirement",
        "SchemaDefRequirement",
        "ShellCommandRequirement",
        "StepInputExpressionRequirement",
        "SubworkflowFeatureRequirement",
        "ToolTimeLimit",
    }
)

# Hints a runner may ignore, so every CWL v1.2 class, but only those: cwltool
# fetches the IRI of a class it does not know.
ALLOWED_HINTS = ALLOWED_REQUIREMENTS | {
    "InplaceUpdateRequirement",
    "SoftwareRequirement",
    "WorkReuse",
}

# DiracX's own classes (``dirac:Transformation``…), accepted as hints only.
# cwltool --pack writes them with the prefix expanded, and
# check_self_contained refuses them written with it.
# TODO: any class under the namespace is accepted for now; list the classes
# DiracX defines once they are settled.
DIRAC_NAMESPACE = "https://diracgrid.org/cwl#"

# DockerRequirement fields that make the worker node build an image or fetch
# one from a URL at run time, rather than pull a published one: Apptainer
# cannot build from a Dockerfile there. Refused in hints too, since a runner
# with a container engine acts on a DockerRequirement hint.
UNSUPPORTED_DOCKER_FIELDS = ("dockerFile", "dockerImport", "dockerLoad")

# Largest canonical form of one $graph entry. A year of LHCb submissions
# peaks below 20 kB; large data belongs in a sandbox, not in the document.
MAX_PROCESS_BYTES = 256 * 1024

# Largest canonical form of the whole document. It also bounds each stored
# row, which is an entry with everything it runs.
MAX_DOCUMENT_BYTES = 1024 * 1024

# How deep inline run bodies may nest below a $graph entry. cwltool --pack
# makes an entry of every process a step runs, so real documents nest none or
# a handful; without a bound, a small document could nest enough of them to
# exhaust the stack of whatever walks it.
MAX_INLINE_DEPTH = 32


class PolicyViolationError(ValueError):
    """A process is valid CWL but not something DiracX accepts."""


def check_policy(document: dict[str, Any]) -> None:
    """Check every process of a packed document against the submission rules.

    This is the second check on a submission, after it has been found to be
    self-contained. The schema is only validated afterwards, so the document
    may not be valid CWL yet.

    Args:
        document: A packed CWL document that passes
            :func:`.identity.check_self_contained`, so every class is
            written bare or as a full IRI, never with a prefix.

    Raises:
        PolicyViolationError: Listing every violation found, each naming the
            ``$graph`` entry (or the step running an inline body) it is in.
    """
    problems = []
    try:
        size = len(canonical_json_bytes(document))
    except RecursionError:
        # Whatever is nested, nothing below could walk it either
        raise PolicyViolationError(
            "the document is nested too deeply to be read"
        ) from None
    if size > MAX_DOCUMENT_BYTES:
        problems.append(
            f"the document is {size} bytes, more than {MAX_DOCUMENT_BYTES}; "
            "move large content into a sandbox or File input"
        )
    for entry_id, entry in _graph(document).items():
        size = len(canonical_json_bytes(entry))
        if size > MAX_PROCESS_BYTES:
            problems.append(
                f"{entry_id} is {size} bytes, more than {MAX_PROCESS_BYTES}; "
                "move large content into a sandbox or File input"
            )
        problems.extend(_problems(entry_id, entry))
    if problems:
        raise PolicyViolationError("; ".join(problems))


def _problems(name: str, process: dict[str, Any], depth: int = 0) -> Iterator[str]:
    """What is wrong with a process and the inline bodies its steps run."""
    yield from (f"{name} {problem}" for problem in _check(process))
    for step in _steps(process) or []:
        if not isinstance(step.get("run"), dict):
            continue
        if depth == MAX_INLINE_DEPTH:
            yield (
                f"{name} runs an inline process nested more than "
                f"{MAX_INLINE_DEPTH} deep; make it a $graph entry and name it "
                "in run"
            )
        else:
            inline = f"the inline process of step {step.get('id', '?')}"
            yield from _problems(inline, step["run"], depth + 1)


def _check(process: dict[str, Any]) -> Iterator[str]:
    """What is wrong with one process, its own steps included but not their runs."""
    if process.get("class") == "Operation":
        yield (
            "is an Operation, an abstract process with no command, expression "
            "or steps, so no runner can execute it; replace it with a "
            "CommandLineTool, ExpressionTool or Workflow with the same inputs "
            "and outputs"
        )
    steps = _steps(process)
    if steps is None:
        yield "has steps that are not a list or map of objects"
    for holder in (process, *(steps or [])):
        requirements = _classes(holder.get("requirements"))
        hints = _classes(holder.get("hints"))
        if requirements is None:
            yield "has requirements that are not a list or map of objects"
        if hints is None:
            yield "has hints that are not a list or map of objects"
        requirements, hints = requirements or [], hints or []
        for cls in sorted({cls for cls, _ in requirements} - ALLOWED_REQUIREMENTS):
            yield f"requires {cls}, which DiracX does not support"
        for cls in sorted({cls for cls, _ in hints} - ALLOWED_HINTS):
            if not cls.startswith(DIRAC_NAMESPACE):
                yield f"has a {cls} hint, which DiracX does not support"
        # Every occurrence: a class may be given more than once, and which one
        # a runner acts on is up to the runner.
        fields = {
            field
            for cls, item in requirements + hints
            if cls == "DockerRequirement"
            for field in UNSUPPORTED_DOCKER_FIELDS
            if field in item
        }
        for field in sorted(fields):
            yield (
                f"uses DockerRequirement {field}, which would build or "
                "fetch an image on the worker node; publish the image "
                "and name it with dockerPull"
            )


def _steps(process: dict[str, Any]) -> list[dict[str, Any]] | None:
    """The steps of a process, from list or map form; ``None`` if neither."""
    steps = process.get("steps") or []
    if isinstance(steps, dict):
        steps = list(steps.values())
    if not isinstance(steps, list) or not all(isinstance(s, dict) for s in steps):
        return None
    return steps


def _classes(value: Any) -> list[tuple[str, dict]] | None:
    """Requirements or hints with their class, from list or map form.

    ``None`` if they are neither: the schema is only validated after the
    policy, so the document may not be valid CWL yet.
    """
    if value is None:
        return []
    if isinstance(value, dict):
        if not all(isinstance(body, dict) for body in value.values()):
            return None
        return [(str(cls), {**body, "class": cls}) for cls, body in value.items()]
    if not isinstance(value, list) or not all(isinstance(i, dict) for i in value):
        return None
    return [(str(item.get("class", "?")), item) for item in value]
