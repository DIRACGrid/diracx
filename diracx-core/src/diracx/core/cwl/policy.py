"""What DiracX accepts in a submitted CWL process, beyond being valid CWL.

Checked on the packed document the client submits, after
:func:`.identity.check_self_contained`, so that every process DiracX stores
can run on any of its backends without fetching or building anything.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from diracx.core.hashing import canonical_json_bytes

from .identity import _graph, _steps

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

# Hints a runner may ignore, but which it must recognise: cwltool tries to
# fetch the schema of a class it does not know.
ALLOWED_HINTS = ALLOWED_REQUIREMENTS | {"SoftwareRequirement", "WorkReuse"}

# DiracX's own classes (``dirac:Transformation``…), accepted as hints only.
# cwltool --pack writes them with the prefix expanded.
DIRAC_NAMESPACE = "https://diracgrid.org/cwl#"

# DockerRequirement fields that make the worker node build an image or fetch
# one from a URL at run time, rather than pull a published one: Apptainer
# cannot build from a Dockerfile there. Refused in hints too, since a runner
# with a container engine acts on a DockerRequirement hint.
UNSUPPORTED_DOCKER_FIELDS = ("dockerFile", "dockerImport", "dockerLoad")

# Largest canonical form of one $graph entry. A year of LHCb submissions
# peaks below 20 kB; large data belongs in a sandbox, not in the document.
MAX_PROCESS_BYTES = 256 * 1024


class PolicyViolationError(ValueError):
    """A process is valid CWL but not something DiracX accepts."""


def check_policy(
    document: dict[str, Any],
    *,
    allowed_requirements: frozenset[str] = ALLOWED_REQUIREMENTS,
    allowed_hints: frozenset[str] = ALLOWED_HINTS,
    max_process_bytes: int = MAX_PROCESS_BYTES,
) -> None:
    """Check every process of a packed document against the submission rules.

    This is the second check on a submission, after it has been found to be
    valid, self-contained CWL.

    Args:
        document: A packed CWL document that passes
            :func:`.identity.check_self_contained`.
        allowed_requirements: Requirement classes accepted.
        allowed_hints: Hint classes accepted, besides DiracX's own.
        max_process_bytes: Largest canonical form accepted for one
            ``$graph`` entry.

    Raises:
        PolicyViolationError: Listing every violation found, each naming the
            ``$graph`` entry (or the step running an inline body) it is in.
    """
    namespaces = document.get("$namespaces") or {}
    problems = []
    for entry_id, entry in _graph(document).items():
        size = len(canonical_json_bytes(entry))
        if size > max_process_bytes:
            problems.append(
                f"{entry_id} is {size} bytes, more than {max_process_bytes}; "
                "move large content into a sandbox or File input"
            )
        for name, process in _processes(entry_id, entry):
            problems.extend(
                f"{name} {problem}"
                for problem in _check(
                    process, namespaces, allowed_requirements, allowed_hints
                )
            )
    if problems:
        raise PolicyViolationError("; ".join(problems))


def _processes(name: str, process: dict[str, Any]) -> Iterator[tuple[str, dict]]:
    """A process and the inline bodies its steps run, each with a name."""
    yield name, process
    for step in _steps(process):
        if isinstance(step.get("run"), dict):
            yield from _processes(
                f"the inline process of step {step.get('id', '?')}", step["run"]
            )


def _check(
    process: dict[str, Any],
    namespaces: dict[str, str],
    allowed_requirements: frozenset[str],
    allowed_hints: frozenset[str],
) -> Iterator[str]:
    if process.get("class") == "Operation":
        yield (
            "is an Operation, an abstract process with no command, expression "
            "or steps, so no runner can execute it; replace it with a "
            "CommandLineTool, ExpressionTool or Workflow with the same inputs "
            "and outputs"
        )
    for holder in (process, *_steps(process)):
        requirements = _classes(holder.get("requirements"), namespaces)
        hints = _classes(holder.get("hints"), namespaces)
        for cls in sorted(set(requirements) - allowed_requirements):
            yield f"requires {cls}, which DiracX does not support"
        for cls in sorted(set(hints) - allowed_hints):
            if not cls.startswith(DIRAC_NAMESPACE):
                yield f"has a {cls} hint, which DiracX does not support"
        for classes in (requirements, hints):
            docker = classes.get("DockerRequirement") or {}
            for field in UNSUPPORTED_DOCKER_FIELDS:
                if field in docker:
                    yield (
                        f"uses DockerRequirement {field}, which would build or "
                        "fetch an image on the worker node; publish the image "
                        "and name it with dockerPull"
                    )


def _classes(value: Any, namespaces: dict[str, str]) -> dict[str, dict]:
    """Requirements or hints by class, prefixes expanded, from list or map form."""
    if isinstance(value, dict):
        items = [{**body, "class": cls} for cls, body in value.items()]
    else:
        items = list(value or [])
    classes = {}
    for item in items:
        cls = str(item.get("class", "?"))
        prefix, _, rest = cls.partition(":")
        if prefix in namespaces:
            cls = namespaces[prefix] + rest
        classes[cls] = item
    return classes
