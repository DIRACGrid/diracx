"""What DiracX accepts in a submitted CWL process, beyond being valid CWL.

Checked on the canonical tree (:func:`.canonical.decompose`), so the same rules
apply wherever a document comes from and whichever tool checks it.
"""

from __future__ import annotations

import json

from .canonical import CanonicalProcess

# Requirements every backend can honour. Hints are not restricted: a runner
# is free to ignore them. Kept deliberately narrow: loosening it later is
# safe, tightening it would break documents already accepted.
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

# DockerRequirement fields that make the worker node build an image or fetch
# one from a URL at run time, rather than pull a published one: Apptainer
# cannot build from a Dockerfile there. Refused in hints too, since a runner
# with a container engine acts on a DockerRequirement hint.
UNSUPPORTED_DOCKER_FIELDS = ("dockerFile", "dockerImport", "dockerLoad")

# Largest canonical form of a single process. A year of LHCb submissions
# peaks below 20 kB; large data belongs in a sandbox, not in the document.
MAX_PROCESS_BYTES = 256 * 1024


class PolicyViolationError(ValueError):
    """A process is valid CWL but not something DiracX accepts."""


def check_policy(
    nodes: dict[bytes, CanonicalProcess],
    *,
    allowed_requirements: frozenset[str] = ALLOWED_REQUIREMENTS,
    max_process_bytes: int = MAX_PROCESS_BYTES,
) -> None:
    """Check every process of a canonical tree against the submission rules.

    Args:
        nodes: Processes of the tree, as returned by ``decompose``.
        allowed_requirements: Requirement classes accepted.
        max_process_bytes: Largest canonical form accepted for one process.

    Raises:
        PolicyViolationError: Listing every violation found.
    """
    problems = []
    for process_id, node in sorted(nodes.items()):
        name = f"process sha256-{process_id.hex()[:12]}"
        if len(node.body) > max_process_bytes:
            problems.append(
                f"{name} is {len(node.body)} bytes, more than {max_process_bytes}; "
                "move large content into a sandbox or File input"
            )
        body = json.loads(node.body)
        if body.get("class") == "Operation":
            problems.append(
                f"{name} is an Operation: it declares inputs and outputs but no "
                "command, expression or steps, so no runner can execute it; "
                "replace it with a CommandLineTool, ExpressionTool or Workflow "
                "taking and producing the same inputs and outputs"
            )
        holders = [body, *(body.get("steps") or {}).values()]
        for holder in holders:
            requirements = holder.get("requirements") or {}
            for cls in sorted(set(requirements) - allowed_requirements):
                problems.append(f"{name} requires {cls}, which DiracX does not support")
            for classes in (requirements, holder.get("hints") or {}):
                docker = classes.get("DockerRequirement") or {}
                for field in UNSUPPORTED_DOCKER_FIELDS:
                    if field in docker:
                        problems.append(
                            f"{name} uses DockerRequirement {field}, which would "
                            "build or fetch an image on the worker node; publish "
                            "the image to a registry and name it with dockerPull"
                        )
    if problems:
        raise PolicyViolationError("; ".join(problems))
