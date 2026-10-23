"""Storing and materialising content-addressed CWL processes (DX-ADR-004/007).

Each transformation's process is stored flat: one ``Processes`` row holding
its whole subgraph as a packed ``$graph`` document, keyed by the Merkle root
of its canonical tree (:mod:`diracx.core.cwl.canonical`). Materialising the
payload a parcel runs is then a single read of that row.

The packed document names every component by its own identity
(``#sha256-<hex>``), so per-component rows, references and lineage indexes can
be derived from stored rows later without changing any identity.

A ``run`` may reference a process by identity only if that process is itself
stored, i.e. it is the root of a stored row.

Validation is the expensive step (cwl-utils), so it runs only for processes not
already stored: a process in the store was validated when it was stored.

A stored process never changes. What an operator can change is which process a
transformation runs (:func:`replace_transformation_process`), for a hotfix;
each parcel records the process it was created with, so that change never
rewrites what a parcel already ran. End users cannot make it.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from typing import Any

from uuid_utils import UUID

from diracx.core.cwl import CanonicalProcess, check_policy, decompose, pack, unpack
from diracx.core.models.auth import UserInfo
from diracx.core.models.transformation import TransformationStatus
from diracx.core.properties import OPERATOR, PRODUCTION_MANAGEMENT
from diracx.db.sql.transformation.db import TransformationDB

from .validation import validate_packed

# The only properties allowed to change what a transformation runs. Neither is
# ever granted to end users; the submission API offers no such change at all.
REPLACE_PROCESS_PROPERTIES = frozenset({PRODUCTION_MANAGEMENT, OPERATOR})

PROCESS_REPLACED = "ProcessReplaced"


class ProcessNotFoundError(LookupError):
    """A process, or a process it references, is not in the store."""


class ProcessReplacementForbiddenError(PermissionError):
    """The caller may not change what a transformation runs."""


class TransformationNotFoundError(LookupError):
    """There is no compute transformation with that id."""


class TransformationNotPausedError(ValueError):
    """A transformation's process can only be replaced while it is paused."""


class IncompatibleProcessError(ValueError):
    """A replacement does not take and produce what the process it replaces did."""


@dataclass(frozen=True)
class ProcessReplacement:
    """The outcome of replacing a transformation's process.

    Attributes:
        old: Identity of the process replaced.
        new: Identity of the process the transformation runs from now on.
        repointed_parcels: ``Unassigned`` parcels moved to the new process.
    """

    old: bytes
    new: bytes
    repointed_parcels: int


async def store_process(db: TransformationDB, process: dict[str, Any]) -> bytes:
    """Validate a process and store it, deduplicated by content.

    Args:
        db: Transformation database, inside a transaction.
        process: CWL process as the resolved JSON value a client submits
            (the CLI's ``diracx.cli.cwl.load_document``); each ``run`` is an
            inline body or a reference to a stored process.

    Returns:
        The identity of the process.

    Raises:
        ValueError: If the process is not CWL v1.2 or not canonicalisable.
        diracx.core.cwl.PolicyViolationError: If it uses something DiracX does
            not support.
        diracx.logic.transformation.validation.InvalidProcessError: If cwl-utils
            rejects it.
        ProcessNotFoundError: If it references a process that is not stored.
    """
    root, nodes = decompose(process)
    check_policy(nodes)
    if await db.get_processes([root]):
        return root
    referenced = {child for node in nodes.values() for child in node.children}
    for process_id in sorted(referenced - set(nodes)):
        nodes.update(await _load_tree(db, process_id))
    packed = pack(root, nodes)
    validate_packed(packed)
    await db.insert_processes({root: packed})
    return root


async def load_process(db: TransformationDB, process_id: bytes) -> dict[str, Any]:
    """Return a stored process as a self-contained packed CWL document.

    Args:
        db: Transformation database.
        process_id: Identity of the process.

    Returns:
        The packed document a parcel runs.

    Raises:
        ProcessNotFoundError: If the process is not stored.
    """
    found = await db.get_processes([process_id])
    if process_id not in found:
        raise ProcessNotFoundError(process_id.hex())
    return found[process_id]


async def _load_tree(
    db: TransformationDB, process_id: bytes
) -> dict[bytes, CanonicalProcess]:
    root, nodes = unpack(await load_process(db, process_id))
    if root != process_id:
        raise ValueError(
            f"Stored process {process_id.hex()} does not match its content"
        )
    return nodes


async def replace_transformation_process(
    db: TransformationDB,
    transformation_id: UUID,
    process: dict[str, Any],
    *,
    operator: UserInfo,
    properties: Collection[str],
    reason: str,
) -> ProcessReplacement:
    """Make a paused transformation run another process, for operators only.

    The replacement is stored like any process (policy, cwl-utils validation,
    deduplication) and must keep the interface of the process it replaces,
    since the feeder, the packer, the declared outputs and downstream edges
    all depend on it. The transformation and its ``Unassigned`` parcels are
    repointed; parcels already handed to a backend keep the process they were
    created with. The change is recorded in the transformation's log.

    Everything happens in the caller's transaction: nothing is changed unless
    every check passes.

    Args:
        db: Transformation database, inside a transaction.
        transformation_id: The compute transformation.
        process: The replacement CWL process, as for :func:`store_process`.
        operator: Who is making the change.
        properties: The caller's security properties.
        reason: Why, recorded in the log.

    Returns:
        What was replaced, and how many parcels were repointed.

    Raises:
        ProcessReplacementForbiddenError: Without an operator property.
        ValueError: Without a reason, or if the replacement is invalid.
        TransformationNotFoundError: If there is no such compute transformation.
        TransformationNotPausedError: If the transformation is not paused.
        IncompatibleProcessError: If the replacement's interface differs.
    """
    if not REPLACE_PROCESS_PROPERTIES & set(properties):
        raise ProcessReplacementForbiddenError(
            "Replacing a transformation's process needs one of "
            f"{sorted(REPLACE_PROCESS_PROPERTIES)}"
        )
    if not reason.strip():
        raise ValueError("A reason is required to replace a transformation's process")

    locked = await db.lock_transformation_process(transformation_id)
    if locked is None:
        raise TransformationNotFoundError(str(transformation_id))
    status, old = locked
    if status != TransformationStatus.PAUSED:
        raise TransformationNotPausedError(
            f"Transformation {transformation_id} is {status.value}: pause it first"
        )

    new = await store_process(db, process)
    if new == old:
        return ProcessReplacement(old, new, 0)
    before = _interface(await load_process(db, old))
    after = _interface(await load_process(db, new))
    if before != after:
        raise IncompatibleProcessError(
            f"The replacement changes the interface: {before} becomes {after}"
        )

    await db.set_transformation_process(transformation_id, new)
    repointed = await db.repoint_unassigned_parcels(transformation_id, old, new)
    await db.insert_transformation_log(
        transformation_id,
        PROCESS_REPLACED,
        operator.preferred_username[:64],
        message=reason[:512],
        data={
            "old": old.hex(),
            "new": new.hex(),
            "repointed_parcels": repointed,
            "operator": operator.sub,
        },
    )
    return ProcessReplacement(old, new, repointed)


def _interface(packed: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The names and types a packed process takes and produces.

    A named type is replaced by its definition, so that changing a record's
    fields behind an unchanged name changes the interface.
    """
    main = next(entry for entry in packed["$graph"] if entry["id"] == "#main")
    schema = (main.get("requirements") or {}).get("SchemaDefRequirement") or {}
    definitions = {d["name"]: d for d in schema.get("types") or []}

    def resolve(value: Any, seen: frozenset[str]) -> Any:
        # Stored processes have no type shorthands: the canonical form
        # expands them, so a type name is the whole string
        if isinstance(value, str):
            if value in definitions and value not in seen:
                definition = {
                    k: v for k, v in definitions[value].items() if k != "name"
                }
                return resolve(definition, seen | {value})
            return value
        if isinstance(value, list):
            return [resolve(v, seen) for v in value]
        if isinstance(value, dict):
            return {k: resolve(v, seen) for k, v in value.items() if k != "name"}
        return value

    return {
        side: {
            name: resolve(spec.get("type"), frozenset())
            for name, spec in (main.get(side) or {}).items()
        }
        for side in ("inputs", "outputs")
    }
