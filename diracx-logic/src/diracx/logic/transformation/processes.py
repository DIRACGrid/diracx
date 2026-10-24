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
"""

from __future__ import annotations

from typing import Any

from diracx.core.cwl import CanonicalProcess, check_policy, decompose, pack, unpack
from diracx.db.sql.transformation.db import TransformationDB

from .validation import validate_packed


class ProcessNotFoundError(LookupError):
    """A process, or a process it references, is not in the store."""


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
