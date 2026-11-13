"""Storing and loading the CWL processes transformations run (DX-ADR-004/007).

The client submits one packed document (``cwltool --pack``) holding every
process of a workgraph. Each transformation's process is stored flat: one
``Processes`` row holding :func:`diracx.core.cwl.subgraph` of its entry, the
entry and everything it runs as submitted, keyed by the entry's identity.
Loading what a parcel runs is then a single read of that row, and the row is
a document a stock CWL runner executes.

Validation is the expensive step (cwl-utils), so it runs only for processes not
already stored: a process in the store was validated when it was stored.
See ``docs/dev/explanations/cwl-process-store.md``.
"""

from __future__ import annotations

from typing import Any, NamedTuple

from diracx.core.cwl import (
    check_policy,
    check_self_contained,
    decompose,
    entry_identity,
    subgraph,
)
from diracx.db.sql.transformation.db import TransformationDB

from .validation import validate_packed


class ProcessNotFoundError(LookupError):
    """A process is not in the store."""


class StoredProcess(NamedTuple):
    """A stored process, as a runner is given it.

    Attributes:
        document: The packed CWL document.
        entry_id: The ``$graph`` entry to run, e.g. ``#merge.cwl``.
    """

    document: dict[str, Any]
    entry_id: str


async def store_process(
    db: TransformationDB, document: dict[str, Any], entry_id: str
) -> bytes:
    """Check one entry of a submitted document and store what it runs.

    Args:
        db: Transformation database, inside a transaction.
        document: The packed CWL document the client submitted.
        entry_id: The entry a transformation runs, e.g. a workgraph step's
            ``run``.

    Returns:
        The identity of the entry.

    Raises:
        diracx.core.cwl.NotSelfContainedError: If the document needs
            anything from outside itself.
        diracx.core.cwl.PolicyViolationError: If it uses something DiracX does
            not support.
        diracx.logic.transformation.validation.InvalidProcessError: If cwl-utils
            rejects what the entry runs.
        KeyError: If the document has no such entry.
        ValueError: If it is not a CWL v1.2 document.
    """
    check_self_contained(document)
    check_policy(document)
    process_id = entry_identity(document, entry_id)
    if await db.get_processes([process_id]):
        return process_id
    row = subgraph(document, entry_id)
    validate_packed(row)
    await db.insert_processes({process_id: row})
    return process_id


async def load_process(db: TransformationDB, process_id: bytes) -> StoredProcess:
    """Return a stored process and the entry a runner starts from.

    Rows keep the ids they were submitted with, so the entry is found as the
    one whose identity is the row's key, which also checks that the row
    still matches it.

    Args:
        db: Transformation database.
        process_id: Identity of the process.

    Returns:
        The packed document and the entry to run.

    Raises:
        ProcessNotFoundError: If the process is not stored.
        ValueError: If the stored row does not match its identity.
    """
    found = await db.get_processes([process_id])
    if process_id not in found:
        raise ProcessNotFoundError(process_id.hex())
    document = found[process_id]
    component = decompose(document).get(process_id)
    if component is None or component.entry_id is None:
        raise ValueError(
            f"Stored process {process_id.hex()} does not match its content"
        )
    return StoredProcess(document, component.entry_id)
