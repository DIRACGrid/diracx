"""The core side of packing a transformation's inputs into parcels (DX-ADR-006).

The packer plugin only proposes; this module checks every proposal and is the
only thing that writes. Everything happens inside the caller's transaction, so
a proposal that fails a check rolls back the whole invocation, provided the
caller rolls back on an exception. The task worker does not yet
(DIRACGrid/diracx#1101).
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterator
from datetime import datetime, timezone
from typing import Any, get_type_hints

from pydantic import BaseModel
from uuid_utils import uuid7

from diracx.core.exceptions import ConfigurationError
from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.models.transformation import (
    INPUT_TRANSITIONS,
    BadInput,
    ComputeParcel,
    DataParcel,
    DelayInput,
    InputStatus,
    PackerInput,
    PackerProposal,
    TransformationKind,
    TransformationStatus,
)
from diracx.db.sql.transformation.db import (
    AnyUUID,
    NewComputeParcel,
    TransformationDB,
)

logger = logging.getLogger(__name__)

# Inputs claimed from the database per query
CLAIM_BATCH_SIZE = 1000
# Parcels buffered before they are written
PARCEL_BATCH_SIZE = 500
# Author recorded on the log rows the packer writes
PACKER_AUTHOR = "TransformationPacker"


class PackingSummary(BaseModel):
    """What one packer invocation did.

    Attributes:
        skipped: Whether the transformation was not ``Active``, so nothing ran.
        parcels_created: Parcels inserted.
        inputs_assigned: Inputs moved to ``Assigned``.
        inputs_problematic: Inputs moved to ``Problematic``.
        inputs_delayed: Inputs given a ``DelayedUntil``.
        inputs_left: Inputs claimed but left ``Unassigned`` by the packer.
    """

    skipped: bool = False
    parcels_created: int = 0
    inputs_assigned: int = 0
    inputs_problematic: int = 0
    inputs_delayed: int = 0
    inputs_left: int = 0


def load_packer(name: str) -> tuple[Callable[..., Any], type[BaseModel]]:
    """Find a packer plugin and the model of its arguments.

    The highest priority extension registering ``name`` wins. The arguments
    model is the annotation of the plugin's ``packer_args`` parameter.

    Args:
        name: Entry point name of the packer.

    Returns:
        The plugin function and its arguments model.

    Raises:
        ConfigurationError: If no packer is registered under ``name``, or its
            ``packer_args`` is not annotated with a pydantic model.
    """
    entry_points = select_from_extension(
        group=DiracEntryPoint.TRANSFORMATION_PACKERS, name=name
    )
    if not entry_points:
        raise ConfigurationError(
            f"No packer named {name!r} is registered under "
            f"{DiracEntryPoint.TRANSFORMATION_PACKERS.value!r}"
        )
    plugin = entry_points[0].load()
    args_model = get_type_hints(plugin).get("packer_args")
    if not (isinstance(args_model, type) and issubclass(args_model, BaseModel)):
        raise ConfigurationError(
            f"Packer {name!r} must annotate packer_args with a pydantic model"
        )
    return plugin, args_model


def content_hash(document: dict[str, Any]) -> bytes:
    """Hash a JSON document by its canonical form.

    Args:
        document: The document to hash.

    Returns:
        The SHA-256 digest of the document with sorted keys and no whitespace.
    """
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).digest()


def _require_transition(old: InputStatus, new: InputStatus) -> None:
    if new not in INPUT_TRANSITIONS[old]:
        raise ValueError(f"Inputs cannot move from {old} to {new}")


async def pack_transformation(
    transformation_db: TransformationDB,
    transformation_id: AnyUUID,
    *,
    flush: bool = False,
    max_inputs: int,
) -> PackingSummary:
    """Run a transformation's packer once and apply what it proposes.

    Args:
        transformation_db: Database, inside the caller's transaction.
        transformation_id: The transformation to pack.
        flush: Ask the packer to also pack groups it would hold back as too
            small.
        max_inputs: Maximum number of inputs offered to the packer.

    Returns:
        What the invocation did.

    Raises:
        NotImplementedError: For data transformations and data parcels, which
            the core does not support yet.
        ValueError: If the packer proposes an input it was not offered, or the
            same input twice.
        ConfigurationError: If the packer is unknown or its arguments are
            invalid.
    """
    transformation = await transformation_db.get_transformation(transformation_id)
    if transformation["status"] != TransformationStatus.ACTIVE:
        return PackingSummary(skipped=True)
    if transformation["kind"] != TransformationKind.COMPUTE:
        raise NotImplementedError("Packing data transformations is not supported yet")

    plugin, args_model = load_packer(transformation["packer"])
    try:
        packer_args = args_model.model_validate(transformation["packer_args"] or {})
    except ValueError as e:
        raise ConfigurationError(
            f"Invalid arguments for packer {transformation['packer']!r}: {e}"
        ) from e
    feeder_active = (
        transformation["feeder"] is not None and transformation["feeder_enabled"]
    )
    input_overview = await transformation_db.get_input_overview(transformation_id)
    process_id, template = await transformation_db.get_compute_facet(transformation_id)

    now = datetime.now(tz=timezone.utc)
    claimed: set[AnyUUID] = set()

    async def claim() -> AsyncGenerator[PackerInput, None]:
        after = None
        while len(claimed) < max_inputs:
            page = await transformation_db.claim_unassigned_inputs(
                transformation_id,
                now=now,
                after_input_id=after,
                limit=min(CLAIM_BATCH_SIZE, max_inputs - len(claimed)),
            )
            for packer_input in page:
                claimed.add(packer_input.input_id)
                yield packer_input
            if len(page) < CLAIM_BATCH_SIZE:
                return
            after = page[-1].input_id

    kwargs = {
        "packer_args": packer_args,
        "flush": flush,
        "feeder_active": feeder_active,
        "input_overview": input_overview,
    }
    proposals: AsyncIterator[PackerProposal]
    if inspect.isasyncgenfunction(plugin):
        proposals = plugin(inputs=claim(), **kwargs)
    else:
        # A synchronous packer cannot await the claim, so it is offered every
        # input up front, which max_inputs keeps bounded
        offered = [packer_input async for packer_input in claim()]
        proposals = _aiter(plugin(inputs=iter(offered), **kwargs))

    summary = PackingSummary()
    used: set[AnyUUID] = set()
    parcels: list[NewComputeParcel] = []
    problematic: dict[AnyUUID, str] = {}
    # Requirement sets awaiting insertion, and those already inserted
    new_requirements: dict[bytes, dict[str, Any]] = {}
    stored_requirements: set[bytes] = set()

    def consume(input_ids: list[AnyUUID]) -> None:
        for input_id in input_ids:
            if input_id not in claimed:
                raise ValueError(f"Packer proposed input {input_id} it was not offered")
            if input_id in used:
                raise ValueError(f"Packer proposed input {input_id} twice")
            used.add(input_id)

    async def write() -> None:
        for requirements_id, requirements in new_requirements.items():
            await transformation_db.insert_requirements(requirements_id, requirements)
        stored_requirements.update(new_requirements)
        new_requirements.clear()
        if parcels:
            _require_transition(InputStatus.UNASSIGNED, InputStatus.ASSIGNED)
            await transformation_db.insert_compute_parcels(
                transformation_id, process_id, parcels
            )
            summary.parcels_created += len(parcels)
            summary.inputs_assigned += sum(len(p.input_ids) for p in parcels)
            parcels.clear()
        if problematic:
            _require_transition(InputStatus.UNASSIGNED, InputStatus.PROBLEMATIC)
            await transformation_db.set_inputs_problematic(
                transformation_id, problematic, author=PACKER_AUTHOR
            )
            summary.inputs_problematic += len(problematic)
            problematic.clear()

    async for proposal in proposals:
        match proposal:
            case ComputeParcel():
                consume(proposal.inputs)
                requirements = {**template, **(proposal.requirements or {})}
                requirements_id = content_hash(requirements) if requirements else None
                if requirements_id and requirements_id not in stored_requirements:
                    new_requirements[requirements_id] = requirements
                parcels.append(
                    NewComputeParcel(
                        parcel_id=uuid7(),
                        requirements_id=requirements_id,
                        parameters=proposal.parameters,
                        annotations=proposal.metadata,
                        input_ids=list(proposal.inputs),
                    )
                )
            case BadInput():
                consume([proposal.input_id])
                problematic[proposal.input_id] = proposal.reason
            case DelayInput():
                consume(proposal.input_ids)
                await transformation_db.delay_inputs(
                    transformation_id, proposal.input_ids, proposal.delay_until
                )
                summary.inputs_delayed += len(proposal.input_ids)
            case DataParcel():
                raise NotImplementedError("Data parcels are not supported yet")
            case _:
                raise TypeError(f"Packer yielded {proposal!r}, not a proposal")
        if len(parcels) + len(problematic) >= PARCEL_BATCH_SIZE:
            await write()
    await write()

    # Inputs the packer held back stay Unassigned; their locks go at commit
    summary.inputs_left = len(claimed) - len(used)
    logger.info("Packed transformation %s: %s", transformation_id, summary)
    return summary


async def _aiter(iterator: Iterator[PackerProposal]) -> AsyncIterator[PackerProposal]:
    for item in iterator:
        yield item
