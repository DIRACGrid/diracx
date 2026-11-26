"""The core side of packing a transformation's inputs into parcels (DX-ADR-006).

The packer plugin only proposes; this module checks every proposal and is the
only thing that writes. It commits each batch of proposals on its own, so a
failure rolls back only the batch in progress: earlier batches stay, and the
inputs of the failed one stay ``Unassigned`` for the next run.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, get_type_hints

from pydantic import BaseModel
from uuid_utils import uuid7

from diracx.core.exceptions import ConfigurationError
from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.hashing import hash_json
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
from diracx.core.utils import recursive_merge
from diracx.db.sql.transformation.db import (
    AnyUUID,
    NewComputeParcel,
    TransformationDB,
)

logger = logging.getLogger(__name__)

# Inputs claimed from the database per query
CLAIM_BATCH_SIZE = 1000
# Parcels, problematic inputs and delays written per transaction
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


@dataclass
class _Batch:
    """Proposals written together, in one transaction."""

    parcels: list[NewComputeParcel] = field(default_factory=list)
    problematic: dict[AnyUUID, str] = field(default_factory=dict)
    # Requirement sets first used in this batch, by their hash
    requirements: dict[bytes, dict[str, Any]] = field(default_factory=dict)
    # Delays are written as they come, not buffered, but each is a statement
    # in the batch's transaction, so they count towards its size
    delays: int = 0

    def __len__(self) -> int:
        return len(self.parcels) + len(self.problematic) + self.delays


# TODO(#1179): once #1186 is merged, replace load_packer and its tests with the
# shared loader, which also checks the plugin against the Packer protocol, and
# make the docs example's ByRunPackerArgs a PluginArgs:
#
#     from diracx.logic.transformations.plugins import PACKER_KIND, load_plugin
#
#     plugin, args_model = load_plugin(PACKER_KIND, transformation["packer"])
def load_packer(name: str) -> tuple[Callable[..., Any], type[BaseModel]]:
    """Find a packer plugin and the model of its arguments.

    If both DiracX and the installed extension register a packer under
    ``name``, the extension's is used, so an extension can replace a packer
    shipped by DiracX. The arguments model is the annotation of the plugin's
    ``packer_args`` parameter.

    Args:
        name: Entry point name of the packer.

    Returns:
        The plugin function and its arguments model.

    Raises:
        ConfigurationError: If no packer is registered under ``name``, it is
            not an async generator function, or its ``packer_args`` is not
            annotated with a pydantic model.
    """
    entry_points = select_from_extension(group=DiracEntryPoint.PACKERS, name=name)
    if not entry_points:
        raise ConfigurationError(
            f"No packer named {name!r} is registered under "
            f"{DiracEntryPoint.PACKERS.value!r}"
        )
    plugin = entry_points[0].load()
    if not inspect.isasyncgenfunction(plugin):
        raise ConfigurationError(f"Packer {name!r} must be an async generator function")
    args_model = get_type_hints(plugin).get("packer_args")
    if not (isinstance(args_model, type) and issubclass(args_model, BaseModel)):
        raise ConfigurationError(
            f"Packer {name!r} must annotate packer_args with a pydantic model"
        )
    return plugin, args_model


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

    Each batch of proposals is written in its own transaction (DX-ADR-006),
    opened here: a batch that fails is rolled back, the ones before it stay.

    Args:
        transformation_db: Database, not entered by the caller.
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
    async with transformation_db:
        transformation = await transformation_db.get_transformation(transformation_id)
        if transformation["status"] != TransformationStatus.ACTIVE:
            return PackingSummary(skipped=True)
        if transformation["kind"] != TransformationKind.COMPUTE:
            raise NotImplementedError(
                "Packing data transformations is not supported yet"
            )
        input_overview = await transformation_db.get_input_overview(transformation_id)
        process_id, template = await transformation_db.get_compute_facet(
            transformation_id
        )

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
    proposals: AsyncIterator[PackerProposal] = plugin(inputs=claim(), **kwargs)

    summary = PackingSummary()
    used: set[AnyUUID] = set()
    # Requirement sets inserted by an earlier batch
    stored_requirements: set[bytes] = set()

    def consume(input_ids: list[AnyUUID]) -> None:
        """Record the inputs a proposal uses, rejecting any it may not use.

        The plugin should only propose inputs it was offered, and each one at most
        once. A packer is extension code, so its proposals are not trusted: each
        input must be one offered in this run, and appear in one proposal only.

        Raises:
            ValueError: If an input was not offered, or was already used.
        """
        for input_id in input_ids:
            if input_id not in claimed:
                raise ValueError(f"Packer proposed input {input_id} it was not offered")
            if input_id in used:
                raise ValueError(f"Packer proposed input {input_id} twice")
            used.add(input_id)

    async def apply(batch: _Batch, proposal: PackerProposal) -> None:
        match proposal:
            case ComputeParcel():
                consume(proposal.input_ids)
                requirements = recursive_merge(template, proposal.requirements or {})
                requirements_id = hash_json(requirements) if requirements else None
                if requirements_id and requirements_id not in stored_requirements:
                    batch.requirements[requirements_id] = requirements
                batch.parcels.append(
                    NewComputeParcel(
                        parcel_id=uuid7(),
                        requirements_id=requirements_id,
                        parameters=proposal.parameters,
                        annotations=proposal.metadata,
                        input_ids=list(proposal.input_ids),
                    )
                )
            case BadInput():
                consume([proposal.input_id])
                batch.problematic[proposal.input_id] = proposal.reason
            case DelayInput():
                consume(proposal.input_ids)
                await transformation_db.delay_inputs(
                    transformation_id, proposal.input_ids, proposal.delay_until
                )
                batch.delays += 1
                summary.inputs_delayed += len(proposal.input_ids)
            case DataParcel():
                raise NotImplementedError("Data parcels are not supported yet")
            case _:
                raise TypeError(f"Packer yielded {proposal!r}, not a proposal")

    async def write(batch: _Batch) -> None:
        for requirements_id, requirements in batch.requirements.items():
            await transformation_db.insert_requirements(requirements_id, requirements)
        stored_requirements.update(batch.requirements)
        _require_transition(InputStatus.UNASSIGNED, InputStatus.ASSIGNED)
        await transformation_db.insert_compute_parcels(
            transformation_id, process_id, batch.parcels
        )
        _require_transition(InputStatus.UNASSIGNED, InputStatus.PROBLEMATIC)
        await transformation_db.set_inputs_problematic(
            transformation_id, batch.problematic, author=PACKER_AUTHOR
        )
        summary.parcels_created += len(batch.parcels)
        summary.inputs_assigned += sum(len(p.input_ids) for p in batch.parcels)
        summary.inputs_problematic += len(batch.problematic)

    finished = False
    while not finished:
        # One transaction per batch: leaving the block commits it, which also
        # releases the row locks claimed so far, or rolls it back on an error.
        # Breaking out of the loop leaves the packer paused, and the next batch
        # resumes it where it stopped.
        async with transformation_db:
            batch = _Batch()
            finished = True
            async for proposal in proposals:
                await apply(batch, proposal)
                if len(batch) >= PARCEL_BATCH_SIZE:
                    finished = False
                    break
            await write(batch)

    # Inputs the packer held back stay Unassigned; their locks went at commit
    summary.inputs_left = len(claimed) - len(used)
    logger.info("Packed transformation %s: %s", transformation_id, summary)
    return summary
