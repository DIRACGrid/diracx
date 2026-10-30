"""Feeder plugin loading and materialisation into TransformationDB rows."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable
from typing import Any, get_type_hints

from pydantic import BaseModel
from uuid_utils import UUID, uuid7

from diracx.core.exceptions import ConfigurationError
from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.hashing import compute_input_hash
from diracx.core.models.feeder import Input, NoMoreInputs, SetFeederState
from diracx.db.sql import TransformationDB

FeederYield = Input | SetFeederState | NoMoreInputs

# Default insert chunk size when a feeder yields many ``Input``s before a
# ``SetFeederState``. Hash dedup makes early commits safe on restart.
DEFAULT_INPUT_BATCH_SIZE = 1000


def load_feeder(name: str) -> tuple[Callable[..., Any], type[BaseModel]]:
    """Find a feeder plugin and the model of its arguments.

    The highest priority extension registering ``name`` wins. The arguments
    model is the annotation of the plugin's ``feeder_args`` parameter.

    Args:
        name: Entry-point name stored in ``Transformations.Feeder``.

    Returns:
        The plugin function and its arguments model.

    Raises:
        ConfigurationError: If no feeder is registered under ``name``, it is
            not a sync generator function, or its ``feeder_args`` is not
            annotated with a pydantic model.
    """
    entry_points = select_from_extension(group=DiracEntryPoint.FEEDERS, name=name)
    if not entry_points:
        raise ConfigurationError(
            f"No feeder named {name!r} is registered under "
            f"{DiracEntryPoint.FEEDERS.value!r}"
        )
    plugin = entry_points[0].load()
    if not inspect.isgeneratorfunction(plugin):
        raise ConfigurationError(f"Feeder {name!r} must be a sync generator function")
    args_model = get_type_hints(plugin).get("feeder_args")
    if not (isinstance(args_model, type) and issubclass(args_model, BaseModel)):
        raise ConfigurationError(
            f"Feeder {name!r} must annotate feeder_args with a pydantic model"
        )
    return plugin, args_model


def _input_row(proposed: Input) -> dict[str, Any]:
    """Build an ``insert_unassigned_inputs`` row from a proposal."""
    return {
        "input_id": uuid7(),
        "input_hash": compute_input_hash(proposed.lfn, proposed.descriptor),
        "lfn": proposed.lfn,
        "lfn_size": proposed.lfn_size,
        "descriptor": proposed.descriptor,
        "delayed_until": proposed.delayed_until,
    }


async def _commit_batch(
    transformation_db: TransformationDB,
    transformation_id: UUID,
    pending: list[dict[str, Any]],
    *,
    feeder_state: dict[str, Any] | None = None,
    update_feeder_state: bool = False,
    feeder_enabled: bool | None = None,
) -> None:
    """Write one batch in a single transaction and commit."""
    async with transformation_db:
        if pending:
            await transformation_db.insert_unassigned_inputs(transformation_id, pending)
        if update_feeder_state:
            await transformation_db.set_feeder_state(transformation_id, feeder_state)
        if feeder_enabled is not None:
            await transformation_db.set_feeder_enabled(
                transformation_id, feeder_enabled
            )


async def materialise_feeder_yields(
    transformation_id: UUID,
    yields: Iterable[FeederYield],
    transformation_db: TransformationDB,
    *,
    input_batch_size: int = DEFAULT_INPUT_BATCH_SIZE,
) -> None:
    """Consume feeder proposals and write them through ``TransformationDB``.

    Owns transaction boundaries. ``Input`` proposals are inserted in chunks of
    ``input_batch_size`` so a long run does not hold every row in memory or in
    one transaction. ``SetFeederState`` flushes any remainder and updates the
    bookmark in the same transaction. ``NoMoreInputs`` flushes any remainder,
    clears ``FeederEnabled``, then stops. A trailing remainder when the
    iterable ends is flushed as an insert-only commit (bookmark unchanged).

    Inserting ahead of the bookmark is safe: ``InputHash`` deduplication drops
    repeats if the feeder is restarted before the next ``SetFeederState``.

    Args:
        transformation_id: Transformation being fed.
        yields: Sync iterable of proposal objects from a feeder plugin.
        transformation_db: DB instance; must not already be in a transaction
            (this function opens one per batch).
        input_batch_size: Max ``Input`` rows buffered before an insert-only
            commit. Must be >= 1.

    Raises:
        TypeError: If the iterable yields an unsupported type.
        ValueError: If ``input_batch_size`` is < 1.
    """
    if input_batch_size < 1:
        raise ValueError("input_batch_size must be >= 1")

    pending: list[dict[str, Any]] = []

    for item in yields:
        if isinstance(item, Input):
            pending.append(_input_row(item))
            if len(pending) >= input_batch_size:
                await _commit_batch(transformation_db, transformation_id, pending)
                pending = []
            continue

        if isinstance(item, SetFeederState):
            await _commit_batch(
                transformation_db,
                transformation_id,
                pending,
                feeder_state=item.state,
                update_feeder_state=True,
            )
            pending = []
            continue

        if isinstance(item, NoMoreInputs):
            await _commit_batch(
                transformation_db,
                transformation_id,
                pending,
                feeder_enabled=False,
            )
            return

        raise TypeError(f"unsupported feeder yield type: {type(item).__name__}")

    # Remainder without a trailing SetFeederState / NoMoreInputs: still persist.
    # Bookmark may lag; InputHash dedup covers a later re-yield.
    if pending:
        await _commit_batch(transformation_db, transformation_id, pending)
