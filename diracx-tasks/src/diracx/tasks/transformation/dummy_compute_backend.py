"""A compute backend that pretends to run parcels, for demo deployments.

This replaces, for the Transformation System, what the dummy job executor
does for WMS jobs: it reads parcels from the ``TransformationDB`` rather than
jobs from the ``JobDB``, and walks each one through the parcel state machine
of DX-ADR-005 instead of the job state machine.

It stands in for two components that do not exist yet:

* the **dispatcher** (DX-ADR-003), which claims ``Unassigned`` parcels and
  hands them to a backend: ``Unassigned -> Reserved -> Assigned``;
* a real **compute backend**, which runs the work and reports the outcome:
  ``Assigned -> Completing -> Done``.

Every parcel succeeds and produces no output files, so no output is
registered while it is ``Completing``. Reaching ``Done`` moves the parcel's
inputs to ``Processed`` (see ``diracx.logic.transformations``).

The work is split the same way as for the dummy job executor:

* :class:`DummyComputeBackendMonitorTask` runs periodically, claims and
  "submits" a batch of parcels, then schedules one
  :class:`DummyComputeBackendTask` per parcel;
* :class:`DummyComputeBackendTask` "completes" one parcel after its simulated
  run time.

Both rely on the transitions being compare-and-set, so running two monitors,
or retrying a completion task, never moves a parcel twice.

Nothing creates parcels yet (there is no packer), so for a demonstration the
module can seed some, and list every parcel with its state, in the database
``DIRACX_DB_URL_TRANSFORMATIONDB`` points at (e.g. in ``pixi run local-shell``)::

    python -m diracx.tasks.transformation.dummy_compute_backend seed [N]
    python -m diracx.tasks.transformation.dummy_compute_backend list
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import sys
from datetime import UTC, datetime, timedelta
from typing import Annotated

from pydantic import NonNegativeInt, PositiveInt
from sqlalchemy import insert, select
from uuid_utils import UUID, uuid7

from diracx.core.models.transformation import (
    InputStatus,
    ParcelStatus,
    TransformationKind,
)
from diracx.core.settings import ServiceSettingsBase
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import (
    Inputs,
    Ownership,
    ParcelInputs,
    Parcels,
    Transformations,
)
from diracx.db.sql.utils import BaseSQLDB
from diracx.logic.transformations import set_parcel_status
from diracx.tasks.plumbing.base_task import BaseTask, PeriodicBaseTask
from diracx.tasks.plumbing.depends import NoTransaction
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import PARCEL
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff
from diracx.tasks.plumbing.schedules import IntervalSeconds

logger = logging.getLogger(__name__)

# Recorded in Parcels.Backend, so the dummy's parcels can be told apart from
# those of real backends
BACKEND_NAME = "dummy"


class DummyComputeBackendSettings(ServiceSettingsBase):
    """Settings controlling the dummy compute backend."""

    model_config = ServiceSettingsBase.model_config | {
        "env_prefix": "DIRACX_TASKS_DUMMY_COMPUTE_BACKEND_",
        "use_attribute_docstrings": True,
    }

    enabled: bool = False
    """Whether the monitor is scheduled automatically."""

    interval_seconds: PositiveInt = 10
    """How often the enabled monitor looks for unassigned compute parcels."""

    max_parcels_per_cycle: PositiveInt = 1000
    """Maximum number of parcels the monitor claims in one run."""

    runtime_seconds: NonNegativeInt = 5
    """Simulated run time between a parcel's submission and its completion."""


_settings = DummyComputeBackendSettings()


@dataclasses.dataclass
class DummyComputeBackendMonitorTask(PeriodicBaseTask):
    """Periodically claim unassigned compute parcels and "submit" them.

    Each run claims up to ``max_parcels_per_cycle`` ``Unassigned`` compute
    parcels, oldest first, moves them to ``Assigned`` on the ``dummy``
    backend, and schedules a :class:`DummyComputeBackendTask` for each to run
    ``runtime_seconds`` later.
    """

    priority = Priority.BACKGROUND
    size = Size.SMALL
    _enabled = _settings.enabled
    default_schedule = IntervalSeconds(_settings.interval_seconds)

    async def execute(
        self,
        # The transaction is managed here: it must be committed before the
        # completion tasks are scheduled, or a worker could pick one up
        # before the parcel it completes is visible as Assigned
        transformation_db: Annotated[TransformationDB, NoTransaction()],
    ) -> int:
        async with transformation_db:
            parcel_ids = await transformation_db.get_parcel_ids(
                ParcelStatus.UNASSIGNED,
                kind=TransformationKind.COMPUTE,
                # Leave alone parcels the packer pinned to another backend
                backends=[None, BACKEND_NAME],
                limit=_settings.max_parcels_per_cycle,
            )
            if not parcel_ids:
                return 0

            # Claim: only the parcels still Unassigned are reserved, so a
            # concurrent monitor cannot claim the same ones
            reserved = await set_parcel_status(
                parcel_ids,
                ParcelStatus.UNASSIGNED,
                ParcelStatus.RESERVED,
                transformation_db=transformation_db,
                backend=BACKEND_NAME,
            )
            # Submit: a real backend's submit() would run here, and Reserved
            # would be committed on its own beforehand to bound what a crash
            # can double-submit (DX-ADR-003). Submitting to the dummy cannot
            # fail and has no side effect, so both steps share a transaction.
            submitted = await set_parcel_status(
                reserved,
                ParcelStatus.RESERVED,
                ParcelStatus.ASSIGNED,
                transformation_db=transformation_db,
            )

        logger.info("Submitted %d parcel(s) to the dummy backend", len(submitted))
        # A crash between the commit above and the end of this loop leaves
        # parcels Assigned with no completion task; reconciling such parcels
        # is an open issue of DX-ADR-003 for real backends too
        complete_at = datetime.now(UTC) + timedelta(seconds=_settings.runtime_seconds)
        for parcel_id in submitted:
            await DummyComputeBackendTask(parcel_id=str(parcel_id)).schedule(
                at_time=complete_at
            )
        return len(submitted)


@dataclasses.dataclass
class DummyComputeBackendTask(BaseTask):
    """Simulate the successful completion of one parcel.

    The parcel is walked through ``Assigned -> Completing -> Done``, the path
    a real backend reporting success takes (DX-ADR-005). Both steps are taken
    in one transaction, so a failure leaves the parcel ``Assigned`` and the
    retry starts again from there, and reaching ``Done`` moves the parcel's
    inputs to ``Processed`` in that same transaction.
    """

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = ExponentialBackoff(base_delay_seconds=10, max_retries=3)

    # Kept as a string: task arguments are serialised with msgpack, which
    # does not know UUIDs
    parcel_id: str

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(PARCEL, self.parcel_id)]

    async def execute(self, transformation_db: TransformationDB) -> bool:
        parcel_id = UUID(self.parcel_id)
        logger.info("Simulating the completion of parcel %s", parcel_id)
        # The backend reports the work finished...
        completing = await set_parcel_status(
            [parcel_id],
            ParcelStatus.ASSIGNED,
            ParcelStatus.COMPLETING,
            transformation_db=transformation_db,
        )
        if not completing:
            # Already completed by an earlier run, or moved by someone else
            # (an operator, a cancellation): nothing left for the dummy to do
            logger.info("Parcel %s is no longer Assigned, skipping", parcel_id)
            return False
        # ...and, there being no outputs to register, the outcome is recorded
        await set_parcel_status(
            completing,
            ParcelStatus.COMPLETING,
            ParcelStatus.DONE,
            transformation_db=transformation_db,
        )
        return True


async def seed_demo_parcels(transformation_db: TransformationDB, n: int) -> list[UUID]:
    """Create a compute transformation with ``n`` parcels for the dummy to run.

    Each parcel is ``Unassigned`` and made of one ``Assigned`` input, as a
    packer would leave it.
    """
    ownership_id, transformation_id = uuid7(), uuid7()
    await transformation_db.conn.execute(
        insert(Ownership).values(ownership_id=ownership_id, vo="demo", sub="demo")
    )
    await transformation_db.conn.execute(
        insert(Transformations).values(
            transformation_id=transformation_id,
            ownership_id=ownership_id,
            type="Demo",
            kind=TransformationKind.COMPUTE,
            packer="Demo",
        )
    )
    parcel_ids = []
    for _ in range(n):
        parcel_id, input_id = uuid7(), uuid7()
        await transformation_db.conn.execute(
            insert(Parcels).values(
                parcel_id=parcel_id,
                transformation_id=transformation_id,
                kind=TransformationKind.COMPUTE,
            )
        )
        await transformation_db.conn.execute(
            insert(Inputs).values(
                transformation_id=transformation_id,
                input_id=input_id,
                input_hash=input_id.bytes,
                status=InputStatus.ASSIGNED,
                last_parcel_id=parcel_id,
            )
        )
        await transformation_db.conn.execute(
            insert(ParcelInputs).values(
                transformation_id=transformation_id,
                parcel_id=parcel_id,
                input_id=input_id,
            )
        )
        parcel_ids.append(parcel_id)
    return parcel_ids


async def list_parcels(
    transformation_db: TransformationDB,
) -> list[tuple[UUID, ParcelStatus, str | None, InputStatus | None]]:
    """Every parcel, oldest first, with its backend and its input's status."""
    rows = await transformation_db.conn.execute(
        select(Parcels.parcel_id, Parcels.status, Parcels.backend, Inputs.status)
        .outerjoin(Inputs, Inputs.last_parcel_id == Parcels.parcel_id)
        .order_by(Parcels.parcel_id)
    )
    return [tuple(row) for row in rows]  # type: ignore[misc]


async def _demo(argv: list[str]) -> None:
    command = argv[0] if argv else "list"
    db = TransformationDB(BaseSQLDB.available_urls()["TransformationDB"])
    async with db.engine_context(), db:
        if command == "seed":
            for parcel_id in await seed_demo_parcels(
                db, int(argv[1]) if len(argv) > 1 else 3
            ):
                print(f"Created Unassigned parcel {parcel_id}")
        elif command == "list":
            for parcel_id, status, backend, input_status in await list_parcels(db):
                print(
                    f"{parcel_id}  {status.value:<11} backend={backend}  "
                    f"input={input_status.value if input_status else None}"
                )
        else:
            sys.exit(
                "Usage: python -m diracx.tasks.transformation.dummy_compute_backend"
                " {seed [N] | list}"
            )


if __name__ == "__main__":
    asyncio.run(_demo(sys.argv[1:]))
