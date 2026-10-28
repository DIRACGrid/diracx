"""A compute backend that pretends to run parcels, for demo deployments.

It reads parcels from the ``TransformationDB`` and walks each one through the
parcel state machine of DX-ADR-005.

It stands in for two components that do not exist yet:

* the **dispatcher** (DX-ADR-003), which claims ``Unassigned`` parcels and
  hands them to a backend: ``Unassigned -> Reserved -> Assigned``;
* a real **compute backend**, which runs the work and reports the outcome:
  ``Assigned -> Completing -> Done``.

Every parcel succeeds and produces no output files, so no output is
registered while it is ``Completing``. Reaching ``Done`` moves the parcel's
inputs to ``Processed`` (see ``diracx.logic.transformations``).

The work is split in two tasks:

* :class:`DummyComputeBackendMonitorTask` runs periodically, claims and
  "submits" a batch of parcels, then schedules one
  :class:`DummyComputeBackendTask` per parcel;
* :class:`DummyComputeBackendTask` "completes" one parcel after its simulated
  run time.

Both rely on the transitions being compare-and-set, so running two monitors,
or retrying a completion task, never moves a parcel twice.
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import UTC, datetime, timedelta
from typing import Annotated

from pydantic import NonNegativeInt, PositiveInt
from uuid_utils import UUID

from diracx.core.models.search import ScalarSearchOperator, SortDirection
from diracx.core.models.transformation import ParcelStatus, TransformationKind
from diracx.core.settings import ServiceSettingsBase
from diracx.db.sql import TransformationDB
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
            # The oldest first: the parcel identifier is a UUIDv7, so key
            # order is creation order, the order the dispatcher claims in
            # (DX-ADR-003)
            _, parcels = await transformation_db.search_parcels(
                ["ParcelID"],
                [
                    {
                        "parameter": "Status",
                        "operator": ScalarSearchOperator.EQUAL,
                        "value": ParcelStatus.UNASSIGNED,
                    },
                    {
                        "parameter": "Kind",
                        "operator": ScalarSearchOperator.EQUAL,
                        "value": TransformationKind.COMPUTE,
                    },
                ],
                [{"parameter": "ParcelID", "direction": SortDirection.ASC}],
                # Leave alone parcels the packer pinned to another backend
                backends=[None, BACKEND_NAME],
                per_page=_settings.max_parcels_per_cycle,
                page=1,
            )
            parcel_ids = [parcel["ParcelID"] for parcel in parcels]
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
