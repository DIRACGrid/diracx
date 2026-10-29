"""Tasks that run transformation packers (DX-ADR-006)."""

from __future__ import annotations

import dataclasses
import logging
from typing import Any
from uuid import UUID

from pydantic import PositiveInt

from diracx.core.models.transformation import TransformationStatus
from diracx.core.settings import ServiceSettingsBase
from diracx.db.sql import TransformationDB
from diracx.logic.transformations import pack_transformation
from diracx.tasks.plumbing.base_task import BaseTask, PeriodicBaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import TRANSFORMATION
from diracx.tasks.plumbing.locks import BaseLock, MutexLock, SharedRWLock
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff
from diracx.tasks.plumbing.schedules import IntervalSeconds

logger = logging.getLogger(__name__)


class PackerSettings(ServiceSettingsBase):
    """Settings controlling the transformation packer."""

    model_config = ServiceSettingsBase.model_config | {
        "env_prefix": "DIRACX_TASKS_PACKER_",
        "use_attribute_docstrings": True,
    }

    enabled: bool = False
    """Whether the packer sweep is scheduled automatically."""

    interval_seconds: PositiveInt = 60
    """How often the sweep schedules a packer run for each active transformation."""

    max_inputs_per_invocation: PositiveInt = 10_000
    """Maximum number of inputs offered to a packer in one run."""


_settings = PackerSettings()


@dataclasses.dataclass
class PackerSweepTask(PeriodicBaseTask):
    """Schedule one packer run per ``Active`` transformation.

    Each run holds a mutex on its transformation, so thousands of
    transformations pack in parallel while no two runs for one overlap.
    """

    priority = Priority.BACKGROUND
    size = Size.SMALL
    _enabled = _settings.enabled
    default_schedule = IntervalSeconds(_settings.interval_seconds)

    async def execute(self, transformation_db: TransformationDB) -> int:
        transformation_ids = await transformation_db.list_transformation_ids(
            TransformationStatus.ACTIVE
        )
        for transformation_id in transformation_ids:
            await PackTransformationTask(
                transformation_id=str(transformation_id)
            ).schedule()
        return len(transformation_ids)


@dataclasses.dataclass
class PackTransformationTask(BaseTask):
    """Run one transformation's packer and apply its proposals.

    Attributes:
        transformation_id: The transformation to pack.
        flush: Ask the packer to also pack groups it would hold back.
    """

    priority = Priority.NORMAL
    size = Size.MEDIUM
    retry_policy = ExponentialBackoff(base_delay_seconds=30, max_retries=3)

    transformation_id: str
    flush: bool = False

    @property
    def execution_locks(self) -> list[BaseLock]:
        # The mutex keeps two packer runs for one transformation apart while
        # letting its feeder run alongside; the shared status lock holds off
        # status changes, which take the exclusive side, until the run ends.
        return [
            MutexLock(TRANSFORMATION, self.transformation_id, "packer"),
            SharedRWLock(TRANSFORMATION, self.transformation_id, "status"),
        ]

    async def execute(self, transformation_db: TransformationDB) -> dict[str, Any]:
        summary = await pack_transformation(
            transformation_db,
            UUID(self.transformation_id),
            flush=self.flush,
            max_inputs=_settings.max_inputs_per_invocation,
        )
        return summary.model_dump()
