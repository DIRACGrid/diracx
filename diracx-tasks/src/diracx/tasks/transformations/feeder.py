"""Periodic feeder sweep and per-transformation feed tasks."""

from __future__ import annotations

import dataclasses
import logging
from typing import Annotated

from pydantic import PositiveInt
from uuid_utils import UUID

from diracx.core.exceptions import ConfigurationError
from diracx.core.settings import ServiceSettingsBase
from diracx.db.sql import TransformationDB
from diracx.logic.transformations import load_feeder, materialise_feeder_yields
from diracx.tasks.depends import NoTransaction
from diracx.tasks.plumbing.base_task import BaseTask, PeriodicBaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import TRANSFORMATION
from diracx.tasks.plumbing.locks import BaseLock, MutexLock, SharedRWLock
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff
from diracx.tasks.plumbing.schedules import IntervalSeconds

logger = logging.getLogger(__name__)


class FeederTaskSettings(ServiceSettingsBase):
    """Settings controlling the periodic feeder sweep."""

    model_config = ServiceSettingsBase.model_config | {
        "env_prefix": "DIRACX_TASKS_FEEDER_",
        "use_attribute_docstrings": True,
    }

    enabled: bool = False
    """Whether the sweep is scheduled automatically."""

    interval_seconds: PositiveInt = 60
    """How often the sweep lists feedable transformations."""


_settings = FeederTaskSettings()


@dataclasses.dataclass
class PeriodicSweepFeeders(PeriodicBaseTask):
    """Enqueue one ``FeedTransformation`` per feedable transformation.

    Selects Active transformations with ``FeederEnabled`` and a ``Feeder`` set.
    Uses the default periodic class mutex so only one sweep runs at a time.
    """

    priority = Priority.BACKGROUND
    size = Size.SMALL
    _enabled = _settings.enabled
    default_schedule = IntervalSeconds(_settings.interval_seconds)

    async def execute(self, transformation_db: TransformationDB) -> int:
        transformation_ids = await transformation_db.list_feedable_transformation_ids()
        for transformation_id in transformation_ids:
            await FeedTransformation(
                transformation_id=str(transformation_id)
            ).schedule()
        if transformation_ids:
            logger.info(
                "Scheduled FeedTransformation for %d transformation(s)",
                len(transformation_ids),
            )
        return len(transformation_ids)


@dataclasses.dataclass
class FeedTransformation(BaseTask):
    """Run one transformation's feeder plugin and materialise its yields.

    The feeder mutex keeps two feed passes for one transformation apart while
    still allowing a packer (``…:packer``) to run alongside. The shared status
    lock holds off exclusive status changes until the run ends. Opens its own
    DB transactions (``NoTransaction``) because materialisation commits in
    batches.
    """

    priority = Priority.NORMAL
    size = Size.MEDIUM
    retry_policy = ExponentialBackoff(base_delay_seconds=30, max_retries=3)

    transformation_id: str
    """UUIDv7 of the transformation, as a string for broker serialization."""

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [
            MutexLock(TRANSFORMATION, self.transformation_id, "feeder"),
            SharedRWLock(TRANSFORMATION, self.transformation_id, "status"),
        ]

    async def execute(
        self,
        transformation_db: Annotated[TransformationDB, NoTransaction()],
    ) -> None:
        transformation_id = UUID(self.transformation_id)
        async with transformation_db:
            columns = await transformation_db.get_feeder_columns(transformation_id)

        if columns is None or not columns["feeder"] or not columns["feeder_enabled"]:
            logger.info(
                "Skipping feed for transformation %s (missing or disabled)",
                self.transformation_id,
            )
            return

        feeder_name = columns["feeder"]
        feeder, args_model = load_feeder(feeder_name)
        try:
            feeder_args = args_model.model_validate(columns["feeder_args"] or {})
        except ValueError as e:
            raise ConfigurationError(
                f"Invalid arguments for feeder {feeder_name!r}: {e}"
            ) from e
        logger.info(
            "Running feeder %s for transformation %s",
            feeder_name,
            self.transformation_id,
        )
        yields = feeder(
            feeder_args=feeder_args,
            feeder_state=columns["feeder_state"] or {},
            scouting=False,
        )
        await materialise_feeder_yields(
            transformation_id,
            yields,
            transformation_db,
        )
