"""Toy pilot submission tasks for the advanced tutorial.

Demonstrates:
- One-shot tasks with custom locks and dead letter queue eligibility
- Periodic tasks with CronSchedule (non-VO-aware)
- VO-aware periodic tasks with IntervalSeconds schedule
- Spawning child tasks from a periodic parent
- Custom ``LockedObjectType`` (``MY_PILOT``) for domain-specific locking
- Calling business logic from ``gubbins.logic.my_pilots``
"""

# --8<-- [start:my_pilot_task_imports]
from __future__ import annotations

import dataclasses
import logging
from typing import Any

from diracx.tasks.plumbing.base_task import (
    BaseTask,
    PeriodicBaseTask,
    PeriodicVoAwareBaseTask,
)
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.lock_registry import LockedObjectType
from diracx.tasks.plumbing.locks import BaseLock, MutexLock
from diracx.tasks.plumbing.retry_policies import NoRetry
from diracx.tasks.plumbing.schedules import CronSchedule, IntervalSeconds

from gubbins.db.sql import MyPilotDB
from gubbins.logic.my_pilots import (
    get_available_ces,
    get_pilot_summary,
    submit_pilot,
    transition_pilot_states,
)

from .my_pilot_lock_types import MY_PILOT

logger = logging.getLogger(__name__)
# --8<-- [end:my_pilot_task_imports]


# --8<-- [start:my_pilot_task]
@dataclasses.dataclass
class MyPilotTask(BaseTask):
    """Submit a single pilot to a compute element.

    Delegates to ``gubbins.logic.my_pilots.submit_pilot`` which reads
    the CE's success_rate and determines whether the submission
    succeeds.  No retry is configured — the periodic parent will
    naturally resubmit on the next cycle.
    """

    ce_name: str

    priority = Priority.NORMAL
    size = Size.SMALL
    retry_policy = NoRetry()
    dlq_eligible = False

    @property
    def execution_locks(self) -> list[BaseLock]:
        return [MutexLock(LockedObjectType(MY_PILOT), self.ce_name)]

    async def execute(  # type: ignore[override]
        self, my_pilot_db: MyPilotDB, **kwargs: Any
    ) -> int:
        pilot_id = await submit_pilot(my_pilot_db, self.ce_name)
        logger.info("Submitted pilot %d to %s", pilot_id, self.ce_name)
        return pilot_id


# --8<-- [end:my_pilot_task]


# --8<-- [start:my_pilot_report_task]
class MyPilotReportTask(PeriodicBaseTask):
    """Log global pilot statistics across all VOs.

    Runs hourly via a CronSchedule.  Not VO-aware — reports
    aggregate counts.
    """

    default_schedule = CronSchedule("0 * * * *")

    async def execute(  # type: ignore[override]
        self, my_pilot_db: MyPilotDB, **kwargs: Any
    ) -> dict[str, int]:
        summary = await get_pilot_summary(my_pilot_db)
        logger.info("Pilot summary: %s", summary)
        return summary


# --8<-- [end:my_pilot_report_task]


# --8<-- [start:my_check_pilots_task]
@dataclasses.dataclass
class MyCheckPilotsTask(PeriodicVoAwareBaseTask):
    """Periodically check and transition pilot states.

    Delegates to ``gubbins.logic.my_pilots.transition_pilot_states``
    which queries pilots in SUBMITTED/RUNNING state and
    probabilistically transitions them based on the CE's success_rate.
    """

    vo: str

    default_schedule = IntervalSeconds(30)

    async def execute(  # type: ignore[override]
        self, my_pilot_db: MyPilotDB, **kwargs: Any
    ) -> None:
        await transition_pilot_states(my_pilot_db)


# --8<-- [end:my_check_pilots_task]


# --8<-- [start:my_submit_pilots_task]
@dataclasses.dataclass
class MySubmitPilotsTask(PeriodicVoAwareBaseTask):
    """Periodically submit pilots to available compute elements.

    Queries for CEs with available capacity and spawns a
    ``MyPilotTask`` for each available slot.
    """

    vo: str

    default_schedule = IntervalSeconds(60)

    async def execute(  # type: ignore[override]
        self, my_pilot_db: MyPilotDB, **kwargs: Any
    ) -> int:
        available_ces = await get_available_ces(my_pilot_db)
        spawned = 0
        for ce in available_ces:
            for _ in range(ce["available_slots"]):
                task = MyPilotTask(ce_name=ce["name"])
                await task.schedule()
                spawned += 1
                logger.info("Spawned MyPilotTask for %s", ce["name"])
        logger.info("VO %s: spawned %d pilot tasks", self.vo, spawned)
        return spawned


# --8<-- [end:my_submit_pilots_task]
