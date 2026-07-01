"""Priority calculation helpers for task queues.

This module contains helpers to compute and apply per-user or per-group
task-queue priority shares. The functions translate configured job-share
values into per-task-queue priorities, respecting background-task-queue
settings and grouping task queues with equivalent characteristics.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from diracx.core.config import Config
from diracx.core.properties import JOB_SHARING
from diracx.db.sql.task_queue.db import TaskQueueDB

TQ_MIN_SHARE = 0.001
PRIORITY_IGNORED_FIELDS = ("Sites", "BannedSites")


async def recalculate_tq_shares_for_entity(
    owner: str,
    owner_group: str,
    vo: str,
    config: Config,
    task_queue_db: TaskQueueDB,
):
    """Recalculate the shares for a user/userGroup combo.

    This reads the group configuration to determine the group's job share
    and either sets a single priority for the whole group (when the group
    enables job sharing) or splits the configured share among the group's
    owners. When the number of owners changes, priorities for all owners in
    the group are recalculated.

    Args:
        owner (str): Owner username (may be None when recalculating for group).
        owner_group (str): Owner group name.
        vo (str): Virtual organisation identifier.
        config (Config): Configuration registry used to read group settings.
        task_queue_db (TaskQueueDB): DB helper used to read and update TQ data.
    """
    group_properties = config.registry[vo].groups[owner_group].properties
    job_share = config.registry[vo].groups[owner_group].job_share
    allow_background_tqs = config.registry[vo].groups[owner_group].allow_background_tqs
    if JOB_SHARING in group_properties:
        # If group has JobSharing just set prio for that entry, user is irrelevant
        await set_priorities_for_entity(
            owner=None,
            owner_group=owner_group,
            job_share=job_share,
            allow_background_tqs=allow_background_tqs,
            task_queue_db=task_queue_db,
        )
        return

    # Get all owners from the owner group
    owners = await task_queue_db.get_task_queue_owners_by_group(owner_group)
    num_owners = len(owners)
    # If there are no owners do now
    if num_owners == 0:
        return

    # Split the share amongst the number of owners
    entities_shares = {owner: job_share / num_owners for owner, _ in owners.items()}

    # TODO: implement the following
    # If corrector is enabled let it work it's magic
    # if enable_shares_correction:
    #     entities_shares = await self.__shares_corrector.correct_shares(
    #         entitiesShares, group=group
    #     )

    # If the user is already known and has more than 1 tq, the rest of the users don't need to be modified
    # (The number of owners didn't change)
    if owner in owners and owners[owner] > 1:
        await set_priorities_for_entity(
            owner=owner,
            owner_group=owner_group,
            job_share=entities_shares[owner],
            allow_background_tqs=allow_background_tqs,
            task_queue_db=task_queue_db,
        )
        return

    # Oops the number of owners may have changed so we recalculate the prio for all owners in the group
    for owner in owners:
        await set_priorities_for_entity(
            owner=owner,
            owner_group=owner_group,
            job_share=entities_shares[owner],
            allow_background_tqs=allow_background_tqs,
            task_queue_db=task_queue_db,
        )


async def set_priorities_for_entity(
    owner_group: str,
    job_share: float,
    allow_background_tqs: bool,
    task_queue_db: TaskQueueDB,
    owner: str | None = None,
):
    """Set the priority for a user/userGroup combo given a split share.

    Args:
        owner_group (str): Owner group name.
        job_share (float): Share allocated to the owner or owner group.
        allow_background_tqs (bool): Whether background task queues may be used.
        task_queue_db (TaskQueueDB): DB helper used to read and update TQ data.
        owner (str | None): Optional specific owner to update. If omitted,
            priorities for all owners in the group are adjusted.
    """
    tq_dict = await task_queue_db.get_task_queue_priorities(owner_group, owner)
    if not tq_dict:
        return

    rows = await task_queue_db.retrieve_task_queues(list(tq_dict))
    prio_dict = await calculate_priority(tq_dict, rows, job_share, allow_background_tqs)
    for prio, tqs in prio_dict.items():
        await task_queue_db.set_priorities_for_entity(tqs, prio)


async def calculate_priority(
    tq_dict: dict[int, float],
    all_tqs_data: dict[int, dict[str, Any]],
    share: float,
    allow_bg_tqs: bool,
) -> dict[float, list[int]]:
    """Calculate effective priorities for task queues from a share.

    Args:
        tq_dict (dict[int, float]): Mapping of task-queue id to its configured
            priority weight.
        all_tqs_data (dict[int, dict[str, Any]]): Mapping of task-queue id to
            its data dictionary (fields describing the TQ).
        share (float): Share to be distributed among the task queues.
        allow_bg_tqs (bool): Whether background task queues are permitted.

    Returns:
        dict[float, list[int]]: Mapping of computed priority values to lists
            of task-queue ids that share that priority.
    """

    def is_background(tq_priority: float, allow_bg_tqs: bool) -> bool:
        """Return True when a task queue should be considered background.

        A TQ is considered background when its configured priority is small
        (<= 0.1) and background TQs are allowed by policy.
        """
        return tq_priority <= 0.1 and allow_bg_tqs

    # Calculate Sum of priorities of non background TQs
    total_prio = sum(
        [prio for prio in tq_dict.values() if not is_background(prio, allow_bg_tqs)]
    )

    # Update prio for each TQ
    for tq_id, tq_priority in tq_dict.items():
        if is_background(tq_priority, allow_bg_tqs):
            prio = TQ_MIN_SHARE
        else:
            prio = max((share / total_prio) * tq_priority, TQ_MIN_SHARE)
        tq_dict[tq_id] = prio

    # Generate groups of TQs that will have the same prio=sum(prios) maomenos
    tq_groups: dict[str, list[int]] = defaultdict(list)
    for tq_id, tq_data in all_tqs_data.items():
        for field in ("Jobs", "Priority") + PRIORITY_IGNORED_FIELDS:
            if field in tq_data:
                tq_data.pop(field)
        tq_hash = []
        for f in sorted(tq_data):
            tq_hash.append(f"{f}:{tq_data[f]}")
        tq_hash = "|".join(tq_hash)
        # if tq_hash not in tq_groups:
        #     tq_groups[tq_hash] = []
        tq_groups[tq_hash].append(tq_id)

    # Do the grouping
    for tq_group in tq_groups.values():
        total_prio = sum(tq_dict[tq_id] for tq_id in tq_group)
        for tq_id in tq_group:
            tq_dict[tq_id] = total_prio

    # Group by priorities
    result: dict[float, list[int]] = defaultdict(list)
    for tq_id, tq_priority in tq_dict.items():
        result[tq_priority].append(tq_id)

    return result
