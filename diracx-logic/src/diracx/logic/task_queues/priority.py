from __future__ import annotations

from collections import defaultdict
from typing import Any

from diracx.core.config.schema import Config
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
    """Recalculate the shares for a user/userGroup combo."""
    group_properties = config.Registry[vo].Groups[owner_group].Properties
    job_share = config.Registry[vo].Groups[owner_group].JobShare
    allow_background_tqs = config.Registry[vo].Groups[owner_group].AllowBackgroundTQs
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
    """Set the priority for a user/userGroup combo given a splitted share."""
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
    """Calculate the priority for each TQ given a share.

    :param tq_dict: dict of {tq_id: prio}
    :param all_tqs_data: dict of {tq_id: {tq_data}}, where tq_data is a dict of {field: value}
    :param share: share to be distributed among TQs
    :param allow_bg_tqs: allow background TQs to be used
    :return: dict of {priority: [tq_ids]}
    """

    def is_background(tq_priority: float, allow_bg_tqs: bool) -> bool:
        """A TQ is background if its priority is below a threshold and background TQs are allowed."""
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
