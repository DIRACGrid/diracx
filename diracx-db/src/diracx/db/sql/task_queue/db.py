from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select, update

if TYPE_CHECKING:
    pass

from diracx.core.properties import JOB_SHARING, SecurityProperty

from ..utils import BaseSQLDB
from .schema import (
    BannedSitesQueue,
    GridCEsQueue,
    JobsQueue,
    JobTypesQueue,
    PlatformsQueue,
    SitesQueue,
    TagsQueue,
    TaskQueueDBBase,
    TaskQueues,
)


class TaskQueueDB(BaseSQLDB):
    metadata = TaskQueueDBBase.metadata

    async def get_tq_infos_for_jobs(
        self, job_ids: list[int]
    ) -> set[tuple[int, str, str, str]]:
        """Get the task queue info for given jobs."""
        stmt = (
            select(
                TaskQueues.TQId, TaskQueues.Owner, TaskQueues.OwnerGroup, TaskQueues.VO
            )
            .join(JobsQueue, TaskQueues.TQId == JobsQueue.TQId)
            .where(JobsQueue.JobId.in_(job_ids))
        )
        return set(
            (int(row[0]), str(row[1]), str(row[2]), str(row[3]))
            for row in (await self.conn.execute(stmt)).all()
        )

    async def get_owner_for_task_queue(self, tq_id: int) -> dict[str, str]:
        """Get the owner and owner group for a task queue."""
        stmt = select(TaskQueues.Owner, TaskQueues.OwnerGroup, TaskQueues.VO).where(
            TaskQueues.TQId == tq_id
        )
        return dict((await self.conn.execute(stmt)).one()._mapping)

    async def remove_job(self, job_id: int):
        """Remove a job from the task queues."""
        stmt = delete(JobsQueue).where(JobsQueue.JobId == job_id)
        await self.conn.execute(stmt)

    async def remove_jobs(self, job_ids: list[int]):
        """Remove jobs from the task queues."""
        stmt = delete(JobsQueue).where(JobsQueue.JobId.in_(job_ids))
        await self.conn.execute(stmt)

    async def delete_task_queue_if_empty(
        self,
        tq_id: int,
        tq_owner: str,
        tq_group: str,
        job_share: int,
        group_properties: set[SecurityProperty],
        enable_shares_correction: bool,
        allow_background_tqs: bool,
    ):
        """Try to delete a task queue if it's empty."""
        # Check if the task queue is empty
        stmt = (
            select(TaskQueues.TQId)
            .where(TaskQueues.Enabled >= 1)
            .where(TaskQueues.TQId == tq_id)
            .where(~TaskQueues.TQId.in_(select(JobsQueue.TQId)))
        )
        rows = await self.conn.execute(stmt)
        if not rows.rowcount:
            return

        # Deleting the task queue (the other tables will be deleted in cascade)
        stmt = delete(TaskQueues).where(TaskQueues.TQId == tq_id)
        await self.conn.execute(stmt)

        await self.recalculate_tq_shares_for_entity(
            tq_owner,
            tq_group,
            job_share,
            group_properties,
            enable_shares_correction,
            allow_background_tqs,
        )

    async def recalculate_tq_shares_for_entity(
        self,
        owner: str,
        group: str,
        job_share: int,
        group_properties: set[SecurityProperty],
        enable_shares_correction: bool,
        allow_background_tqs: bool,
    ):
        """Recalculate the shares for a user/userGroup combo."""
        if JOB_SHARING in group_properties:
            # If group has JobSharing just set prio for that entry, user is irrelevant
            return await self.__set_priorities_for_entity(
                owner, group, job_share, group_properties, allow_background_tqs
            )

        stmt = (
            select(TaskQueues.Owner, func.count(TaskQueues.Owner))
            .where(TaskQueues.OwnerGroup == group)
            .group_by(TaskQueues.Owner)
        )
        rows = await self.conn.execute(stmt)
        # make the rows a list of tuples
        # Get owners in this group and the amount of times they appear
        # TODO: I guess the rows are already a list of tupes
        # maybe refactor
        data = [(r[0], r[1]) for r in rows if r]
        numOwners = len(data)
        # If there are no owners do now
        if numOwners == 0:
            return
        # Split the share amongst the number of owners
        entities_shares = {row[0]: job_share / numOwners for row in data}

        # TODO: implement the following
        # If corrector is enabled let it work it's magic
        # if enable_shares_correction:
        #     entities_shares = await self.__shares_corrector.correct_shares(
        #         entitiesShares, group=group
        #     )

        # Keep updating
        owners = dict(data)
        # IF the user is already known and has more than 1 tq, the rest of the users don't need to be modified
        # (The number of owners didn't change)
        if owner in owners and owners[owner] > 1:
            await self.__set_priorities_for_entity(
                owner,
                group,
                entities_shares[owner],
                group_properties,
                allow_background_tqs,
            )
            return
        # Oops the number of owners may have changed so we recalculate the prio for all owners in the group
        for owner in owners:
            await self.__set_priorities_for_entity(
                owner,
                group,
                entities_shares[owner],
                group_properties,
                allow_background_tqs,
            )

    async def __set_priorities_for_entity(
        self,
        owner: str,
        group: str,
        share,
        properties: set[SecurityProperty],
        allow_background_tqs: bool,
    ):
        """Set the priority for a user/userGroup combo given a splitted share."""
        from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import calculate_priority

        stmt = (
            select(
                TaskQueues.TQId,
                func.sum(JobsQueue.RealPriority) / func.count(JobsQueue.RealPriority),
            )
            .join(JobsQueue, TaskQueues.TQId == JobsQueue.TQId)
            .where(TaskQueues.OwnerGroup == group)
            .group_by(TaskQueues.TQId)
        )
        if JOB_SHARING not in properties:
            stmt = stmt.where(TaskQueues.Owner == owner)
        rows = await self.conn.execute(stmt)
        tq_dict: dict[int, float] = {tq_id: priority for tq_id, priority in rows}

        if not tq_dict:
            return

        rows = await self.retrieve_task_queues(list(tq_dict))

        prio_dict = calculate_priority(tq_dict, rows, share, allow_background_tqs)

        # Execute updates
        for prio, tqs in prio_dict.items():
            update_stmt = (
                update(TaskQueues).where(TaskQueues.TQId.in_(tqs)).values(Priority=prio)
            )
            await self.conn.execute(update_stmt)

    async def retrieve_task_queues(self, tq_id_list=None):
        """Get all the task queues."""
        if tq_id_list is not None and not tq_id_list:
            # Empty list => Fast-track no matches
            return {}

        stmt = (
            select(
                TaskQueues.TQId,
                TaskQueues.Priority,
                func.count(JobsQueue.TQId).label("Jobs"),
                TaskQueues.Owner,
                TaskQueues.OwnerGroup,
                TaskQueues.VO,
                TaskQueues.CPUTime,
            )
            .join(JobsQueue, TaskQueues.TQId == JobsQueue.TQId)
            .join(SitesQueue, TaskQueues.TQId == SitesQueue.TQId)
            .join(GridCEsQueue, TaskQueues.TQId == GridCEsQueue.TQId)
            .group_by(
                TaskQueues.TQId,
                TaskQueues.Priority,
                TaskQueues.Owner,
                TaskQueues.OwnerGroup,
                TaskQueues.VO,
                TaskQueues.CPUTime,
            )
        )
        if tq_id_list is not None:
            stmt = stmt.where(TaskQueues.TQId.in_(tq_id_list))

        tq_data: dict[int, dict[str, list[str]]] = dict(
            dict(row._mapping) for row in await self.conn.execute(stmt)
        )
        # TODO: the line above should be equivalent to the following commented code, check this is the case
        # for record in rows:
        #     tqId = record[0]
        #     tqData[tqId] = {
        #         "Priority": record[1],
        #         "Jobs": record[2],
        #         "Owner": record[3],
        #         "OwnerGroup": record[4],
        #         "VO": record[5],
        #         "CPUTime": record[6],
        #     }

        for tq_id in tq_data:
            # TODO: maybe factorize this handy tuple list
            for table, field in {
                (SitesQueue, "Sites"),
                (GridCEsQueue, "GridCEs"),
                (BannedSitesQueue, "BannedSites"),
                (PlatformsQueue, "Platforms"),
                (JobTypesQueue, "JobTypes"),
                (TagsQueue, "Tags"),
            }:
                stmt = select(table.Value).where(table.TQId == tq_id)
                tq_data[tq_id][field] = list(
                    row[0] for row in await self.conn.execute(stmt)
                )

        return tq_data
