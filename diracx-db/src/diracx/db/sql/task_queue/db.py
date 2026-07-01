"""Task queue SQL DB access helpers.

This module implements helper methods for querying and managing task queue
metadata and task queue job assignments.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select, update

if TYPE_CHECKING:
    pass

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
    """Database helper for task queue.

    Attributes:
        metadata: SQLAlchemy metadata bound from :class:`TaskQueueDBBase`.
    """

    metadata = TaskQueueDBBase.metadata

    async def get_tq_infos_for_jobs(
        self, job_ids: list[int]
    ) -> set[tuple[int, str, str, str]]:
        """Get the task queue info for given jobs.

        Args:
            job_ids (list[int]): Job IDs to query.

        Returns:
            set[tuple[int, str, str, str]]: Set of tuples with task queue id,
                owner, owner group, and VO.
        """
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
        """Get the owner and owner group for a task queue.

        Args:
            tq_id (int): Task queue identifier.

        Returns:
            dict[str, str]: Mapping containing owner, owner group, and VO.
        """
        stmt = select(TaskQueues.Owner, TaskQueues.OwnerGroup, TaskQueues.VO).where(
            TaskQueues.TQId == tq_id
        )
        return dict((await self.conn.execute(stmt)).one()._mapping)

    async def get_task_queue_owners_by_group(self, group: str) -> dict[str, int]:
        """Get the owners for a task queue and group.

        Args:
            group (str): Owner group name.

        Returns:
            dict[str, int]: Mapping from owner name to count of queues.
        """
        stmt = (
            select(TaskQueues.Owner, func.count(TaskQueues.Owner))
            .where(TaskQueues.OwnerGroup == group)
            .group_by(TaskQueues.Owner)
        )
        rows = await self.conn.execute(stmt)
        # Get owners in this group and the amount of times they appear
        # TODO: I guess the rows are already a list of tuples
        # maybe refactor
        return {r[0]: r[1] for r in rows if r}

    async def get_task_queue_priorities(
        self, group: str, owner: str | None = None
    ) -> dict[int, float]:
        """Get the priorities for task queues.

        Args:
            group (str): Owner group to filter by.
            owner (str | None): Optional owner name to filter by.

        Returns:
            dict[int, float]: Mapping from task queue id to average priority.
        """
        stmt = (
            select(
                TaskQueues.TQId,
                func.sum(JobsQueue.RealPriority) / func.count(JobsQueue.RealPriority),
            )
            .join(JobsQueue, TaskQueues.TQId == JobsQueue.TQId)
            .where(TaskQueues.OwnerGroup == group)
            .group_by(TaskQueues.TQId)
        )
        if owner:
            stmt = stmt.where(TaskQueues.Owner == owner)
        rows = await self.conn.execute(stmt)
        return {tq_id: priority for tq_id, priority in rows}

    async def remove_jobs(self, job_ids: list[int]):
        """Remove jobs from the task queues.

        Args:
            job_ids (list[int]): Job IDs to remove from the task queues.
        """
        stmt = delete(JobsQueue).where(JobsQueue.JobId.in_(job_ids))
        await self.conn.execute(stmt)

    async def is_task_queue_empty(self, tq_id: int) -> bool:
        """Check if a task queue is empty.

        Args:
            tq_id (int): Task queue identifier.

        Returns:
            bool: ``True`` if the queue is empty, otherwise ``False``.
        """
        stmt = (
            select(TaskQueues.TQId)
            .where(TaskQueues.Enabled >= 1)
            .where(TaskQueues.TQId == tq_id)
            .where(~TaskQueues.TQId.in_(select(JobsQueue.TQId)))
        )
        rows = await self.conn.execute(stmt)
        return not rows.rowcount

    async def delete_task_queue(
        self,
        tq_id: int,
    ):
        """Delete a task queue.

        Args:
            tq_id (int): Task queue identifier to delete.
        """
        # Deleting the task queue (the other tables will be deleted in cascade)
        stmt = delete(TaskQueues).where(TaskQueues.TQId == tq_id)
        await self.conn.execute(stmt)

    async def set_priorities_for_entity(
        self,
        tq_ids: list[int],
        priority: float,
    ):
        """Set priorities for a list of task queues.

        Args:
            tq_ids (list[int]): Task queue ids to update.
            priority (float): Priority value to set.
        """
        update_stmt = (
            update(TaskQueues)
            .where(TaskQueues.TQId.in_(tq_ids))
            .values(Priority=priority)
        )
        await self.conn.execute(update_stmt)

    async def retrieve_task_queues(self, tq_id_list=None):
        """Get all task queues or a filtered subset.

        Args:
            tq_id_list (list[int] | None): Optional list of task queue ids to limit the query.

        Returns:
            dict[int, dict[str, list[str]]]: Mapping from task queue id to data
                including sites, CEs, banned sites, platforms, job types, and tags.
        """
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
