"""SQL database operations for task queues and their associated records."""

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
    """Database queries and updates for task queues.

    Attributes:
        metadata: SQLAlchemy metadata containing the task queue tables.
    """

    metadata = TaskQueueDBBase.metadata

    async def get_tq_infos_for_jobs(
        self, job_ids: list[int]
    ) -> set[tuple[int, str, str, str]]:
        """Get task queue identifiers and ownership for the given jobs.

        Args:
            job_ids: Job identifiers to look up.

        Returns:
            Unique tuples of task queue ID, owner, owner group, and VO.
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
        """Get the owner, owner group, and VO for a task queue.

        Args:
            tq_id: Task queue identifier.

        Returns:
            Mapping containing the task queue owner, owner group, and VO.
        """
        stmt = select(TaskQueues.Owner, TaskQueues.OwnerGroup, TaskQueues.VO).where(
            TaskQueues.TQId == tq_id
        )
        return dict((await self.conn.execute(stmt)).one()._mapping)

    async def get_task_queue_owners_by_group(self, group: str) -> dict[str, int]:
        """Count task queues for each owner in an owner group.

        Args:
            group: Owner group to query.

        Returns:
            Mapping from owner names to their task queue counts.
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
        return {r[0]: r[1] for r in rows}

    async def get_task_queue_priorities(
        self, group: str, owner: str | None = None
    ) -> dict[int, float]:
        """Get average real job priority for task queues in an owner group.

        Args:
            group: Owner group whose task queues should be queried.
            owner: Optional owner name to further filter the task queues.

        Returns:
            Mapping from task queue identifiers to average job priority.
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
        """Remove job-to-task-queue mappings for the specified jobs.

        Args:
            job_ids: Job identifiers to remove from task queues.
        """
        stmt = delete(JobsQueue).where(JobsQueue.JobId.in_(job_ids))
        await self.conn.execute(stmt)

    async def is_task_queue_empty(self, tq_id: int) -> bool:
        """Check whether an enabled task queue has no associated jobs.

        Args:
            tq_id: Task queue identifier.

        Returns:
            Whether the task queue is empty.
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
        """Delete a task queue and its cascading associated records.

        Args:
            tq_id: Task queue identifier to delete.
        """
        # Deleting the task queue (the other tables will be deleted in cascade)
        stmt = delete(TaskQueues).where(TaskQueues.TQId == tq_id)
        await self.conn.execute(stmt)

    async def set_priorities_for_entity(
        self,
        tq_ids: list[int],
        priority: float,
    ):
        """Set the priority for task queues belonging to an entity.

        Args:
            tq_ids: Task queue identifiers to update.
            priority: Priority value to assign.
        """
        update_stmt = (
            update(TaskQueues)
            .where(TaskQueues.TQId.in_(tq_ids))
            .values(Priority=priority)
        )
        await self.conn.execute(update_stmt)

    async def retrieve_task_queues(self, tq_id_list=None):
        """Retrieve task queue details and their associated values.

        Args:
            tq_id_list: Optional task queue identifiers to retrieve. An empty
                list returns no results.

        Returns:
            Mapping from task queue identifiers to queue details, including
            associated sites, grid CEs, platforms, job types, and tags.
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
