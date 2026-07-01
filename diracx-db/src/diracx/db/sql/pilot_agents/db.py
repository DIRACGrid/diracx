"""Pilot agents SQL DB helpers.

This module provides the :class:`PilotAgentsDB` helper used to insert and
manage pilot reference records in the ``PilotAgents`` table.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import insert

from ..utils import BaseSQLDB
from .schema import PilotAgents, PilotAgentsDBBase


class PilotAgentsDB(BaseSQLDB):
    """Front-end for the PilotAgents database.

    Attributes:
        metadata: SQLAlchemy metadata bound from :class:`PilotAgentsDBBase`.
    """

    metadata = PilotAgentsDBBase.metadata

    async def add_pilot_references(
        self,
        pilot_ref: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        pilot_stamps: dict | None = None,
    ) -> None:
        """Bulk insert pilot reference records into the DB.

        Inserts one row per value in ``pilot_ref`` into the ``PilotAgents``
        table. For each inserted row the method sets ``SubmissionTime``,
        ``LastUpdateTime`` to the current UTC time and ``Status`` to
        ``"Submitted"``.

        Args:
            pilot_ref (list[str]): Pilot job reference strings to insert.
            vo (str): Virtual organization name.
            grid_type (str): Grid type string. Defaults to ``"DIRAC"``.
            pilot_stamps (dict | None): Optional mapping of pilot reference to
                a pilot stamp string to store alongside the record.

        Returns:
            None
        """
        if pilot_stamps is None:
            pilot_stamps = {}

        now = datetime.now(tz=timezone.utc)

        # Prepare the list of dictionaries for bulk insertion
        values = [
            {
                "PilotJobReference": ref,
                "VO": vo,
                "GridType": grid_type,
                "SubmissionTime": now,
                "LastUpdateTime": now,
                "Status": "Submitted",
                "PilotStamp": pilot_stamps.get(ref, ""),
            }
            for ref in pilot_ref
        ]

        # Insert multiple rows in a single execute call
        stmt = insert(PilotAgents).values(values)
        await self.conn.execute(stmt)
        return
