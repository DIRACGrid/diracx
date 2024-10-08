from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import insert

from ..utils import BaseSQLDB
from .schema import PilotAgents, PilotAgentsDBBase


class PilotAgentsDB(BaseSQLDB):
    metadata = PilotAgentsDBBase.metadata

    async def addPilotReferences(
        self,
        pilot_ref: list[str],
        vo: str,
        grid_type: str = "DIRAC",
        pilot_stamps: dict | None = None,
    ) -> list[int]:

        if pilotStampDict is None:
            pilotStampDict = {}
        row_ids = []
        for ref in pilotRef:
            stamp = pilotStampDict.get(ref, "")
            now = datetime.now(tz=timezone.utc)
            stmt = insert(PilotAgents).values(
                PilotJobReference=ref,
                VO=vo,
                GridType=gridType,
                SubmissionTime=now,
                LastUpdateTime=now,
                Status="Submitted",
                PilotStamp=stamp,
            )
            result = await self.conn.execute(stmt)
            row_ids.append(result.lastrowid)

        return row_ids
