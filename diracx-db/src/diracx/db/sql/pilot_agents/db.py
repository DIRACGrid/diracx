from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import insert

from ..utils import BaseSQLDB
from .schema import PilotAgents, PilotAgentsDBBase


class PilotAgentsDB(BaseSQLDB):
    metadata = PilotAgentsDBBase.metadata

    async def addPilotReferences(
        self,
        pilotRef: list[str],
        ownerGroup: str,
        gridType: str = "DIRAC",
        pilotStampDict: dict = {},
    ) -> list[int]:

        row_ids = []
        for ref in pilotRef:
            stamp = ""
            if ref in pilotStampDict:
                stamp = pilotStampDict[ref]
            now = datetime.now(tz=timezone.utc)
            stmt = insert(PilotAgents).values(
                PilotJobReference=ref,
                TaskQueueID=0,
                OwnerDN="Unknown",
                OwnerGroup=ownerGroup,
                GridType=gridType,
                SubmissionTime=now,
                LastUpdateTime=now,
                Status="submitted",
                PilotStamp=stamp,
            )
            result = await self.conn.execute(stmt)
            row_ids.append(result.lastrowid)

        return row_ids
