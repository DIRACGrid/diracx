from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, insert, select, update

from diracx.core.exceptions import InvalidQueryError
from diracx.core.utils import JobStatus

from diracx.db.utils import BaseDB
from .schema import CustomObject

class myDB(BaseDB):
    metadata = CustomObject.metadata

    async def _insert(self, Data: dict[str, Any]):
        stmt = insert(CustomObject).values(Data)
        return await self.conn.execute(stmt)

    async def insert(
        self,
        PathValueAsString,
        IntegerValue,
    ):
        InitialUpdate = datetime.now(tz=timezone.utc)
        attrs = dict()
        attrs["PathValueAsString"] = PathValueAsString
        attrs["IntegerValue"] = IntegerValue
        attrs["InitialUpdate"] = InitialUpdate
        attrs["LastUpdate"] = InitialUpdate

        result = await self._insert(attrs)

        return {
               "ID": result.lastrowid,
               "PathValueAsString": PathValueAsString,
               "IntegerValue": IntegerValue,
               "InitialUpdate": InitialUpdate,
               "LastUpdate": InitialUpdate,
           }

    async def search(
        self,
        PathValue,
    ):
        # Find which columns to select
        columns = [x for x in CustomObject.__table__.columns]
        stmt = select(*columns).where(CustomObject.PathValueAsString==PathValue)
        # Execute the query
        return [dict(row._mapping) async for row in (await self.conn.stream(stmt))]

