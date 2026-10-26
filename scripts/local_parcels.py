#!/usr/bin/env python3
"""Seed and list parcels in the local TransformationDB.

Run inside ``pixi run local-shell``, which points at the local-start databases::

    python scripts/local_parcels.py seed [N]   # insert N Unassigned compute parcels (default 3)
    python scripts/local_parcels.py list       # show every parcel and its status
"""

from __future__ import annotations

import asyncio
import sys

from sqlalchemy import insert, select
from uuid_utils import uuid7

from diracx.core.models.transformation import InputStatus, TransformationKind
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.schema import (
    Inputs,
    Ownership,
    ParcelInputs,
    Parcels,
    Transformations,
)
from diracx.db.sql.utils import BaseSQLDB


async def seed(db: TransformationDB, n: int) -> None:
    ownership_id, transformation_id = uuid7(), uuid7()
    await db.conn.execute(
        insert(Ownership).values(ownership_id=ownership_id, vo="diracAdmin", sub="me")
    )
    await db.conn.execute(
        insert(Transformations).values(
            transformation_id=transformation_id,
            ownership_id=ownership_id,
            type="MCSimulation",
            kind=TransformationKind.COMPUTE,
            packer="Standard",
        )
    )
    # One Assigned input per Unassigned parcel, as a packer would leave them
    for _ in range(n):
        parcel_id, input_id = uuid7(), uuid7()
        await db.conn.execute(
            insert(Parcels).values(
                parcel_id=parcel_id,
                transformation_id=transformation_id,
                kind=TransformationKind.COMPUTE,
            )
        )
        await db.conn.execute(
            insert(Inputs).values(
                transformation_id=transformation_id,
                input_id=input_id,
                input_hash=input_id.bytes,
                status=InputStatus.ASSIGNED,
                last_parcel_id=parcel_id,
            )
        )
        await db.conn.execute(
            insert(ParcelInputs).values(
                transformation_id=transformation_id,
                parcel_id=parcel_id,
                input_id=input_id,
            )
        )
        print(f"Created Unassigned parcel {parcel_id}")


async def show(db: TransformationDB) -> None:
    rows = await db.conn.execute(
        select(Parcels.parcel_id, Parcels.status, Parcels.backend)
        .outerjoin(Inputs, Inputs.last_parcel_id == Parcels.parcel_id)
        .add_columns(Inputs.status)
    )
    for parcel_id, status, backend, input_status in rows:
        print(
            f"{parcel_id}  {status.value:<11} backend={backend}  input={input_status}"
        )


async def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "list"
    db = TransformationDB(BaseSQLDB.available_urls()["TransformationDB"])
    async with db.engine_context(), db:
        if command == "seed":
            await seed(db, int(sys.argv[2]) if len(sys.argv) > 2 else 3)
        else:
            await show(db)


asyncio.run(main())
