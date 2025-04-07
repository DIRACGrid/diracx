from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.properties import (
    GENERIC_PILOT,
    LIMITED_DELEGATION,
)
from diracx.db.sql import JobDB, PilotAgentsDB
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class PilotWMSAccessPolicy(BaseAccessPolicy):
    """Rules:
    * A pilot needs either GENERIC_PILOT or LIMITED_DELEGATION in its properties
    * It has to be associated with the wanted jobs.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        pilot_db: PilotAgentsDB | None = None,
        job_db: JobDB | None = None,
        job_ids: list[int] | None = None,
    ):
        assert job_db, "job_db is a mandatory parameter"
        assert pilot_db, "pilot_db is a mandatory parameter when using a pilot action"
        assert job_ids, "job_ids has to be defined"
        pilot_info = user_info  # For semantic

        # Syntax to avoid code duplication
        if {GENERIC_PILOT, LIMITED_DELEGATION} & set(pilot_info.properties):
            # Get its information
            pilot_data = await pilot_db.get_pilot_by_reference(
                pilot_info.preferred_username
            )

            if not pilot_data:
                raise HTTPException(
                    status.HTTP_403_FORBIDDEN, "this pilot is not registered"
                )

            # Get its jobs
            pilot_jobs = await pilot_db.get_pilot_job_ids(
                pilot_id=pilot_data["PilotID"]
            )

            # Equivalent of issubset, but cleaner
            if set(job_ids) <= set(pilot_jobs):
                return

            forbidden_jobs_ids = set(job_ids) - set(pilot_jobs)

            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                f"this pilot can't access/modify some jobs: ids={forbidden_jobs_ids}",
            )

        raise HTTPException(status.HTTP_403_FORBIDDEN, "you are not a pilot")


CheckPilotWMSPolicyCallable = Annotated[Callable, Depends(PilotWMSAccessPolicy.check)]
