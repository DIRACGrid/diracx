from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from typing import Annotated

from fastapi import Depends, HTTPException, status

from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.db.sql import JobDB, SandboxMetadataDB
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils.users import AuthorizedUserInfo


class ActionType(StrEnum):
    # Create a job or a sandbox
    CREATE = auto()
    # Check job status, download a sandbox
    READ = auto()
    # Delete, kill, remove, set status, etc of a job
    # Delete or assign a sandbox
    MANAGE = auto()
    # Search
    QUERY = auto()
    # Actions from a pilot (e.g. heartbeat)
    PILOT = auto()


class WMSAccessPolicy(BaseAccessPolicy):
    """Rules:
    * You need either NORMAL_USER or JOB_ADMINISTRATOR in your properties
    * An admin cannot create any resource but can read everything and modify everything
    * A NORMAL_USER can create
    * a NORMAL_USER can query and read only his own jobs.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
        job_db: JobDB | None = None,
        job_ids: list[int] | None = None,
    ):
        assert action, "action is a mandatory parameter"
        assert job_db, "job_db is a mandatory parameter"

        if action == ActionType.PILOT:
            # TODO: For now we map this to MANAGE but it should be changed once
            # we have pilot credentials
            action = ActionType.MANAGE

        if action == ActionType.CREATE:
            if job_ids is not None:
                raise NotImplementedError(
                    "job_ids is not None with ActionType.CREATE. This shouldn't happen"
                )
            if NORMAL_USER not in user_info.properties:
                raise HTTPException(status.HTTP_403_FORBIDDEN)
            return

        if JOB_ADMINISTRATOR in user_info.properties:
            return

        if NORMAL_USER not in user_info.properties:
            raise HTTPException(status.HTTP_403_FORBIDDEN)

        if action == ActionType.QUERY:
            if job_ids is not None:
                raise NotImplementedError(
                    "job_ids is not None with ActionType.QUERY. This shouldn't happen"
                )
            return

        if job_ids is None:
            raise NotImplementedError("job_ids is None. his shouldn't happen")

        # TODO: check the CS global job monitoring flag

        # Now we know we are either in READ/MODIFY for a NORMAL_USER
        # so just make sure that whatever job_id was given belongs
        # to the current user
        job_owners = await job_db.summary(
            ["Owner", "VO"],
            [{"parameter": "JobID", "operator": "in", "values": job_ids}],
        )

        expected_owner = {
            "Owner": user_info.preferred_username,
            "VO": user_info.vo,
            "count": len(set(job_ids)),
        }
        # All the jobs belong to the user doing the query
        # and all of them are present
        if job_owners == [expected_owner]:
            return

        raise HTTPException(status.HTTP_403_FORBIDDEN)


CheckWMSPolicyCallable = Annotated[Callable, Depends(WMSAccessPolicy.check)]


class SandboxAccessPolicy(BaseAccessPolicy):
    """Policy for the sandbox.
    They are similar to the WMS access policies.
    """

    @staticmethod
    async def policy(
        policy_name: str,
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType | None = None,
        sandbox_metadata_db: SandboxMetadataDB | None = None,
        pfns: list[str] | None = None,
        required_prefix: str | None = None,
        se_name: str | None = None,
    ):
        assert action, "action is a mandatory parameter"
        assert sandbox_metadata_db, "sandbox_metadata_db is a mandatory parameter"

        if action == ActionType.CREATE:

            if NORMAL_USER not in user_info.properties:
                raise HTTPException(status.HTTP_403_FORBIDDEN)
            return

        if JOB_ADMINISTRATOR in user_info.properties:
            return

        if NORMAL_USER not in user_info.properties:
            raise HTTPException(status.HTTP_403_FORBIDDEN)

        # Getting a sandbox or modifying it
        if pfns:
            if required_prefix is None:
                raise NotImplementedError(
                    "required_prefix is None. This shouldn't happen"
                )
            if se_name is None:
                raise NotImplementedError("se_name is None. This shouldn't happen")
            for pfn in pfns:
                if not pfn.startswith(required_prefix):
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Invalid PFN. PFN must start with {required_prefix}",
                    )
                # Checking if the user owns the sandbox
                owner_id = await sandbox_metadata_db.get_owner_id(user_info)
                sandbox_owner_id = await sandbox_metadata_db.get_sandbox_owner_id(
                    pfn, se_name
                )
                if not owner_id or owner_id != sandbox_owner_id:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"{user_info.preferred_username} is not the owner of the sandbox",
                    )


CheckSandboxPolicyCallable = Annotated[Callable, Depends(SandboxAccessPolicy.check)]
