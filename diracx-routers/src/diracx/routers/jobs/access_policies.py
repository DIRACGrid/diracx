"""Define job and sandbox authorization policies for router endpoints.

The policies use authenticated user properties and resource ownership to
decide whether create, read, manage, query, and pilot actions are permitted.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum, auto
from http import HTTPStatus
from typing import Annotated

from fastapi import Depends, HTTPException

from diracx.core.models import VectorSearchOperator
from diracx.core.properties import GENERIC_PILOT, JOB_ADMINISTRATOR, NORMAL_USER
from diracx.db.sql import JobDB, SandboxMetadataDB
from diracx.routers.access_policies import BaseAccessPolicy
from diracx.routers.utils import AuthorizedUserInfo


class ActionType(StrEnum):
    """Actions checked by the workload-management and sandbox policies."""

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
    """Authorize workload-management actions for users and pilots.

    Users need the ``NORMAL_USER`` or ``JOB_ADMINISTRATOR`` property. Normal
    users may create and query jobs, but may read or manage only their own
    jobs. Job administrators may read and manage all jobs but cannot create
    them. Generic pilots may perform manage actions.
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
        """Check whether the user may perform a workload-management action.

        Pilot actions are currently treated as manage actions. For normal
        users, job reads and management are allowed only when all requested
        jobs belong to the authenticated user.

        Args:
            policy_name: Name of the access policy being evaluated.
            user_info: Authenticated user identity and security properties.
            action: Workload-management action to authorize.
            job_db: Database used to look up job ownership.
            job_ids: Jobs affected by a read or manage action.

        Raises:
            HTTPException: If the user's properties or job ownership do not
                permit the action (403).
            NotImplementedError: If required policy inputs are missing or
                supplied for an incompatible action.
        """
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
                raise HTTPException(HTTPStatus.FORBIDDEN)
            return

        if GENERIC_PILOT in user_info.properties and action == ActionType.MANAGE:
            # Authorize pilots
            return

        if JOB_ADMINISTRATOR in user_info.properties:
            return

        if NORMAL_USER not in user_info.properties:
            raise HTTPException(HTTPStatus.FORBIDDEN)

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
            [
                {
                    "parameter": "JobID",
                    "operator": VectorSearchOperator.IN,
                    "values": job_ids,
                }
            ],
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

        raise HTTPException(HTTPStatus.FORBIDDEN)


CheckWMSPolicyCallable = Annotated[Callable, Depends(WMSAccessPolicy.check)]


class SandboxAccessPolicy(BaseAccessPolicy):
    """Authorize creation, retrieval, and modification of sandboxes.

    Normal users may create sandboxes and may access or modify only sandboxes
    they own. Job administrators may access and modify any sandbox.
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
        """Check whether the user may perform a sandbox action.

        For sandbox access or modification, each PFN must have the required
        prefix and belong to the authenticated user unless they are a job
        administrator.

        Args:
            policy_name: Name of the access policy being evaluated.
            user_info: Authenticated user identity and security properties.
            action: Sandbox action to authorize.
            sandbox_metadata_db: Database used to check sandbox ownership.
            pfns: Physical file names of the sandboxes being accessed or modified.
            required_prefix: Required prefix for each sandbox PFN.
            se_name: Storage-element name used to look up sandbox ownership.

        Raises:
            HTTPException: If the user's properties, PFN prefix, or sandbox
                ownership do not permit the action (403).
            NotImplementedError: If required policy inputs are missing.
        """
        assert action, "action is a mandatory parameter"
        assert sandbox_metadata_db, "sandbox_metadata_db is a mandatory parameter"

        if action == ActionType.CREATE:
            if NORMAL_USER not in user_info.properties:
                raise HTTPException(HTTPStatus.FORBIDDEN)
            return

        if JOB_ADMINISTRATOR in user_info.properties:
            return

        if NORMAL_USER not in user_info.properties:
            raise HTTPException(HTTPStatus.FORBIDDEN)

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
                        status_code=HTTPStatus.FORBIDDEN,
                        detail=f"Invalid PFN. PFN must start with {required_prefix}",
                    )
                # Checking if the user owns the sandbox
                owner_id = await sandbox_metadata_db.get_owner_id(user_info)
                sandbox_owner_id = await sandbox_metadata_db.get_sandbox_owner_id(
                    pfn, se_name
                )
                if not owner_id or owner_id != sandbox_owner_id:
                    raise HTTPException(
                        status_code=HTTPStatus.FORBIDDEN,
                        detail=f"{user_info.preferred_username} is not the owner of the sandbox",
                    )


CheckSandboxPolicyCallable = Annotated[Callable, Depends(SandboxAccessPolicy.check)]
