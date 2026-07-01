"""Job and sandbox access policy definitions for DIRACX routers.

This module defines router-level access control policies for job management
and sandbox operations. It includes action enums, WMS policy rules, and
sandbox-specific PFN/ownership validation logic.
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
    """Access policy for WMS (job) operations.

    This policy enforces access control for job-related actions. Key rules:
    - Either ``NORMAL_USER`` or ``JOB_ADMINISTRATOR`` must be present in the
      requester's properties.
    - ``JOB_ADMINISTRATOR`` may read and modify all resources.
    - ``NORMAL_USER`` may create jobs and may read/query only their own jobs.
    - ``GENERIC_PILOT`` is allowed for pilot-related manage actions.
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
        """Evaluate WMS access policy for the current request.

        Args:
            policy_name (str): Name of the policy invocation (unused by logic).
            user_info (AuthorizedUserInfo): Authenticated user information.
            action (ActionType | None): The action being requested.
            job_db (JobDB | None): Job database access object; required for
                ownership checks when verifying access to specific job IDs.
            job_ids (list[int] | None): Optional list of job IDs the action
                targets. Used to verify ownership for non-admin users.

        Returns:
            None

        Raises:
            HTTPException: If the caller is not authorized for the requested
                action.
            NotImplementedError: If required arguments are missing or an
                unsupported combination of parameters is supplied.
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
    """Access policy for sandbox operations.

    This policy enforces permissions for sandbox creation, reading and
    modification. It follows the general principles of the WMS policy but
    includes additional checks for PFN prefixes and sandbox ownership.
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
        """Evaluate sandbox access policy for the current request.

        Args:
            policy_name (str): Name of the policy invocation (unused by logic).
            user_info (AuthorizedUserInfo): Authenticated user information.
            action (ActionType | None): The action being requested.
            sandbox_metadata_db (SandboxMetadataDB | None): Metadata DB used
                to query sandbox ownership.
            pfns (list[str] | None): Optional list of PFNs to validate and
                check ownership for.
            required_prefix (str | None): Required PFN prefix for validation.
            se_name (str | None): Storage element name used to resolve sandbox
                ownership.

        Returns:
            None

        Raises:
            HTTPException: If the caller is not authorized for the requested
                action or does not own the specified sandbox.
            NotImplementedError: If required arguments are missing or an
                unsupported combination of parameters is supplied.
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
