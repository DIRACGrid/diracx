from __future__ import annotations

import contextlib
import functools
import os
from enum import StrEnum, auto
from typing import Annotated, AsyncIterator, Callable, Self

from fastapi import Depends, HTTPException, status

from diracx.core.extensions import select_from_extension
from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.db.sql import JobDB

from ..auth import AuthorizedUserInfo, verify_dirac_access_token


class ActionType(StrEnum):
    CREATE = auto()
    READ = auto()
    MANAGE = auto()
    QUERY = auto()


async def default_wms_policy(
    user_info: AuthorizedUserInfo,
    /,
    *,
    action: ActionType,
    job_db: JobDB,
    job_ids: list[int] | None = None,
):
    """Implement the JobPolicy"""
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


class BaseAccessPolicy:

    policy: Callable

    @classmethod
    def check(cls) -> Self:
        raise NotImplementedError("This should never be called")

    @contextlib.asynccontextmanager
    async def lifetime_function(self) -> AsyncIterator[None]:
        """A context manager that can be used to run code at startup and shutdown."""
        yield

    @classmethod
    def available_implementations(
        cls, access_policy_name: str
    ) -> list[type[BaseAccessPolicy]]:
        """Return the available implementations of the AccessPolicy in reverse priority order."""
        policy_classes: list[type[BaseAccessPolicy]] = [
            entry_point.load()
            for entry_point in select_from_extension(
                group="diracx.access_policies", name=access_policy_name
            )
        ]
        if not policy_classes:
            raise NotImplementedError(
                f"Could not find any matches for {access_policy_name=}"
            )
        return policy_classes


class WMSAccessPolicy(BaseAccessPolicy):
    policy = staticmethod(default_wms_policy)


def check_permissions(
    access_policy_instance,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
):
    """
    This is what every route should depend on to check user permissions.

    It yield an access policy that needs to be checked.
    If this is declared as a dependency but not called
    """
    has_been_called = False

    # # TODO: query the CS to find the actual policy
    # policy = default_wms_policy

    @functools.wraps(access_policy_instance.policy)
    async def wrapped_policy(**kwargs):
        """This wrapper is just to update the has_been_called flag"""
        nonlocal has_been_called
        has_been_called = True
        return await access_policy_instance.policy(user_info, **kwargs)

    try:
        yield wrapped_policy
    finally:
        if not has_been_called:
            # TODO nice error message with inspect
            # That should really not happen
            print(
                "THIS SHOULD NOT HAPPEN, ALWAYS VERIFY PERMISSION",
                "(PS: I hope you are in a CI)",
                flush=True,
            )
            os._exit(1)


def check_permissions_alone(
    policy,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
):
    """
    This is what every route should depend on to check user permissions.

    It yield an access policy that needs to be checked.
    If this is declared as a dependency but not called
    """
    has_been_called = False

    # # TODO: query the CS to find the actual policy
    # policy = default_wms_policy

    @functools.wraps(policy)
    async def wrapped_policy(**kwargs):
        """This wrapper is just to update the has_been_called flag"""
        nonlocal has_been_called
        has_been_called = True
        return await policy(user_info, **kwargs)

    try:
        yield wrapped_policy
    finally:
        if not has_been_called:
            # TODO nice error message with inspect
            # That should really not happen
            print(
                "THIS SHOULD NOT HAPPEN, ALWAYS VERIFY PERMISSION",
                "(PS: I hope you are in a CI)",
                flush=True,
            )
            os._exit(1)


WMSAccessPolicyCallable = Annotated[Callable, Depends(WMSAccessPolicy.check)]
