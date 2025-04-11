"""AccessPolicy.

We define a set of Policy classes (WMS, DFC, etc).
They have a default implementation in diracx.
If an extension wants to change it, it can be overwritten in the entry point
diracx.access_policies

Each route should either:
* have the open_access decorator to make explicit that it does not implement policy
* have a callable and call it that will perform the access policy


Adding a new policy:
1. Create a class that inherits from BaseAccessPolicy and implement the ``policy`` and ``enrich_tokens`` methods
2. create an entry in diracx.access_policy entrypoints
3. Create a dependency such as CheckMyPolicyCallable = Annotated[Callable, Depends(MyAccessPolicy.check)]

"""

import functools
import os
import time
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from typing import Annotated, Self

from fastapi import Depends

from diracx.core.extensions import select_from_extension
from diracx.core.models import (
    AccessTokenPayload,
    RefreshTokenPayload,
)
from diracx.routers.dependencies import DevelopmentSettings
from diracx.routers.utils.users import AuthorizedUserInfo, verify_dirac_access_token

if "annotations" in globals():
    raise NotImplementedError(
        "FastAPI bug: We normally would use `from __future__ import annotations` "
        "but a bug in FastAPI prevents us from doing so "
        "https://github.com/tiangolo/fastapi/pull/11355 "
        "Until it is merged, we can work around it by using strings."
    )


class BaseAccessPolicy(metaclass=ABCMeta):
    """Base class to be used by all the other Access Policy.

    Each child class should implement the policy staticmethod.
    """

    @classmethod
    def check(cls) -> Self:
        """Placeholder which is in the dependency override."""
        raise NotImplementedError("This should never be called")

    @classmethod
    def all_used_access_policies(cls) -> dict[str, "BaseAccessPolicy"]:
        """Returns the list of classes that are actually called.

        This should be overridden by the dependency_override.
        """
        raise NotImplementedError("This should never be called")

    @classmethod
    def available_implementations(cls, access_policy_name: str):
        """Return the available implementations of the AccessPolicy in reverse priority order."""
        policy_classes: list[type["BaseAccessPolicy"]] = [
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

    @staticmethod
    @abstractmethod
    async def policy(policy_name: str, user_info: AuthorizedUserInfo, /):
        """This is the method  to be implemented in child classes.
        It should always take an AuthorizedUserInfo parameter, which
        is passed by check_permissions.
        The rest is whatever the policy actually needs. There are rules to write it:
            * This method must be static and async
            * All parameters must be kw only arguments
            * All parameters must have a default value (Liskov Substitution principle)
        It is expected that a policy denying the access raises HTTPException(status.HTTP_403_FORBIDDEN).
        """
        return

    @staticmethod
    def enrich_tokens(
        access_payload: AccessTokenPayload, refresh_payload: RefreshTokenPayload | None
    ) -> tuple[dict, dict]:
        """This method is called when issuing a token, and can add whatever
        content it wants inside the access or refresh payload.

        :param access_payload: access token payload
        :param refresh_payload: refresh token payload
        :returns: extra content for both payload
        """
        return {}, {}


def check_permissions(
    policy: Callable,
    policy_name: str,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dev_settings: DevelopmentSettings,
):
    """This wrapper just calls the actual implementation, but also makes sure
    that the policy has been called.
    If not, diracx will abruptly crash. It is violent, but necessary to make
    sure that it gets noticed :-).

    This method is never called directly, but used in the dependency_override
    at startup
    """
    has_been_called = False

    @functools.wraps(policy)
    async def wrapped_policy(**kwargs):
        """This wrapper is just to update the has_been_called flag."""
        nonlocal has_been_called
        has_been_called = True
        return await policy(policy_name, user_info, **kwargs)

    try:
        yield wrapped_policy
    finally:

        if not has_been_called:
            # If enable, just crash, meanly
            if dev_settings.crash_on_missed_access_policy:

                # TODO nice error message with inspect
                # It would also be nice to print it when there's a real
                # problem, not when we get 402
                # see https://github.com/DIRACGrid/diracx/issues/275
                print(
                    "THIS SHOULD NOT HAPPEN, ALWAYS VERIFY PERMISSION",
                    "(PS: I hope you are in a CI)",
                    flush=True,
                )
                # Sleep a bit to make sure the flush happened
                time.sleep(1)
                os._exit(1)


def open_access(f):
    """Decorator to put around the route that are part of a DiracxRouter
    that are expected not to do any access policy check.
    The presence of a token will still be checked if the router has require_auth to True.
    This is useful to allow the CI to detect routes which may have forgotten
    to have an access check.
    """
    f.diracx_open_access = True

    @functools.wraps(f)
    def inner(*args, **kwargs):
        return f(*args, **kwargs)

    return inner
