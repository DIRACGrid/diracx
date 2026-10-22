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

from __future__ import annotations

__all__ = ["BaseAccessPolicy", "check_permissions", "open_access"]

import functools
import os
import time
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from typing import Annotated, Self

from fastapi import Depends

from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.models import (
    AccessTokenPayload,
    RefreshTokenPayload,
)
from diracx.core.settings import DevelopmentSettings
from diracx.routers.dependencies import auto_inject
from diracx.routers.utils import AuthorizedUserInfo, verify_dirac_access_token


class BaseAccessPolicy(metaclass=ABCMeta):
    """Base class for access-policy implementations.

    Subclasses implement the asynchronous ``policy`` method. They may also
    override ``enrich_tokens`` to add policy-specific token claims.
    """

    @classmethod
    def check(cls) -> Self:
        """Provide a dependency placeholder replaced during application setup.

        Returns:
            An instance of the concrete access-policy class.

        Raises:
            NotImplementedError: Always; application setup must override this
                dependency before it is used.
        """
        raise NotImplementedError("This should never be called")

    @classmethod
    def all_used_access_policies(cls) -> dict[str, "BaseAccessPolicy"]:
        """Provide a placeholder for retrieving the active policy implementations.

        Returns:
            Mapping from policy names to the implementations used by the application.

        Raises:
            NotImplementedError: Always; application setup must override this
                dependency before it is used.
        """
        raise NotImplementedError("This should never be called")

    @classmethod
    def available_implementations(cls, access_policy_name: str):
        """Load registered implementations of a named access policy.

        Implementations are returned in the priority order provided by the
        extension entry-point selection.

        Args:
            access_policy_name: Entry-point name identifying the policy.

        Returns:
            Registered policy implementation classes.

        Raises:
            NotImplementedError: If no implementation is registered for the name.
        """
        policy_classes: list[type["BaseAccessPolicy"]] = [
            entry_point.load()
            for entry_point in select_from_extension(
                group=DiracEntryPoint.ACCESS_POLICY, name=access_policy_name
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
        """Evaluate whether an authenticated user may perform an action.

        Implementations must be static and asynchronous. ``user_info`` is
        supplied by ``check_permissions``; any additional parameters must be
        keyword-only and have defaults so policies remain substitutable.

        Args:
            policy_name: Name used to select the policy implementation.
            user_info: Authenticated user identity and token claims.

        Raises:
            HTTPException: If the user is not permitted to perform the action.
                Policies must deny access with a 403 status code.
        """
        return

    @staticmethod
    def enrich_tokens(
        access_payload: AccessTokenPayload, refresh_payload: RefreshTokenPayload | None
    ) -> tuple[dict, dict]:
        """Add policy-specific claims to token payloads.

        Override this method to add content to either token payload when a
        token is issued.

        Args:
            access_payload: Access-token claims being issued.
            refresh_payload: Refresh-token claims being issued, if applicable.

        Returns:
            A pair of dictionaries containing additional access-token and
            refresh-token claims, respectively.
        """
        return {}, {}


@auto_inject
def check_permissions(
    policy: Callable,
    policy_name: str,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    dev_settings: DevelopmentSettings,
):
    """Create a dependency wrapper that verifies the route invokes its policy.

    The yielded callable passes the policy name and authenticated user to the
    implementation. If ``crash_on_missed_access_policy`` is enabled, the
    application will terminate after the request if the route failed to call
    the policy.

    This helper is not called directly by route handlers; it is installed as
    a dependency override during application startup.

    Args:
        policy: Policy callable to invoke for the request.
        policy_name: Name associated with the selected policy implementation.
        user_info: Authenticated user information supplied to the policy.
        dev_settings: Development settings controlling missed-policy handling.

    Yields:
        An asynchronous callable that invokes the policy with request arguments.
    """
    has_been_called = False

    @functools.wraps(policy)
    async def wrapped_policy(**kwargs):
        """Mark the policy as invoked and run it with the request arguments.

        Args:
            **kwargs: Policy-specific keyword arguments supplied by the route.

        Returns:
            The result of the policy implementation.
        """
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
    """Mark a route as intentionally exempt from access-policy checks.

    Authentication is still enforced when the router requires it. The marker
    also lets checks distinguish intentional open access from a missing policy
    invocation.

    Args:
        f: Route function to mark as open access.

    Returns:
        A wrapped route function carrying the open-access marker.
    """
    f.diracx_open_access = True

    @functools.wraps(f)
    def inner(*args, **kwargs):
        """Call the original route function unchanged."""
        return f(*args, **kwargs)

    return inner
