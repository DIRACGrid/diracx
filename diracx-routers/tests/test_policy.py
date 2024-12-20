from __future__ import annotations

import inspect
from collections import defaultdict
from typing import TYPE_CHECKING

from diracx.core.extensions import select_from_extension
from diracx.routers.access_policies import (
    BaseAccessPolicy,
)

if TYPE_CHECKING:
    from diracx.routers.fastapi_classes import DiracxRouter


def test_all_routes_have_policy():
    """Loop over all the routers, loop over every route,
    and make sure there is a dependency on a BaseAccessPolicy class.

    If the router is created with "require_auth=False", we skip it.
    We also skip routes that have the "diracx_open_access" decorator

    """
    missing_security: defaultdict[list[str]] = defaultdict(list)
    for entry_point in select_from_extension(group="diracx.services"):
        router: DiracxRouter = entry_point.load()

        # If the router was created with the
        # require_auth = False, skip it
        if not router.diracx_require_auth:
            continue

        for route in router.routes:

            # If the route is decorated with the diracx_open_access
            # decorator, we skip it
            if getattr(route.endpoint, "diracx_open_access", False):
                continue

            for dependency in route.dependant.dependencies:
                if inspect.ismethod(dependency.call) and issubclass(
                    dependency.call.__self__, BaseAccessPolicy
                ):
                    # We found a dependency on check_permissions
                    break
            else:
                # We looked at all dependency without finding
                # check_permission
                missing_security[entry_point.name].append(route.name)

    assert not missing_security
