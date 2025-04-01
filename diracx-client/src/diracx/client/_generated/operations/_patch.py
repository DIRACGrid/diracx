from __future__ import annotations

__all__ = [
    "WellKnownOperations",
    "AuthOperations",
    "ConfigOperations",
    "JobsOperations",
]

from diracx.client._generated.operations._operations import (
    WellKnownOperations as _WellKnownOperations,
)

try:
    from diracx.client._patches.operations import (  # type: ignore[attr-defined]
        WellKnownOperations as _WellKnownOperationsPatch,
    )
except ImportError:

    class _WellKnownOperationsPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.operations._operations import (
    AuthOperations as _AuthOperations,
)

try:
    from diracx.client._patches.operations import (  # type: ignore[attr-defined]
        AuthOperations as _AuthOperationsPatch,
    )
except ImportError:

    class _AuthOperationsPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.operations._operations import (
    ConfigOperations as _ConfigOperations,
)

try:
    from diracx.client._patches.operations import (  # type: ignore[attr-defined]
        ConfigOperations as _ConfigOperationsPatch,
    )
except ImportError:

    class _ConfigOperationsPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.operations._operations import (
    JobsOperations as _JobsOperations,
)

try:
    from diracx.client._patches.operations import (  # type: ignore[attr-defined]
        JobsOperations as _JobsOperationsPatch,
    )
except ImportError:

    class _JobsOperationsPatch:  # type: ignore[no-redef]
        pass


class WellKnownOperations(_WellKnownOperationsPatch, _WellKnownOperations):
    pass


class AuthOperations(_AuthOperationsPatch, _AuthOperations):
    pass


class ConfigOperations(_ConfigOperationsPatch, _ConfigOperations):
    pass


class JobsOperations(_JobsOperationsPatch, _JobsOperations):
    pass


def patch_sdk():
    pass
