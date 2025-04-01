from __future__ import annotations

__all__ = [
    "WellKnownOperations",
    "AuthOperations",
    "ConfigOperations",
    "JobsOperations",
    "LollygagOperations",
]

from gubbins.client._generated.aio.operations._operations import (
    WellKnownOperations as _WellKnownOperations,
)

try:
    from diracx.client._patches.aio.operations import (  # type: ignore[attr-defined]
        WellKnownOperations as _WellKnownOperationsPatch,
    )
except ImportError:

    class _WellKnownOperationsPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.aio.operations import (  # type: ignore[attr-defined]
        WellKnownOperations as _WellKnownOperationsPatchExt,
    )
except ImportError:

    class _WellKnownOperationsPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.aio.operations._operations import (
    AuthOperations as _AuthOperations,
)

try:
    from diracx.client._patches.aio.operations import (  # type: ignore[attr-defined]
        AuthOperations as _AuthOperationsPatch,
    )
except ImportError:

    class _AuthOperationsPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.aio.operations import (  # type: ignore[attr-defined]
        AuthOperations as _AuthOperationsPatchExt,
    )
except ImportError:

    class _AuthOperationsPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.aio.operations._operations import (
    ConfigOperations as _ConfigOperations,
)

try:
    from diracx.client._patches.aio.operations import (  # type: ignore[attr-defined]
        ConfigOperations as _ConfigOperationsPatch,
    )
except ImportError:

    class _ConfigOperationsPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.aio.operations import (  # type: ignore[attr-defined]
        ConfigOperations as _ConfigOperationsPatchExt,
    )
except ImportError:

    class _ConfigOperationsPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.aio.operations._operations import (
    JobsOperations as _JobsOperations,
)

try:
    from diracx.client._patches.aio.operations import (  # type: ignore[attr-defined]
        JobsOperations as _JobsOperationsPatch,
    )
except ImportError:

    class _JobsOperationsPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.aio.operations import (  # type: ignore[attr-defined]
        JobsOperations as _JobsOperationsPatchExt,
    )
except ImportError:

    class _JobsOperationsPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.aio.operations._operations import (
    LollygagOperations as _LollygagOperations,
)

try:
    from diracx.client._patches.aio.operations import (  # type: ignore[attr-defined]
        LollygagOperations as _LollygagOperationsPatch,
    )
except ImportError:

    class _LollygagOperationsPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.aio.operations import (  # type: ignore[attr-defined]
        LollygagOperations as _LollygagOperationsPatchExt,
    )
except ImportError:

    class _LollygagOperationsPatchExt:  # type: ignore[no-redef]
        pass


class WellKnownOperations(
    _WellKnownOperationsPatchExt, _WellKnownOperationsPatch, _WellKnownOperations
):
    pass


class AuthOperations(_AuthOperationsPatchExt, _AuthOperationsPatch, _AuthOperations):
    pass


class ConfigOperations(
    _ConfigOperationsPatchExt, _ConfigOperationsPatch, _ConfigOperations
):
    pass


class JobsOperations(_JobsOperationsPatchExt, _JobsOperationsPatch, _JobsOperations):
    pass


class LollygagOperations(
    _LollygagOperationsPatchExt, _LollygagOperationsPatch, _LollygagOperations
):
    pass


def patch_sdk():
    pass
