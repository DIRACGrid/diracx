from __future__ import annotations

__all__ = [
    "BodyAuthGetOidcToken",
    "BodyAuthGetOidcTokenGrantType",
    "GroupInfo",
    "HTTPValidationError",
    "InitiateDeviceFlowResponse",
    "InsertedJob",
    "JobSearchParams",
    "JobSearchParamsSearchItem",
    "JobStatusUpdate",
    "JobSummaryParams",
    "JobSummaryParamsSearchItem",
    "Metadata",
    "OpenIDConfiguration",
    "SandboxDownloadResponse",
    "SandboxInfo",
    "SandboxUploadResponse",
    "ScalarSearchSpec",
    "ScalarSearchSpecValue",
    "SetJobStatusReturn",
    "SetJobStatusReturnSuccess",
    "SortSpec",
    "SupportInfo",
    "TokenResponse",
    "UserInfoResponse",
    "VOInfo",
    "ValidationError",
    "ValidationErrorLocItem",
    "VectorSearchSpec",
    "VectorSearchSpecValues",
]

from diracx.client._generated.models._models import (
    BodyAuthGetOidcToken as _BodyAuthGetOidcToken,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        BodyAuthGetOidcToken as _BodyAuthGetOidcTokenPatch,
    )
except ImportError:

    class _BodyAuthGetOidcTokenPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    BodyAuthGetOidcTokenGrantType as _BodyAuthGetOidcTokenGrantType,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        BodyAuthGetOidcTokenGrantType as _BodyAuthGetOidcTokenGrantTypePatch,
    )
except ImportError:

    class _BodyAuthGetOidcTokenGrantTypePatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import GroupInfo as _GroupInfo

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        GroupInfo as _GroupInfoPatch,
    )
except ImportError:

    class _GroupInfoPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    HTTPValidationError as _HTTPValidationError,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        HTTPValidationError as _HTTPValidationErrorPatch,
    )
except ImportError:

    class _HTTPValidationErrorPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    InitiateDeviceFlowResponse as _InitiateDeviceFlowResponse,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        InitiateDeviceFlowResponse as _InitiateDeviceFlowResponsePatch,
    )
except ImportError:

    class _InitiateDeviceFlowResponsePatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import InsertedJob as _InsertedJob

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        InsertedJob as _InsertedJobPatch,
    )
except ImportError:

    class _InsertedJobPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import JobSearchParams as _JobSearchParams

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobSearchParams as _JobSearchParamsPatch,
    )
except ImportError:

    class _JobSearchParamsPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    JobSearchParamsSearchItem as _JobSearchParamsSearchItem,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobSearchParamsSearchItem as _JobSearchParamsSearchItemPatch,
    )
except ImportError:

    class _JobSearchParamsSearchItemPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import JobStatusUpdate as _JobStatusUpdate

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobStatusUpdate as _JobStatusUpdatePatch,
    )
except ImportError:

    class _JobStatusUpdatePatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    JobSummaryParams as _JobSummaryParams,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobSummaryParams as _JobSummaryParamsPatch,
    )
except ImportError:

    class _JobSummaryParamsPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    JobSummaryParamsSearchItem as _JobSummaryParamsSearchItem,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobSummaryParamsSearchItem as _JobSummaryParamsSearchItemPatch,
    )
except ImportError:

    class _JobSummaryParamsSearchItemPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import Metadata as _Metadata

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        Metadata as _MetadataPatch,
    )
except ImportError:

    class _MetadataPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    OpenIDConfiguration as _OpenIDConfiguration,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        OpenIDConfiguration as _OpenIDConfigurationPatch,
    )
except ImportError:

    class _OpenIDConfigurationPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    SandboxDownloadResponse as _SandboxDownloadResponse,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SandboxDownloadResponse as _SandboxDownloadResponsePatch,
    )
except ImportError:

    class _SandboxDownloadResponsePatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import SandboxInfo as _SandboxInfo

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SandboxInfo as _SandboxInfoPatch,
    )
except ImportError:

    class _SandboxInfoPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    SandboxUploadResponse as _SandboxUploadResponse,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SandboxUploadResponse as _SandboxUploadResponsePatch,
    )
except ImportError:

    class _SandboxUploadResponsePatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    ScalarSearchSpec as _ScalarSearchSpec,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        ScalarSearchSpec as _ScalarSearchSpecPatch,
    )
except ImportError:

    class _ScalarSearchSpecPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    ScalarSearchSpecValue as _ScalarSearchSpecValue,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        ScalarSearchSpecValue as _ScalarSearchSpecValuePatch,
    )
except ImportError:

    class _ScalarSearchSpecValuePatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    SetJobStatusReturn as _SetJobStatusReturn,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SetJobStatusReturn as _SetJobStatusReturnPatch,
    )
except ImportError:

    class _SetJobStatusReturnPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    SetJobStatusReturnSuccess as _SetJobStatusReturnSuccess,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SetJobStatusReturnSuccess as _SetJobStatusReturnSuccessPatch,
    )
except ImportError:

    class _SetJobStatusReturnSuccessPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import SortSpec as _SortSpec

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SortSpec as _SortSpecPatch,
    )
except ImportError:

    class _SortSpecPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import SupportInfo as _SupportInfo

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SupportInfo as _SupportInfoPatch,
    )
except ImportError:

    class _SupportInfoPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import TokenResponse as _TokenResponse

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        TokenResponse as _TokenResponsePatch,
    )
except ImportError:

    class _TokenResponsePatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    UserInfoResponse as _UserInfoResponse,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        UserInfoResponse as _UserInfoResponsePatch,
    )
except ImportError:

    class _UserInfoResponsePatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import VOInfo as _VOInfo

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        VOInfo as _VOInfoPatch,
    )
except ImportError:

    class _VOInfoPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import ValidationError as _ValidationError

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        ValidationError as _ValidationErrorPatch,
    )
except ImportError:

    class _ValidationErrorPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    ValidationErrorLocItem as _ValidationErrorLocItem,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        ValidationErrorLocItem as _ValidationErrorLocItemPatch,
    )
except ImportError:

    class _ValidationErrorLocItemPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    VectorSearchSpec as _VectorSearchSpec,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        VectorSearchSpec as _VectorSearchSpecPatch,
    )
except ImportError:

    class _VectorSearchSpecPatch:  # type: ignore[no-redef]
        pass


from diracx.client._generated.models._models import (
    VectorSearchSpecValues as _VectorSearchSpecValues,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        VectorSearchSpecValues as _VectorSearchSpecValuesPatch,
    )
except ImportError:

    class _VectorSearchSpecValuesPatch:  # type: ignore[no-redef]
        pass


class BodyAuthGetOidcToken(_BodyAuthGetOidcTokenPatch, _BodyAuthGetOidcToken):
    pass


class BodyAuthGetOidcTokenGrantType(
    _BodyAuthGetOidcTokenGrantTypePatch, _BodyAuthGetOidcTokenGrantType
):
    pass


class GroupInfo(_GroupInfoPatch, _GroupInfo):
    pass


class HTTPValidationError(_HTTPValidationErrorPatch, _HTTPValidationError):
    pass


class InitiateDeviceFlowResponse(
    _InitiateDeviceFlowResponsePatch, _InitiateDeviceFlowResponse
):
    pass


class InsertedJob(_InsertedJobPatch, _InsertedJob):
    pass


class JobSearchParams(_JobSearchParamsPatch, _JobSearchParams):
    pass


class JobSearchParamsSearchItem(
    _JobSearchParamsSearchItemPatch, _JobSearchParamsSearchItem
):
    pass


class JobStatusUpdate(_JobStatusUpdatePatch, _JobStatusUpdate):
    pass


class JobSummaryParams(_JobSummaryParamsPatch, _JobSummaryParams):
    pass


class JobSummaryParamsSearchItem(
    _JobSummaryParamsSearchItemPatch, _JobSummaryParamsSearchItem
):
    pass


class Metadata(_MetadataPatch, _Metadata):
    pass


class OpenIDConfiguration(_OpenIDConfigurationPatch, _OpenIDConfiguration):
    pass


class SandboxDownloadResponse(_SandboxDownloadResponsePatch, _SandboxDownloadResponse):
    pass


class SandboxInfo(_SandboxInfoPatch, _SandboxInfo):
    pass


class SandboxUploadResponse(_SandboxUploadResponsePatch, _SandboxUploadResponse):
    pass


class ScalarSearchSpec(_ScalarSearchSpecPatch, _ScalarSearchSpec):
    pass


class ScalarSearchSpecValue(_ScalarSearchSpecValuePatch, _ScalarSearchSpecValue):
    pass


class SetJobStatusReturn(_SetJobStatusReturnPatch, _SetJobStatusReturn):
    pass


class SetJobStatusReturnSuccess(
    _SetJobStatusReturnSuccessPatch, _SetJobStatusReturnSuccess
):
    pass


class SortSpec(_SortSpecPatch, _SortSpec):
    pass


class SupportInfo(_SupportInfoPatch, _SupportInfo):
    pass


class TokenResponse(_TokenResponsePatch, _TokenResponse):
    pass


class UserInfoResponse(_UserInfoResponsePatch, _UserInfoResponse):
    pass


class VOInfo(_VOInfoPatch, _VOInfo):
    pass


class ValidationError(_ValidationErrorPatch, _ValidationError):
    pass


class ValidationErrorLocItem(_ValidationErrorLocItemPatch, _ValidationErrorLocItem):
    pass


class VectorSearchSpec(_VectorSearchSpecPatch, _VectorSearchSpec):
    pass


class VectorSearchSpecValues(_VectorSearchSpecValuesPatch, _VectorSearchSpecValues):
    pass


def patch_sdk():
    pass


from typing import TYPE_CHECKING
from diracx.client._patches.models import (
    DeviceFlowErrorResponse,
)

if TYPE_CHECKING:
    __all__.extend(
        [
            "DeviceFlowErrorResponse",
        ]
    )
else:
    from diracx.client._patches.models import __all__ as _patch_all

    __all__.extend(_patch_all)
    __all__ = sorted(set(__all__))
