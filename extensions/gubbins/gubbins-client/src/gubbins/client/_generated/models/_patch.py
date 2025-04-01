from __future__ import annotations

__all__ = [
    "BodyAuthGetOidcToken",
    "BodyAuthGetOidcTokenGrantType",
    "ExtendedMetadata",
    "GroupInfo",
    "HTTPValidationError",
    "InitiateDeviceFlowResponse",
    "InsertedJob",
    "JobSearchParams",
    "JobSearchParamsSearchItem",
    "JobStatusUpdate",
    "JobSummaryParams",
    "JobSummaryParamsSearchItem",
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

from gubbins.client._generated.models._models import (
    BodyAuthGetOidcToken as _BodyAuthGetOidcToken,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        BodyAuthGetOidcToken as _BodyAuthGetOidcTokenPatch,
    )
except ImportError:

    class _BodyAuthGetOidcTokenPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        BodyAuthGetOidcToken as _BodyAuthGetOidcTokenPatchExt,
    )
except ImportError:

    class _BodyAuthGetOidcTokenPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    BodyAuthGetOidcTokenGrantType as _BodyAuthGetOidcTokenGrantType,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        BodyAuthGetOidcTokenGrantType as _BodyAuthGetOidcTokenGrantTypePatch,
    )
except ImportError:

    class _BodyAuthGetOidcTokenGrantTypePatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        BodyAuthGetOidcTokenGrantType as _BodyAuthGetOidcTokenGrantTypePatchExt,
    )
except ImportError:

    class _BodyAuthGetOidcTokenGrantTypePatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    ExtendedMetadata as _ExtendedMetadata,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        ExtendedMetadata as _ExtendedMetadataPatch,
    )
except ImportError:

    class _ExtendedMetadataPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        ExtendedMetadata as _ExtendedMetadataPatchExt,
    )
except ImportError:

    class _ExtendedMetadataPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import GroupInfo as _GroupInfo

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        GroupInfo as _GroupInfoPatch,
    )
except ImportError:

    class _GroupInfoPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        GroupInfo as _GroupInfoPatchExt,
    )
except ImportError:

    class _GroupInfoPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    HTTPValidationError as _HTTPValidationError,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        HTTPValidationError as _HTTPValidationErrorPatch,
    )
except ImportError:

    class _HTTPValidationErrorPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        HTTPValidationError as _HTTPValidationErrorPatchExt,
    )
except ImportError:

    class _HTTPValidationErrorPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    InitiateDeviceFlowResponse as _InitiateDeviceFlowResponse,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        InitiateDeviceFlowResponse as _InitiateDeviceFlowResponsePatch,
    )
except ImportError:

    class _InitiateDeviceFlowResponsePatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        InitiateDeviceFlowResponse as _InitiateDeviceFlowResponsePatchExt,
    )
except ImportError:

    class _InitiateDeviceFlowResponsePatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import InsertedJob as _InsertedJob

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        InsertedJob as _InsertedJobPatch,
    )
except ImportError:

    class _InsertedJobPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        InsertedJob as _InsertedJobPatchExt,
    )
except ImportError:

    class _InsertedJobPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import JobSearchParams as _JobSearchParams

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobSearchParams as _JobSearchParamsPatch,
    )
except ImportError:

    class _JobSearchParamsPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        JobSearchParams as _JobSearchParamsPatchExt,
    )
except ImportError:

    class _JobSearchParamsPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    JobSearchParamsSearchItem as _JobSearchParamsSearchItem,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobSearchParamsSearchItem as _JobSearchParamsSearchItemPatch,
    )
except ImportError:

    class _JobSearchParamsSearchItemPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        JobSearchParamsSearchItem as _JobSearchParamsSearchItemPatchExt,
    )
except ImportError:

    class _JobSearchParamsSearchItemPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import JobStatusUpdate as _JobStatusUpdate

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobStatusUpdate as _JobStatusUpdatePatch,
    )
except ImportError:

    class _JobStatusUpdatePatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        JobStatusUpdate as _JobStatusUpdatePatchExt,
    )
except ImportError:

    class _JobStatusUpdatePatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    JobSummaryParams as _JobSummaryParams,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobSummaryParams as _JobSummaryParamsPatch,
    )
except ImportError:

    class _JobSummaryParamsPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        JobSummaryParams as _JobSummaryParamsPatchExt,
    )
except ImportError:

    class _JobSummaryParamsPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    JobSummaryParamsSearchItem as _JobSummaryParamsSearchItem,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        JobSummaryParamsSearchItem as _JobSummaryParamsSearchItemPatch,
    )
except ImportError:

    class _JobSummaryParamsSearchItemPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        JobSummaryParamsSearchItem as _JobSummaryParamsSearchItemPatchExt,
    )
except ImportError:

    class _JobSummaryParamsSearchItemPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    OpenIDConfiguration as _OpenIDConfiguration,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        OpenIDConfiguration as _OpenIDConfigurationPatch,
    )
except ImportError:

    class _OpenIDConfigurationPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        OpenIDConfiguration as _OpenIDConfigurationPatchExt,
    )
except ImportError:

    class _OpenIDConfigurationPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    SandboxDownloadResponse as _SandboxDownloadResponse,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SandboxDownloadResponse as _SandboxDownloadResponsePatch,
    )
except ImportError:

    class _SandboxDownloadResponsePatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        SandboxDownloadResponse as _SandboxDownloadResponsePatchExt,
    )
except ImportError:

    class _SandboxDownloadResponsePatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import SandboxInfo as _SandboxInfo

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SandboxInfo as _SandboxInfoPatch,
    )
except ImportError:

    class _SandboxInfoPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        SandboxInfo as _SandboxInfoPatchExt,
    )
except ImportError:

    class _SandboxInfoPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    SandboxUploadResponse as _SandboxUploadResponse,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SandboxUploadResponse as _SandboxUploadResponsePatch,
    )
except ImportError:

    class _SandboxUploadResponsePatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        SandboxUploadResponse as _SandboxUploadResponsePatchExt,
    )
except ImportError:

    class _SandboxUploadResponsePatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    ScalarSearchSpec as _ScalarSearchSpec,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        ScalarSearchSpec as _ScalarSearchSpecPatch,
    )
except ImportError:

    class _ScalarSearchSpecPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        ScalarSearchSpec as _ScalarSearchSpecPatchExt,
    )
except ImportError:

    class _ScalarSearchSpecPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    ScalarSearchSpecValue as _ScalarSearchSpecValue,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        ScalarSearchSpecValue as _ScalarSearchSpecValuePatch,
    )
except ImportError:

    class _ScalarSearchSpecValuePatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        ScalarSearchSpecValue as _ScalarSearchSpecValuePatchExt,
    )
except ImportError:

    class _ScalarSearchSpecValuePatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    SetJobStatusReturn as _SetJobStatusReturn,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SetJobStatusReturn as _SetJobStatusReturnPatch,
    )
except ImportError:

    class _SetJobStatusReturnPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        SetJobStatusReturn as _SetJobStatusReturnPatchExt,
    )
except ImportError:

    class _SetJobStatusReturnPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    SetJobStatusReturnSuccess as _SetJobStatusReturnSuccess,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SetJobStatusReturnSuccess as _SetJobStatusReturnSuccessPatch,
    )
except ImportError:

    class _SetJobStatusReturnSuccessPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        SetJobStatusReturnSuccess as _SetJobStatusReturnSuccessPatchExt,
    )
except ImportError:

    class _SetJobStatusReturnSuccessPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import SortSpec as _SortSpec

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SortSpec as _SortSpecPatch,
    )
except ImportError:

    class _SortSpecPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        SortSpec as _SortSpecPatchExt,
    )
except ImportError:

    class _SortSpecPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import SupportInfo as _SupportInfo

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        SupportInfo as _SupportInfoPatch,
    )
except ImportError:

    class _SupportInfoPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        SupportInfo as _SupportInfoPatchExt,
    )
except ImportError:

    class _SupportInfoPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import TokenResponse as _TokenResponse

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        TokenResponse as _TokenResponsePatch,
    )
except ImportError:

    class _TokenResponsePatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        TokenResponse as _TokenResponsePatchExt,
    )
except ImportError:

    class _TokenResponsePatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    UserInfoResponse as _UserInfoResponse,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        UserInfoResponse as _UserInfoResponsePatch,
    )
except ImportError:

    class _UserInfoResponsePatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        UserInfoResponse as _UserInfoResponsePatchExt,
    )
except ImportError:

    class _UserInfoResponsePatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import VOInfo as _VOInfo

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        VOInfo as _VOInfoPatch,
    )
except ImportError:

    class _VOInfoPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        VOInfo as _VOInfoPatchExt,
    )
except ImportError:

    class _VOInfoPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import ValidationError as _ValidationError

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        ValidationError as _ValidationErrorPatch,
    )
except ImportError:

    class _ValidationErrorPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        ValidationError as _ValidationErrorPatchExt,
    )
except ImportError:

    class _ValidationErrorPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    ValidationErrorLocItem as _ValidationErrorLocItem,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        ValidationErrorLocItem as _ValidationErrorLocItemPatch,
    )
except ImportError:

    class _ValidationErrorLocItemPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        ValidationErrorLocItem as _ValidationErrorLocItemPatchExt,
    )
except ImportError:

    class _ValidationErrorLocItemPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    VectorSearchSpec as _VectorSearchSpec,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        VectorSearchSpec as _VectorSearchSpecPatch,
    )
except ImportError:

    class _VectorSearchSpecPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        VectorSearchSpec as _VectorSearchSpecPatchExt,
    )
except ImportError:

    class _VectorSearchSpecPatchExt:  # type: ignore[no-redef]
        pass


from gubbins.client._generated.models._models import (
    VectorSearchSpecValues as _VectorSearchSpecValues,
)

try:
    from diracx.client._patches.models import (  # type: ignore[attr-defined]
        VectorSearchSpecValues as _VectorSearchSpecValuesPatch,
    )
except ImportError:

    class _VectorSearchSpecValuesPatch:  # type: ignore[no-redef]
        pass


try:
    from gubbins.client._patches.models import (  # type: ignore[attr-defined]
        VectorSearchSpecValues as _VectorSearchSpecValuesPatchExt,
    )
except ImportError:

    class _VectorSearchSpecValuesPatchExt:  # type: ignore[no-redef]
        pass


class BodyAuthGetOidcToken(
    _BodyAuthGetOidcTokenPatchExt, _BodyAuthGetOidcTokenPatch, _BodyAuthGetOidcToken
):
    pass


class BodyAuthGetOidcTokenGrantType(
    _BodyAuthGetOidcTokenGrantTypePatchExt,
    _BodyAuthGetOidcTokenGrantTypePatch,
    _BodyAuthGetOidcTokenGrantType,
):
    pass


class ExtendedMetadata(
    _ExtendedMetadataPatchExt, _ExtendedMetadataPatch, _ExtendedMetadata
):
    pass


class GroupInfo(_GroupInfoPatchExt, _GroupInfoPatch, _GroupInfo):
    pass


class HTTPValidationError(
    _HTTPValidationErrorPatchExt, _HTTPValidationErrorPatch, _HTTPValidationError
):
    pass


class InitiateDeviceFlowResponse(
    _InitiateDeviceFlowResponsePatchExt,
    _InitiateDeviceFlowResponsePatch,
    _InitiateDeviceFlowResponse,
):
    pass


class InsertedJob(_InsertedJobPatchExt, _InsertedJobPatch, _InsertedJob):
    pass


class JobSearchParams(
    _JobSearchParamsPatchExt, _JobSearchParamsPatch, _JobSearchParams
):
    pass


class JobSearchParamsSearchItem(
    _JobSearchParamsSearchItemPatchExt,
    _JobSearchParamsSearchItemPatch,
    _JobSearchParamsSearchItem,
):
    pass


class JobStatusUpdate(
    _JobStatusUpdatePatchExt, _JobStatusUpdatePatch, _JobStatusUpdate
):
    pass


class JobSummaryParams(
    _JobSummaryParamsPatchExt, _JobSummaryParamsPatch, _JobSummaryParams
):
    pass


class JobSummaryParamsSearchItem(
    _JobSummaryParamsSearchItemPatchExt,
    _JobSummaryParamsSearchItemPatch,
    _JobSummaryParamsSearchItem,
):
    pass


class OpenIDConfiguration(
    _OpenIDConfigurationPatchExt, _OpenIDConfigurationPatch, _OpenIDConfiguration
):
    pass


class SandboxDownloadResponse(
    _SandboxDownloadResponsePatchExt,
    _SandboxDownloadResponsePatch,
    _SandboxDownloadResponse,
):
    pass


class SandboxInfo(_SandboxInfoPatchExt, _SandboxInfoPatch, _SandboxInfo):
    pass


class SandboxUploadResponse(
    _SandboxUploadResponsePatchExt, _SandboxUploadResponsePatch, _SandboxUploadResponse
):
    pass


class ScalarSearchSpec(
    _ScalarSearchSpecPatchExt, _ScalarSearchSpecPatch, _ScalarSearchSpec
):
    pass


class ScalarSearchSpecValue(
    _ScalarSearchSpecValuePatchExt, _ScalarSearchSpecValuePatch, _ScalarSearchSpecValue
):
    pass


class SetJobStatusReturn(
    _SetJobStatusReturnPatchExt, _SetJobStatusReturnPatch, _SetJobStatusReturn
):
    pass


class SetJobStatusReturnSuccess(
    _SetJobStatusReturnSuccessPatchExt,
    _SetJobStatusReturnSuccessPatch,
    _SetJobStatusReturnSuccess,
):
    pass


class SortSpec(_SortSpecPatchExt, _SortSpecPatch, _SortSpec):
    pass


class SupportInfo(_SupportInfoPatchExt, _SupportInfoPatch, _SupportInfo):
    pass


class TokenResponse(_TokenResponsePatchExt, _TokenResponsePatch, _TokenResponse):
    pass


class UserInfoResponse(
    _UserInfoResponsePatchExt, _UserInfoResponsePatch, _UserInfoResponse
):
    pass


class VOInfo(_VOInfoPatchExt, _VOInfoPatch, _VOInfo):
    pass


class ValidationError(
    _ValidationErrorPatchExt, _ValidationErrorPatch, _ValidationError
):
    pass


class ValidationErrorLocItem(
    _ValidationErrorLocItemPatchExt,
    _ValidationErrorLocItemPatch,
    _ValidationErrorLocItem,
):
    pass


class VectorSearchSpec(
    _VectorSearchSpecPatchExt, _VectorSearchSpecPatch, _VectorSearchSpec
):
    pass


class VectorSearchSpecValues(
    _VectorSearchSpecValuesPatchExt,
    _VectorSearchSpecValuesPatch,
    _VectorSearchSpecValues,
):
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
