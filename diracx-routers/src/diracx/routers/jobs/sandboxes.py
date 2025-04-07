from __future__ import annotations

from http import HTTPStatus
from typing import Annotated, Literal

from fastapi import Body, Depends, HTTPException, Query
from pyparsing import Any

from diracx.core.exceptions import SandboxAlreadyAssignedError, SandboxNotFoundError
from diracx.core.models import (
    SandboxDownloadResponse,
    SandboxInfo,
    SandboxUploadResponse,
)
from diracx.logic.jobs.sandboxes import SANDBOX_PFN_REGEX
from diracx.logic.jobs.sandboxes import (
    assign_sandbox_to_job as assign_sandbox_to_job_bl,
)
from diracx.logic.jobs.sandboxes import get_job_sandbox as get_job_sandbox_bl
from diracx.logic.jobs.sandboxes import get_job_sandboxes as get_job_sandboxes_bl
from diracx.logic.jobs.sandboxes import get_sandbox_file as get_sandbox_file_bl
from diracx.logic.jobs.sandboxes import (
    initiate_sandbox_upload as initiate_sandbox_upload_bl,
)
from diracx.logic.jobs.sandboxes import (
    unassign_jobs_sandboxes as unassign_jobs_sandboxes_bl,
)

from ..dependencies import JobDB, SandboxMetadataDB, SandboxStoreSettings
from ..fastapi_classes import DiracxRouter
from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import (
    ActionType,
    CheckSandboxPolicyCallable,
    CheckWMSPolicyCallable,
)

MAX_SANDBOX_SIZE_BYTES = 100 * 1024 * 1024
router = DiracxRouter()


@router.post("/sandbox")
async def initiate_sandbox_upload(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    sandbox_info: SandboxInfo,
    sandbox_metadata_db: SandboxMetadataDB,
    settings: SandboxStoreSettings,
    check_permissions: CheckSandboxPolicyCallable,
) -> SandboxUploadResponse:
    """Get the PFN for the given sandbox, initiate an upload as required.

    If the sandbox already exists in the database then the PFN is returned
    and there is no "url" field in the response.

    If the sandbox does not exist in the database then the "url" and "fields"
    should be used to upload the sandbox to the storage backend.
    """
    await check_permissions(
        action=ActionType.CREATE, sandbox_metadata_db=sandbox_metadata_db
    )

    try:
        sandbox_upload_response = await initiate_sandbox_upload_bl(
            user_info, sandbox_info, sandbox_metadata_db, settings
        )
    except ValueError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=str(e),
        ) from e
    return sandbox_upload_response


@router.get("/sandbox")
async def get_sandbox_file(
    pfn: Annotated[str, Query(max_length=256, pattern=SANDBOX_PFN_REGEX)],
    settings: SandboxStoreSettings,
    sandbox_metadata_db: SandboxMetadataDB,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    check_permissions: CheckSandboxPolicyCallable,
) -> SandboxDownloadResponse:
    """Get a presigned URL to download a sandbox file.

    This route cannot use a redirect response most clients will also send the
    authorization header when following a redirect. This is not desirable as
    it would leak the authorization token to the storage backend. Additionally,
    most storage backends return an error when they receive an authorization
    header for a presigned URL.
    """
    short_pfn = pfn.split("|", 1)[-1]
    required_prefix = (
        "/"
        + f"S3/{settings.bucket_name}/{user_info.vo}/{user_info.dirac_group}/{user_info.preferred_username}"
        + "/"
    )
    await check_permissions(
        action=ActionType.READ,
        sandbox_metadata_db=sandbox_metadata_db,
        pfns=[short_pfn],
        required_prefix=required_prefix,
        se_name=settings.se_name,
    )

    return await get_sandbox_file_bl(pfn, settings)


@router.get("/{job_id}/sandbox")
async def get_job_sandboxes(
    job_id: int,
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
) -> dict[str, list[Any]]:
    """Get input and output sandboxes of given job."""
    await check_permissions(action=ActionType.READ, job_db=job_db, job_ids=[job_id])
    return await get_job_sandboxes_bl(job_id, sandbox_metadata_db)


@router.get("/{job_id}/sandbox/{sandbox_type}")
async def get_job_sandbox(
    job_id: int,
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    sandbox_type: Literal["input", "output"],
    check_permissions: CheckWMSPolicyCallable,
) -> list[Any]:
    """Get input or output sandbox of given job."""
    await check_permissions(action=ActionType.READ, job_db=job_db, job_ids=[job_id])
    return await get_job_sandbox_bl(job_id, sandbox_metadata_db, sandbox_type)


@router.patch("/{job_id}/sandbox/output")
async def assign_sandbox_to_job(
    job_id: int,
    pfn: Annotated[str, Body(max_length=256, pattern=SANDBOX_PFN_REGEX)],
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    settings: SandboxStoreSettings,
    check_permissions: CheckWMSPolicyCallable,
):
    """Map the pfn as output sandbox to job."""
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])
    try:
        await assign_sandbox_to_job_bl(job_id, pfn, sandbox_metadata_db, settings)
    except SandboxNotFoundError as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail="Sandbox not found"
        ) from e
    except (SandboxAlreadyAssignedError, AssertionError) as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail="Sandbox already assigned"
        ) from e


@router.delete("/{job_id}/sandbox")
async def unassign_job_sandboxes(
    job_id: int,
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
):
    """Delete single job sandbox mapping."""
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])
    await unassign_jobs_sandboxes_bl([job_id], sandbox_metadata_db)


@router.delete("/sandbox")
async def unassign_bulk_jobs_sandboxes(
    jobs_ids: Annotated[list[int], Query()],
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
):
    """Delete bulk jobs sandbox mapping."""
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=jobs_ids)
    await unassign_jobs_sandboxes_bl(jobs_ids, sandbox_metadata_db)
