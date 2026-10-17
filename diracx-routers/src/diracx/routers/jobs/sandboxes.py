"""Expose endpoints for uploading, downloading, and assigning job sandboxes.

Sandbox ownership and job access policies are checked before operations are
performed against the sandbox metadata database and storage backend.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Annotated, Any, Literal

from fastapi import Body, Depends, HTTPException, Query

from diracx.core.exceptions import SandboxAlreadyAssignedError, SandboxNotFoundError
from diracx.core.models import (
    SandboxDownloadResponse,
    SandboxInfo,
    SandboxUploadResponse,
)
from diracx.core.settings import SandboxStoreSettings
from diracx.db.sql import JobDB, SandboxMetadataDB
from diracx.logic.jobs import SANDBOX_PFN_REGEX
from diracx.logic.jobs import (
    assign_sandbox_to_job as assign_sandbox_to_job_bl,
)
from diracx.logic.jobs import get_job_sandbox as get_job_sandbox_bl
from diracx.logic.jobs import get_job_sandboxes as get_job_sandboxes_bl
from diracx.logic.jobs import get_sandbox_file as get_sandbox_file_bl
from diracx.logic.jobs import (
    initiate_sandbox_upload as initiate_sandbox_upload_bl,
)
from diracx.logic.jobs import (
    unassign_jobs_sandboxes as unassign_jobs_sandboxes_bl,
)

from ..fastapi_classes import DiracxRouter
from ..utils import AuthorizedUserInfo, verify_dirac_access_token
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
    """Return a sandbox PFN and initiate an upload when needed.

    If the sandbox already exists, the response contains its PFN and has no
    ``url`` field. Otherwise, the ``url`` and ``fields`` in the response must
    be used to upload the sandbox to the storage backend.

    \f

    Args:
        user_info: Authenticated user who owns the sandbox.
        sandbox_info: Archive format, size, and checksum metadata.
        sandbox_metadata_db: Database used to register and look up the sandbox.
        settings: Storage configuration used to prepare the upload.
        check_permissions: Access-policy dependency used to authorize sandbox creation.

    Returns:
        Sandbox PFN and, when needed, presigned upload details.

    Raises:
        HTTPException: If the sandbox exceeds the size limit or metadata is
            invalid (400), or the user cannot create a sandbox (403).
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
    """Return a presigned URL for downloading a sandbox file.

    The URL is returned rather than redirected to, as most clients resend
    the authorization header when following a redirect. That would leak the
    token to the storage backend, and most storage backends reject requests
    for presigned URLs that include an authorization header.

    \f

    Args:
        pfn: Sandbox physical file name to download.
        settings: Storage configuration used to generate the presigned URL.
        sandbox_metadata_db: Database used to verify and update sandbox access.
        user_info: Authenticated user and sandbox ownership context.
        check_permissions: Access-policy dependency used to authorize the download.

    Returns:
        Presigned download URL and its expiry duration.

    Raises:
        HTTPException: If the PFN is invalid or the user is not permitted to
            download the sandbox (403).
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

    return await get_sandbox_file_bl(pfn, sandbox_metadata_db, settings)


@router.get("/{job_id}/sandbox")
async def get_job_sandboxes(
    job_id: int,
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
) -> dict[str, list[Any]]:
    """Return the input and output sandboxes assigned to a job.

    \f

    Args:
        job_id: Identifier of the job whose sandboxes are requested.
        sandbox_metadata_db: Database containing job-to-sandbox mappings.
        job_db: Database used to check job access permissions.
        check_permissions: Access-policy dependency used to authorize the read.

    Returns:
        A mapping from input and output sandbox types to their sandbox records.

    Raises:
        HTTPException: If the user is not permitted to read the job (403).
    """
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
    """Return one type of sandbox assigned to a job.

    \f

    Args:
        job_id: Identifier of the job whose sandbox is requested.
        sandbox_metadata_db: Database containing job-to-sandbox mappings.
        job_db: Database used to check job access permissions.
        sandbox_type: Whether to retrieve the input or output sandbox.
        check_permissions: Access-policy dependency used to authorize the read.

    Returns:
        Sandbox records of the requested type assigned to the job.

    Raises:
        HTTPException: If the user is not permitted to read the job (403).
    """
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
    """Assign a sandbox as the output sandbox for a job.

    \f

    Args:
        job_id: Identifier of the job to receive the sandbox.
        pfn: Physical file name of the sandbox to assign.
        sandbox_metadata_db: Database containing sandbox and job mappings.
        job_db: Database used to check job management permissions.
        settings: Storage settings used to identify the storage element.
        check_permissions: Access-policy dependency used to authorize the update.

    Raises:
        HTTPException: If the sandbox is missing or already assigned (400),
            or the user is not permitted to manage the job (403).
    """
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
    """Remove all sandbox mappings for a job.

    \f

    Args:
        job_id: Identifier of the job whose sandbox mappings are removed.
        sandbox_metadata_db: Database containing job-to-sandbox mappings.
        job_db: Database used to check job management permissions.
        check_permissions: Access-policy dependency used to authorize the update.

    Raises:
        HTTPException: If the user is not permitted to manage the job (403).
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])
    await unassign_jobs_sandboxes_bl([job_id], sandbox_metadata_db)


EXAMPLE_UNASSIGN = {
    "Default": {
        "value": {"job_ids": [1, 2, 3]},
    },
    "One job": {
        "value": {"job_ids": [1]},
    },
}


@router.post(
    "/sandbox/unassign",
    status_code=HTTPStatus.NO_CONTENT,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"examples": EXAMPLE_UNASSIGN}},
        }
    },
)
async def unassign_bulk_jobs_sandboxes(
    job_ids: Annotated[list[int], Body(embed=True)],
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
):
    """Remove sandbox mappings for multiple jobs.

    \f

    Args:
        job_ids: Identifiers of the jobs whose sandbox mappings are removed.
        sandbox_metadata_db: Database containing job-to-sandbox mappings.
        job_db: Database used to check job management permissions.
        check_permissions: Access-policy dependency used to authorize the update.

    Raises:
        HTTPException: If the user is not permitted to manage any requested
            job (403).
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)
    await unassign_jobs_sandboxes_bl(job_ids, sandbox_metadata_db)
