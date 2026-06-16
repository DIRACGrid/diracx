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
    """Initiate sandbox upload or retrieve a PFN of an existing sandbox.

    If the sandbox metadata already exists, this returns the existing PFN.
    If the sandbox is not present, the response contains presigned upload
    information (``url`` and ``fields``) that the client should use to
    upload the sandbox to the configured storage backend.

    Args:
        user_info (AuthorizedUserInfo): Authenticated user information.
        sandbox_info (SandboxInfo): Metadata describing the sandbox to upload.
        sandbox_metadata_db (SandboxMetadataDB): Database access for sandbox
            metadata.
        settings (SandboxStoreSettings): Storage backend configuration.
        check_permissions (CheckSandboxPolicyCallable): Callable to verify
            that the requesting user has permission to create sandboxes.

    Returns:
        SandboxUploadResponse: Contains the sandbox PFN and, when an upload is
            required, the presigned ``url`` and form ``fields`` to perform the
            upload.

    Raises:
        HTTPException: If the provided input is invalid or the operation
            cannot be performed (e.g. as a result of validation in the
            business logic layer).

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
    """Return a presigned download URL for a sandbox file.

    This endpoint validates access to the requested PFN and returns a
    presigned URL suitable for client downloads. It does not redirect to the
    storage backend because many clients include the Authorization header when
    following redirects, which would leak credentials to the storage service.

    Additionally, when the sandbox already exists the response will not
    include upload fields; clients should use the returned PFN.

    Args:
        pfn (str): The full or short PFN of the sandbox file. Must match
            ``SANDBOX_PFN_REGEX``.
        settings (SandboxStoreSettings): Storage backend configuration.
        sandbox_metadata_db (SandboxMetadataDB): Database access for sandbox
            metadata.
        user_info (AuthorizedUserInfo): Authenticated user information.
        check_permissions (CheckSandboxPolicyCallable): Callable used to
            verify read access to the requested PFN.

    Returns:
        SandboxDownloadResponse: Contains the presigned download URL and any
            associated metadata required by the client.

    Raises:
        HTTPException: If permission checks fail or the requested sandbox
            cannot be accessed.

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
    """Retrieve input and output sandboxes for a job.

    Args:
        job_id (int): Job identifier.
        sandbox_metadata_db (SandboxMetadataDB): Database access for sandbox
            metadata.
        job_db (JobDB): Job database access object, used for permission checks.
        check_permissions (CheckWMSPolicyCallable): Callable to verify that the
            caller may read the job's data.

    Returns:
        dict[str, list[Any]]: A dictionary with keys (e.g. "input", "output")
            mapping to lists of sandbox entries associated with the job.

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
    """Retrieve either input or output sandbox entries for a job.

    Args:
        job_id (int): Job identifier.
        sandbox_metadata_db (SandboxMetadataDB): Database access for sandbox
            metadata.
        job_db (JobDB): Job database access object, used for permission checks.
        sandbox_type (Literal["input", "output"]): Which sandbox type to
            retrieve.
        check_permissions (CheckWMSPolicyCallable): Callable to verify that the
            caller may read the job's data.

    Returns:
        list[Any]: List of sandbox entries for the specified type.

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
    """Assign a sandbox PFN as the job's output sandbox.

    Maps the provided PFN to the job as an output sandbox. Permission checks
    are performed before attempting the assignment.

    Args:
        job_id (int): Identifier of the job to update.
        pfn (str): PFN to assign as the job's output sandbox. Must match
            ``SANDBOX_PFN_REGEX``.
        sandbox_metadata_db (SandboxMetadataDB): Database access for sandbox
            metadata.
        job_db (JobDB): Job database access object, used for permission checks.
        settings (SandboxStoreSettings): Storage backend configuration.
        check_permissions (CheckWMSPolicyCallable): Callable to verify that the
            caller may manage the job.

    Raises:
        HTTPException: If the sandbox does not exist, is already assigned, or
            the caller lacks permission to perform the assignment.

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
    """Remove sandbox mapping(s) for a single job.

    Args:
        job_id (int): Identifier of the job whose sandbox mappings will be
            removed.
        sandbox_metadata_db (SandboxMetadataDB): Database access for sandbox
            metadata.
        job_db (JobDB): Job database access object, used for permission checks.
        check_permissions (CheckWMSPolicyCallable): Callable to verify that the
            caller may manage the job.

    Returns:
        None

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
    """Remove sandbox mappings for multiple jobs in bulk.

    The request body should be a JSON object containing a top-level
    ``job_ids`` array. OpenAPI examples are provided in the module-level
    ``EXAMPLE_UNASSIGN`` constant and are exposed via the route's
    ``openapi_extra`` metadata.

    Args:
        job_ids (list[int]): List of job identifiers to unassign sandboxes
            for. This parameter is provided in the request body as a top-level
            ``job_ids`` key.
        sandbox_metadata_db (SandboxMetadataDB): Database access for sandbox
            metadata.
        job_db (JobDB): Job database access object, used for permission checks.
        check_permissions (CheckWMSPolicyCallable): Callable to verify that the
            caller may manage the specified jobs.

    Returns:
        None

    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)
    await unassign_jobs_sandboxes_bl(job_ids, sandbox_metadata_db)
