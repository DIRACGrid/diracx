from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import NoResultFound

from diracx.core.models import (
    SandboxInfo,
)
from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.core.s3 import (
    PRESIGNED_URL_TIMEOUT,
    generate_presigned_upload,
    hack_get_s3_client,
    s3_object_exists,
)

from ..auth import AuthorizedUserInfo, has_properties, verify_dirac_access_token
from ..dependencies import SandboxMetadataDB
from ..fastapi_classes import DiracxRouter

MAX_SANDBOX_SIZE_BYTES = 100 * 1024 * 1024
router = DiracxRouter(dependencies=[has_properties(NORMAL_USER | JOB_ADMINISTRATOR)])


class SandboxUploadResponse(BaseModel):
    pfn: str
    url: str | None = None
    fields: dict[str, str] = {}


@router.post("/sandbox")
async def initiate_sandbox_upload(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    sandbox_info: SandboxInfo,
    sandbox_metadata_db: SandboxMetadataDB,
) -> SandboxUploadResponse:
    """Get the PFN for the given sandbox, initiate an upload as required.

    If the sandbox already exists in the database then the PFN is returned
    and there is no "url" field in the response.

    If the sandbox does not exist in the database then the "url" and "fields"
    should be used to upload the sandbox to the storage backend.
    """
    if sandbox_info.size > MAX_SANDBOX_SIZE_BYTES:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Sandbox too large. Max size is {MAX_SANDBOX_SIZE_BYTES} bytes",
        )

    s3, bucket_name = hack_get_s3_client()

    pfn = sandbox_metadata_db.get_pfn(bucket_name, user_info, sandbox_info)

    try:
        exists_and_assigned = await sandbox_metadata_db.sandbox_is_assigned(pfn)
    except NoResultFound:
        # The sandbox doesn't exist in the database
        pass
    else:
        # As sandboxes are registered in the DB before uploading to the storage
        # backend we can't on their existence in the database to determine if
        # they have been uploaded. Instead we check if the sandbox has been
        # assigned to a job. If it has then we know it has been uploaded and we
        # can avoid communicating with the storage backend.
        if exists_and_assigned or s3_object_exists(s3, bucket_name, pfn):
            await sandbox_metadata_db.update_sandbox_last_access_time(pfn)
            return SandboxUploadResponse(pfn=pfn)

    upload_info = generate_presigned_upload(
        s3,
        bucket_name,
        pfn,
        sandbox_info.checksum_algorithm,
        sandbox_info.checksum,
        sandbox_info.size,
    )
    await sandbox_metadata_db.insert_sandbox(user_info, pfn, sandbox_info.size)

    return SandboxUploadResponse(**upload_info, pfn=pfn)


class SandboxDownloadResponse(BaseModel):
    url: str
    expires_in: int


@router.get("/sandbox/{file_path:path}")
async def get_sandbox_file(file_path: str) -> SandboxDownloadResponse:
    """Get a presigned URL to download a sandbox file

    This route cannot use a redirect response most clients will also send the
    authorization header when following a redirect. This is not desirable as
    it would leak the authorization token to the storage backend. Additionally,
    most storage backends return an error when they receive an authorization
    header for a presigned URL.
    """
    # TODO: Prevent people from downloading other people's sandboxes?
    s3, bucket_name = hack_get_s3_client()
    presigned_url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket_name, "Key": file_path},
        ExpiresIn=PRESIGNED_URL_TIMEOUT,
    )
    return SandboxDownloadResponse(url=presigned_url, expires_in=PRESIGNED_URL_TIMEOUT)
