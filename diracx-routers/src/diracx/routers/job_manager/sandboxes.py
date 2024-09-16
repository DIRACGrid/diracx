from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from http import HTTPStatus
from typing import TYPE_CHECKING, Annotated, Literal

from aiobotocore.session import get_session
from botocore.config import Config
from botocore.errorfactory import ClientError
from fastapi import Body, Depends, HTTPException, Query
from pydantic import BaseModel, PrivateAttr
from pydantic_settings import SettingsConfigDict
from pyparsing import Any
from sqlalchemy.exc import NoResultFound

from diracx.core.models import (
    SandboxInfo,
    SandboxType,
)
from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.core.s3 import (
    generate_presigned_upload,
    s3_bucket_exists,
    s3_object_exists,
)
from diracx.core.settings import ServiceSettingsBase

from ..utils.users import AuthorizedUserInfo, verify_dirac_access_token
from .access_policies import (
    ActionType,
    CheckSandboxPolicyCallable,
    CheckWMSPolicyCallable,
)

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

from ..auth import has_properties
from ..dependencies import JobDB, SandboxMetadataDB, add_settings_annotation
from ..fastapi_classes import DiracxRouter

MAX_SANDBOX_SIZE_BYTES = 100 * 1024 * 1024
router = DiracxRouter(dependencies=[has_properties(NORMAL_USER | JOB_ADMINISTRATOR)])


@add_settings_annotation
class SandboxStoreSettings(ServiceSettingsBase):
    """Settings for the sandbox store."""

    model_config = SettingsConfigDict(env_prefix="DIRACX_SANDBOX_STORE_")

    bucket_name: str
    s3_client_kwargs: dict[str, str]
    auto_create_bucket: bool = False
    url_validity_seconds: int = 5 * 60
    se_name: str = "SandboxSE"
    _client: S3Client = PrivateAttr(None)

    @contextlib.asynccontextmanager
    async def lifetime_function(self) -> AsyncIterator[None]:
        async with get_session().create_client(
            "s3",
            **self.s3_client_kwargs,
            config=Config(signature_version="v4"),
        ) as self._client:  # type: ignore
            if not await s3_bucket_exists(self._client, self.bucket_name):
                if not self.auto_create_bucket:
                    raise ValueError(
                        f"Bucket {self.bucket_name} does not exist and auto_create_bucket is disabled"
                    )
                try:
                    await self._client.create_bucket(Bucket=self.bucket_name)
                except ClientError as e:
                    raise ValueError(
                        f"Failed to create bucket {self.bucket_name}"
                    ) from e

            yield

    @property
    def s3_client(self) -> S3Client:
        if self._client is None:
            raise RuntimeError("S3 client accessed before lifetime function")
        return self._client


class SandboxUploadResponse(BaseModel):
    pfn: str
    url: str | None = None
    fields: dict[str, str] = {}


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
    pfn = sandbox_metadata_db.get_pfn(settings.bucket_name, user_info, sandbox_info)
    full_pfn = f"SB:{settings.se_name}|{pfn}"
    await check_permissions(
        action=ActionType.CREATE, sandbox_metadata_db=sandbox_metadata_db, pfns=[pfn]
    )

    # TODO: THis test should come first, but if we do
    # the access policy will crash for not having been called
    # so we need to find a way to ackownledge that

    if sandbox_info.size > MAX_SANDBOX_SIZE_BYTES:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Sandbox too large. Max size is {MAX_SANDBOX_SIZE_BYTES} bytes",
        )

    try:
        exists_and_assigned = await sandbox_metadata_db.sandbox_is_assigned(
            pfn, settings.se_name
        )
    except NoResultFound:
        # The sandbox doesn't exist in the database
        pass
    else:
        # As sandboxes are registered in the DB before uploading to the storage
        # backend we can't rely on their existence in the database to determine if
        # they have been uploaded. Instead we check if the sandbox has been
        # assigned to a job. If it has then we know it has been uploaded and we
        # can avoid communicating with the storage backend.
        if exists_and_assigned or s3_object_exists(
            settings.s3_client, settings.bucket_name, pfn_to_key(pfn)
        ):
            await sandbox_metadata_db.update_sandbox_last_access_time(
                settings.se_name, pfn
            )
            return SandboxUploadResponse(pfn=full_pfn)

    upload_info = await generate_presigned_upload(
        settings.s3_client,
        settings.bucket_name,
        pfn_to_key(pfn),
        sandbox_info.checksum_algorithm,
        sandbox_info.checksum,
        sandbox_info.size,
        settings.url_validity_seconds,
    )
    await sandbox_metadata_db.insert_sandbox(
        settings.se_name, user_info, pfn, sandbox_info.size
    )

    return SandboxUploadResponse(**upload_info, pfn=full_pfn)


class SandboxDownloadResponse(BaseModel):
    url: str
    expires_in: int


def pfn_to_key(pfn: str) -> str:
    """Convert a PFN to a key for S3.

    This removes the leading "/S3/<bucket_name>" from the PFN.
    """
    return "/".join(pfn.split("/")[3:])


SANDBOX_PFN_REGEX = (
    # Starts with /S3/<bucket_name> or /SB:<se_name>|/S3/<bucket_name>
    r"^(:?SB:[A-Za-z]+\|)?/S3/[a-z0-9\.\-]{3,63}"
    # Followed /<vo>/<group>/<username>/<checksum_algorithm>:<checksum>.<format>
    r"(?:/[^/]+){3}/[a-z0-9]{3,10}:[0-9a-f]{64}\.[a-z0-9\.]+$"
)


@router.get("/sandbox")
async def get_sandbox_file(
    pfn: Annotated[str, Query(max_length=256, pattern=SANDBOX_PFN_REGEX)],
    settings: SandboxStoreSettings,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
    sandbox_metadata_db: SandboxMetadataDB,
    check_permissions: CheckSandboxPolicyCallable,
) -> SandboxDownloadResponse:
    """Get a presigned URL to download a sandbox file.

    This route cannot use a redirect response most clients will also send the
    authorization header when following a redirect. This is not desirable as
    it would leak the authorization token to the storage backend. Additionally,
    most storage backends return an error when they receive an authorization
    header for a presigned URL.
    """
    pfn = pfn.split("|", 1)[-1]
    required_prefix = (
        "/"
        + f"S3/{settings.bucket_name}/{user_info.vo}/{user_info.dirac_group}/{user_info.preferred_username}"
        + "/"
    )
    await check_permissions(
        action=ActionType.READ,
        sandbox_metadata_db=sandbox_metadata_db,
        pfns=[pfn],
        required_prefix=required_prefix,
    )

    # TODO: Support by name and by job id?
    presigned_url = await settings.s3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": settings.bucket_name, "Key": pfn_to_key(pfn)},
        ExpiresIn=settings.url_validity_seconds,
    )
    return SandboxDownloadResponse(
        url=presigned_url, expires_in=settings.url_validity_seconds
    )


@router.get("/{job_id}/sandbox")
async def get_job_sandboxes(
    job_id: int,
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
) -> dict[str, list[Any]]:
    """Get input and output sandboxes of given job."""
    await check_permissions(action=ActionType.READ, job_db=job_db, job_ids=[job_id])

    input_sb = await sandbox_metadata_db.get_sandbox_assigned_to_job(
        job_id, SandboxType.Input
    )
    output_sb = await sandbox_metadata_db.get_sandbox_assigned_to_job(
        job_id, SandboxType.Output
    )
    return {SandboxType.Input: input_sb, SandboxType.Output: output_sb}


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
    job_sb_pfns = await sandbox_metadata_db.get_sandbox_assigned_to_job(
        job_id, SandboxType(sandbox_type.capitalize())
    )

    return job_sb_pfns


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
    short_pfn = pfn.split("|", 1)[-1]
    await sandbox_metadata_db.assign_sandbox_to_jobs(
        jobs_ids=[job_id],
        pfn=short_pfn,
        sb_type=SandboxType.Output,
        se_name=settings.se_name,
    )


@router.delete("/{job_id}/sandbox")
async def unassign_job_sandboxes(
    job_id: int,
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
):
    """Delete single job sandbox mapping."""
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=[job_id])
    await sandbox_metadata_db.unassign_sandboxes_to_jobs([job_id])


@router.delete("/sandbox")
async def unassign_bulk_jobs_sandboxes(
    jobs_ids: Annotated[list[int], Query()],
    sandbox_metadata_db: SandboxMetadataDB,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
):
    """Delete bulk jobs sandbox mapping."""
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=jobs_ids)
    await sandbox_metadata_db.unassign_sandboxes_to_jobs(jobs_ids)
