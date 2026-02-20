from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Literal

from diracx.core.exceptions import SandboxAlreadyInsertedError, SandboxNotFoundError
from diracx.core.models.auth import UserInfo
from diracx.core.models.sandbox import (
    SandboxDownloadResponse,
    SandboxInfo,
    SandboxType,
    SandboxUploadResponse,
)
from diracx.core.s3 import (
    generate_presigned_upload,
    s3_bulk_delete_with_retry,
    s3_object_exists,
)
from diracx.core.settings import SandboxStoreSettings
from diracx.db.sql.sandbox_metadata.db import SandboxMetadataDB

if TYPE_CHECKING:
    from diracx.core.s3 import S3Object

MAX_SANDBOX_SIZE_BYTES = 100 * 1024 * 1024

SANDBOX_PFN_REGEX = (
    # Starts with /S3/<bucket_name> or /SB:<se_name>|/S3/<bucket_name>
    r"^(:?SB:[A-Za-z]+\|)?/S3/[a-z0-9\.\-]{3,63}"
    # Followed /<vo>/<group>/<username>/<checksum_algorithm>:<checksum>.<format>
    r"(?:/[^/]+){3}/[a-z0-9]{3,10}:[0-9a-f]{64}\.[a-z0-9\.]+$"
)

logger = logging.getLogger(__name__)


async def initiate_sandbox_upload(
    user_info: UserInfo,
    sandbox_info: SandboxInfo,
    sandbox_metadata_db: SandboxMetadataDB,
    settings: SandboxStoreSettings,
) -> SandboxUploadResponse:
    """Get the PFN for the given sandbox, initiate an upload as required.

    If the sandbox already exists in the database then the PFN is returned
    and there is no "url" field in the response.

    If the sandbox does not exist in the database then the "url" and "fields"
    should be used to upload the sandbox to the storage backend.
    """
    pfn = sandbox_metadata_db.get_pfn(settings.bucket_name, user_info, sandbox_info)

    # TODO: This test should come first, but if we do
    # the access policy will crash for not having been called
    # so we need to find a way to acknowledge that

    if sandbox_info.size > MAX_SANDBOX_SIZE_BYTES:
        raise ValueError(
            f"Sandbox too large, maximum allowed is {MAX_SANDBOX_SIZE_BYTES} bytes"
        )
    full_pfn = f"SB:{settings.se_name}|{pfn}"

    try:
        exists_and_assigned = await sandbox_metadata_db.sandbox_is_assigned(
            pfn, settings.se_name
        )
    except SandboxNotFoundError:
        # The sandbox doesn't exist in the database
        pass
    else:
        # As sandboxes are registered in the DB before uploading to the storage
        # backend we can't rely on their existence in the database to determine if
        # they have been uploaded. Instead we check if the sandbox has been
        # assigned to a job. If it has then we know it has been uploaded and we
        # can avoid communicating with the storage backend.
        if exists_and_assigned or await s3_object_exists(
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
    await insert_sandbox(
        sandbox_metadata_db, settings.se_name, user_info, pfn, sandbox_info.size
    )

    return SandboxUploadResponse(**upload_info, pfn=full_pfn)


async def get_sandbox_file(
    pfn: str,
    sandbox_metadata_db: SandboxMetadataDB,
    settings: SandboxStoreSettings,
) -> SandboxDownloadResponse:
    """Get a presigned URL to download a sandbox file."""
    short_pfn = pfn.split("|", 1)[-1]

    await sandbox_metadata_db.update_sandbox_last_access_time(
        settings.se_name, short_pfn
    )

    # TODO: Support by name and by job id?
    presigned_url = await settings.s3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": settings.bucket_name, "Key": pfn_to_key(short_pfn)},
        ExpiresIn=settings.url_validity_seconds,
    )
    return SandboxDownloadResponse(
        url=presigned_url, expires_in=settings.url_validity_seconds
    )


async def get_job_sandboxes(
    job_id: int,
    sandbox_metadata_db: SandboxMetadataDB,
) -> dict[str, list[Any]]:
    """Get input and output sandboxes of given job."""
    input_sb = await sandbox_metadata_db.get_sandbox_assigned_to_job(
        job_id, SandboxType.Input
    )
    output_sb = await sandbox_metadata_db.get_sandbox_assigned_to_job(
        job_id, SandboxType.Output
    )
    return {SandboxType.Input: input_sb, SandboxType.Output: output_sb}


async def get_job_sandbox(
    job_id: int,
    sandbox_metadata_db: SandboxMetadataDB,
    sandbox_type: Literal["input", "output"],
) -> list[Any]:
    """Get input or output sandbox of given job."""
    return await sandbox_metadata_db.get_sandbox_assigned_to_job(
        job_id, SandboxType(sandbox_type.capitalize())
    )


async def assign_sandbox_to_job(
    job_id: int,
    pfn: str,
    sandbox_metadata_db: SandboxMetadataDB,
    settings: SandboxStoreSettings,
):
    """Map the pfn as output sandbox to job."""
    short_pfn = pfn.split("|", 1)[-1]
    await sandbox_metadata_db.assign_sandbox_to_jobs(
        jobs_ids=[job_id],
        pfn=short_pfn,
        sb_type=SandboxType.Output,
        se_name=settings.se_name,
    )


async def unassign_jobs_sandboxes(
    jobs_ids: list[int],
    sandbox_metadata_db: SandboxMetadataDB,
):
    """Delete bulk jobs sandbox mapping."""
    await sandbox_metadata_db.unassign_sandboxes_to_jobs(jobs_ids)


def pfn_to_key(pfn: str) -> str:
    """Convert a PFN to a key for S3.

    This removes the leading "/S3/<bucket_name>" from the PFN.
    """
    return "/".join(pfn.split("/")[3:])


async def insert_sandbox(
    sandbox_metadata_db: SandboxMetadataDB,
    se_name: str,
    user: UserInfo,
    pfn: str,
    size: int,
) -> None:
    """Add a new sandbox in SandboxMetadataDB."""
    # TODO: Follow https://github.com/DIRACGrid/diracx/issues/49
    owner_id = await sandbox_metadata_db.get_owner_id(user)
    if owner_id is None:
        owner_id = await sandbox_metadata_db.insert_owner(user)

    try:
        await sandbox_metadata_db.insert_sandbox(owner_id, se_name, pfn, size)
    except SandboxAlreadyInsertedError:
        await sandbox_metadata_db.update_sandbox_last_access_time(se_name, pfn)


async def clean_sandboxes(
    sandbox_metadata_db: SandboxMetadataDB,
    settings: SandboxStoreSettings,
    *,
    batch_size: int = 50000,
) -> int:
    """Delete sandboxes that are not assigned to any job.

    Each batch:
    1. Selects rows using cursor-based pagination
    2. Deletes from S3 (chunks sent concurrently)
    3. Deletes from DB

    Args:
        sandbox_metadata_db: Database connection (not yet entered).
        settings: Sandbox store settings with S3 client.
        batch_size: Number of sandboxes to process per batch.

    Returns:
        Total number of sandboxes deleted.

    """
    total_deleted = 0
    cursor = 0
    batch_num = 0
    while True:
        batch_num += 1

        # Phase 1: SELECT candidates (short transaction, no locks)
        t0 = time.monotonic()
        async with sandbox_metadata_db:
            (
                sb_ids,
                pfns,
                cursor,
            ) = await sandbox_metadata_db.select_sandboxes_for_deletion(
                se_name=settings.se_name,
                batch_size=batch_size,
                cursor=cursor,
            )
        select_duration = time.monotonic() - t0

        if not pfns:
            logger.info(
                "Batch %d: no candidates found (%.1fs)", batch_num, select_duration
            )
            break

        logger.info(
            "Batch %d: selected %d candidates (%.1fs)",
            batch_num,
            len(pfns),
            select_duration,
        )

        # Phase 2: Delete from S3 (no transaction — prevents dark data
        # since S3 is cleaned first)
        t0 = time.monotonic()
        objects: list[S3Object] = [{"Key": pfn_to_key(pfn)} for pfn in pfns]
        failed_keys = await s3_bulk_delete_with_retry(
            settings.s3_client, settings.bucket_name, objects
        )
        s3_duration = time.monotonic() - t0

        if failed_keys:
            # Only delete DB rows for sandboxes whose S3 objects were
            # actually removed — the rest will be retried next run.
            logger.warning(
                "Batch %d: %d S3 deletions failed, skipping their DB rows",
                batch_num,
                len(failed_keys),
            )
            filtered = [
                (sid, pfn)
                for sid, pfn in zip(sb_ids, pfns)
                if pfn_to_key(pfn) not in failed_keys
            ]
            if filtered:
                sb_ids, pfns = map(list, zip(*filtered))
            else:
                sb_ids, pfns = [], []

        logger.info(
            "Batch %d: deleted %d from S3 (%.1fs)",
            batch_num,
            len(objects) - len(failed_keys),
            s3_duration,
        )

        if not sb_ids:
            continue

        # Phase 3: Delete from DB in small chunks (each chunk is a short
        # transaction to avoid locking millions of rows in a single DELETE).
        # Up to 10 chunks run concurrently via a semaphore.
        t0 = time.monotonic()
        delete_chunk_size = 1000
        sem = asyncio.Semaphore(10)

        async def _delete_chunk(chunk: list[int], _sem: asyncio.Semaphore = sem) -> int:
            async with _sem, sandbox_metadata_db:
                return await sandbox_metadata_db.delete_sandboxes(chunk)

        tasks = [
            _delete_chunk(sb_ids[i : i + delete_chunk_size])
            for i in range(0, len(sb_ids), delete_chunk_size)
        ]
        results = await asyncio.gather(*tasks)
        deleted = sum(results)
        db_duration = time.monotonic() - t0
        total_deleted += deleted
        logger.info(
            "Batch %d: deleted %d from DB (%.1fs, total so far: %d)",
            batch_num,
            deleted,
            db_duration,
            total_deleted,
        )

        # If we got fewer than batch_size, there are no more candidates
        if len(sb_ids) < batch_size:
            break

    return total_deleted
