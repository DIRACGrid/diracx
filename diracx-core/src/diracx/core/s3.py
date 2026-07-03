"""Utilities for interacting with S3-compatible storage.

This module provides helpers for S3 bucket and object existence checks,
presigned upload generation, and bulk delete operations with retry logic.
"""

from __future__ import annotations

__all__ = [
    "b16_to_b64",
    "generate_presigned_upload",
    "s3_bucket_exists",
    "s3_bulk_delete_with_retry",
    "s3_object_exists",
]

import asyncio
import base64
from typing import TYPE_CHECKING, TypedDict, cast

from signurlarity.exceptions import NoSuchBucketError, NoSuchKeyError, PresignError

from diracx.core.models import ChecksumAlgorithm

if TYPE_CHECKING:
    from typing import TypedDict

    from signurlarity.aio.client import AsyncClient

    class S3Object(TypedDict):
        """TypedDict representing an S3 object identifier for deletion.

        Attributes:
            Key (str): Object key in the bucket.
        """

        Key: str


class S3PresignedPostInfo(TypedDict):
    """TypedDict containing presigned upload information.

    Attributes:
        url (str): Presigned URL to upload an object.
        fields (dict[str, str]): Form fields required for the upload.
    """

    url: str
    fields: dict[str, str]


async def s3_bucket_exists(s3_client: AsyncClient, bucket_name: str) -> bool:
    """Check whether a bucket exists in S3.

    Args:
        s3_client (AsyncClient): S3 client instance.
        bucket_name (str): Bucket name to check.

    Returns:
        bool: True if the bucket exists, otherwise False.
    """
    return await _s3_exists(s3_client.head_bucket, Bucket=bucket_name)


async def s3_object_exists(s3_client: AsyncClient, bucket_name: str, key: str) -> bool:
    """Check whether an object exists in an S3 bucket.

    Args:
        s3_client (AsyncClient): S3 client instance.
        bucket_name (str): Bucket containing the object.
        key (str): Object key to check.

    Returns:
        bool: True if the object exists, otherwise False.
    """
    return await _s3_exists(s3_client.head_object, Bucket=bucket_name, Key=key)


async def _s3_exists(method, **kwargs: str) -> bool:
    """Check whether an S3 resource exists by invoking a client HEAD method.

    Args:
        method: S3 client HEAD method to call (for bucket or object existence).
        **kwargs (str): Keyword arguments to pass to the HEAD method.

    Returns:
        bool: True if the resource exists, otherwise False.
    """
    try:
        await method(**kwargs)
    except PresignError:
        raise
    except (NoSuchBucketError, NoSuchKeyError):
        return False
    else:
        return True


async def generate_presigned_upload(
    s3_client: AsyncClient,
    bucket_name: str,
    key: str,
    checksum_algorithm: ChecksumAlgorithm,
    checksum: str,
    size: int,
    validity_seconds: int,
) -> S3PresignedPostInfo:
    """Generate a presigned URL and fields for uploading a file to S3.

    The signature is restricted to only accept data with the given checksum
    and size.

    Args:
        s3_client (S3Client): S3 client instance.
        bucket_name (str): The target S3 bucket name.
        key (str): Object key to upload.
        checksum_algorithm (ChecksumAlgorithm): Checksum algorithm to enforce.
        checksum (str): Checksum value for the uploaded object.
        size (int): Exact size of the uploaded object in bytes.
        validity_seconds (int): Time in seconds that the presigned upload URL is valid.

    Returns:
        S3PresignedPostInfo: Presigned upload URL and form fields.
    """
    fields = {
        "x-amz-checksum-algorithm": checksum_algorithm,
        f"x-amz-checksum-{checksum_algorithm}": b16_to_b64(checksum),
    }
    conditions = [["content-length-range", size, size]] + [
        {k: v} for k, v in fields.items()
    ]
    result = await s3_client.generate_presigned_post(
        Bucket=bucket_name,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=validity_seconds,
    )
    return cast(S3PresignedPostInfo, result)


def b16_to_b64(hex_string: str) -> str:
    """Convert hexadecimal encoded data to base64 encoded data.

    Args:
        hex_string (str): Hexadecimal string to convert.

    Returns:
        str: Base64-encoded representation of the input.
    """
    return base64.b64encode(base64.b16decode(hex_string.upper())).decode()


async def s3_bulk_delete_with_retry(
    s3_client, bucket: str, objects: list[S3Object]
) -> set[str]:
    """Delete objects from S3 in chunks of 1000, retrying failures.

    Args:
        s3_client: S3 client instance.
        bucket (str): Bucket containing the objects to delete.
        objects (list[S3Object]): List of objects to delete.

    Returns:
        set[str]: Keys that failed to delete after all retries.
    """
    max_chunk_size = 1000
    chunks = [
        objects[i : i + max_chunk_size] for i in range(0, len(objects), max_chunk_size)
    ]
    tasks = [_s3_delete_chunk_with_retry(s3_client, bucket, chunk) for chunk in chunks]
    results = await asyncio.gather(*tasks)
    failed_keys: set[str] = set()
    for result in results:
        failed_keys.update(result)
    return failed_keys


async def _s3_delete_chunk_with_retry(
    s3_client, bucket: str, objects: list[S3Object]
) -> set[str]:
    """Try to delete a chunk of S3 objects, retrying partial failures.

    Args:
        s3_client: S3 client instance.
        bucket (str): Bucket containing the objects to delete.
        objects (list[S3Object]): Chunk of objects to delete.

    Returns:
        set[str]: Keys that failed to delete after all retries.
    """
    max_attempts = 5
    delay = 1.0
    remaining = objects
    for attempt in range(1, max_attempts + 1):
        try:
            response = await s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": remaining, "Quiet": True},
            )
        except PresignError:
            if attempt == max_attempts:
                return {obj["Key"] for obj in remaining}
            await asyncio.sleep(delay)
            delay *= 2
            continue

        errors = response.get("Errors", [])
        if not errors:
            return set()

        # Retry only the keys that failed
        failed_keys = {e["Key"] for e in errors}
        if attempt == max_attempts:
            return failed_keys
        remaining = [obj for obj in remaining if obj["Key"] in failed_keys]
        await asyncio.sleep(delay)
        delay *= 2

    return set()
