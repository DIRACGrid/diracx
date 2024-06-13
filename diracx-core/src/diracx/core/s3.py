"""Utilities for interacting with S3-compatible storage."""

from __future__ import annotations

__all__ = (
    "s3_bucket_exists",
    "s3_object_exists",
    "generate_presigned_upload",
)

import base64
from typing import TYPE_CHECKING, TypedDict, cast

from botocore.errorfactory import ClientError

from .models import ChecksumAlgorithm

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client


class S3PresignedPostInfo(TypedDict):
    url: str
    fields: dict[str, str]


async def s3_bucket_exists(s3_client: S3Client, bucket_name: str) -> bool:
    """Check if a bucket exists in S3."""
    return await _s3_exists(s3_client.head_bucket, Bucket=bucket_name)


async def s3_object_exists(s3_client: S3Client, bucket_name: str, key: str) -> bool:
    """Check if an object exists in an S3 bucket."""
    return await _s3_exists(s3_client.head_object, Bucket=bucket_name, Key=key)


async def _s3_exists(method, **kwargs: str) -> bool:
    try:
        await method(**kwargs)
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise
        return False
    else:
        return True


async def generate_presigned_upload(
    s3_client: S3Client,
    bucket_name: str,
    key: str,
    checksum_algorithm: ChecksumAlgorithm,
    checksum: str,
    size: int,
    validity_seconds: int,
) -> S3PresignedPostInfo:
    """Generate a presigned URL and fields for uploading a file to S3.

    The signature is restricted to only accept data with the given checksum and size.
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
    """Convert hexadecimal encoded data to base64 encoded data."""
    return base64.b64encode(base64.b16decode(hex_string.upper())).decode()
