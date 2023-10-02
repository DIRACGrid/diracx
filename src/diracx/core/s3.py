"""Utilities for interacting with S3-compatible storage."""
from __future__ import annotations

import base64
from typing import TypedDict

from botocore.errorfactory import ClientError

PRESIGNED_URL_TIMEOUT = 5 * 60


class S3PresignedPostInfo(TypedDict):
    url: str
    fields: dict[str, str]


def hack_get_s3_client():
    # TODO: Use async
    import boto3
    from botocore.config import Config

    s3_cred = {
        "endpoint": "http://christohersmbp4.localdomain:32000",
        "access_key_id": "console",
        "secret_access_key": "console123",
    }
    bucket_name = "sandboxes"
    my_config = Config(signature_version="v4")
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_cred["endpoint"],
        aws_access_key_id=s3_cred["access_key_id"],
        aws_secret_access_key=s3_cred["secret_access_key"],
        config=my_config,
    )
    try:
        s3.create_bucket(Bucket=bucket_name)
    except Exception:
        pass
    return s3, bucket_name


def s3_object_exists(s3_client, bucket_name, key) -> bool:
    """Check if an object exists in an S3 bucket."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise
        return False
    else:
        return True


def generate_presigned_upload(
    s3_client, bucket_name, key, checksum_algorithm, checksum, size
) -> S3PresignedPostInfo:
    """Generate a presigned URL and fields for uploading a file to S3

    The signature is restricted to only accept data with the given checksum and size.
    """
    fields = {
        "x-amz-checksum-algorithm": checksum_algorithm,
        f"x-amz-checksum-{checksum_algorithm}": b16_to_b64(checksum),
    }
    conditions = [["content-length-range", size, size]] + [
        {k: v} for k, v in fields.items()
    ]
    return s3_client.generate_presigned_post(
        Bucket=bucket_name,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=PRESIGNED_URL_TIMEOUT,
    )


def b16_to_b64(hex_string: str) -> str:
    """Convert hexadecimal encoded data to base64 encoded data"""
    return base64.b64encode(base64.b16decode(hex_string.upper())).decode()
