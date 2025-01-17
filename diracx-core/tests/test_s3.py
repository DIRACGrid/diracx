from __future__ import annotations

import base64
import hashlib
import random
import secrets

import httpx
import pytest
from aiobotocore.session import get_session

from diracx.core.s3 import (
    b16_to_b64,
    generate_presigned_upload,
    s3_bucket_exists,
    s3_object_exists,
)

BUCKET_NAME = "test_bucket"
OTHER_BUCKET_NAME = "other_bucket"
MISSING_BUCKET_NAME = "missing_bucket"
INVALID_BUCKET_NAME = ".."

rng = random.Random(1234)


def _random_file(size_bytes: int):
    file_content = rng.randbytes(size_bytes)
    checksum = hashlib.sha256(file_content).hexdigest()
    return file_content, checksum


def test_b16_to_b64_hardcoded():
    assert b16_to_b64("25") == "JQ==", "%"
    # Make sure we're using the URL-safe variant of base64
    assert b16_to_b64("355b3e51473f") == "NVs+UUc/", "5[>QG?"


def test_b16_to_b64_random():
    data = secrets.token_bytes()
    input_hex = data.hex()
    expected = base64.b64encode(data).decode()
    actual = b16_to_b64(input_hex)
    assert actual == expected, data.hex()


@pytest.fixture(scope="function")
async def moto_s3(aio_moto):
    """Very basic moto-based S3 backend.

    This is a fixture that can be used to test S3 interactions using moto.
    Note that this is not a complete S3 backend, in particular authentication
    and validation of requests is not implemented.
    """
    async with get_session().create_client("s3", **aio_moto) as client:
        await client.create_bucket(Bucket=BUCKET_NAME)
        await client.create_bucket(Bucket=OTHER_BUCKET_NAME)
        yield client


async def test_s3_bucket_exists(moto_s3):
    assert await s3_bucket_exists(moto_s3, BUCKET_NAME)
    assert not await s3_bucket_exists(moto_s3, MISSING_BUCKET_NAME)


async def test_s3_object_exists(moto_s3):
    assert not await s3_object_exists(moto_s3, MISSING_BUCKET_NAME, "key")
    assert not await s3_object_exists(moto_s3, BUCKET_NAME, "key")
    await moto_s3.put_object(Bucket=BUCKET_NAME, Key="key", Body=b"hello")
    assert await s3_object_exists(moto_s3, BUCKET_NAME, "key")


async def test_presigned_upload_moto(moto_s3):
    """Test the presigned upload with moto.

    This doesn't actually test the signature, see test_presigned_upload_minio
    """
    file_content, checksum = _random_file(128)
    key = f"{checksum}.dat"
    upload_info = await generate_presigned_upload(
        moto_s3, BUCKET_NAME, key, "sha256", checksum, len(file_content), 60
    )

    # Upload the file
    async with httpx.AsyncClient() as client:
        r = await client.post(
            upload_info["url"],
            data=upload_info["fields"],
            files={"file": file_content},
        )

    assert r.status_code == 204, r.text

    # Make sure the object is actually there
    obj = await moto_s3.get_object(Bucket=BUCKET_NAME, Key=key)
    assert (await obj["Body"].read()) == file_content


@pytest.fixture(scope="function")
async def minio_client(demo_urls):
    """Create a S3 client that uses minio from the demo as backend."""
    async with get_session().create_client(
        "s3",
        endpoint_url=demo_urls["minio"],
        aws_access_key_id="console",
        aws_secret_access_key="console123",
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def test_bucket(minio_client):
    """Create a test bucket that is cleaned up after the test session."""
    bucket_name = f"dirac-test-{secrets.token_hex(8)}"
    await minio_client.create_bucket(Bucket=bucket_name)
    yield bucket_name
    objects = await minio_client.list_objects(Bucket=bucket_name)
    for obj in objects.get("Contents", []):
        await minio_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    await minio_client.delete_bucket(Bucket=bucket_name)


@pytest.mark.parametrize(
    "content,checksum,size,expected_error",
    [
        # Make sure a valid request works
        pytest.param(*_random_file(128), 128, None, id="valid"),
        # Check with invalid sizes
        pytest.param(*_random_file(128), 127, "exceeds the maximum", id="maximum"),
        pytest.param(*_random_file(128), 129, "smaller than the minimum", id="minimum"),
        # Check with invalid checksum
        pytest.param(
            _random_file(128)[0],
            _random_file(128)[1],
            128,
            "ContentChecksumMismatch",
            id="checksum",
        ),
    ],
)
async def test_presigned_upload_minio(
    minio_client, test_bucket, content, checksum, size, expected_error
):
    """Test the presigned upload with Minio.

    This is a more complete test that checks that the presigned upload works
    and is properly validated by Minio. This is not possible with moto as it
    doesn't actually validate the signature.
    """
    key = f"{checksum}.dat"
    # Prepare the signed URL
    upload_info = await generate_presigned_upload(
        minio_client, test_bucket, key, "sha256", checksum, size, 60
    )
    # Ensure the URL doesn't work
    async with httpx.AsyncClient() as client:
        r = await client.post(
            upload_info["url"], data=upload_info["fields"], files={"file": content}
        )

    if expected_error is None:
        assert r.status_code == 204, r.text
        assert await s3_object_exists(minio_client, test_bucket, key)
    else:
        assert r.status_code == 400, r.text
        assert expected_error in r.text
        assert not (await s3_object_exists(minio_client, test_bucket, key))
