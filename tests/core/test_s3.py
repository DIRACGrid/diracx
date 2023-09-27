from __future__ import annotations

import base64
import hashlib
import secrets

import botocore.exceptions
import pytest
import requests
from moto import mock_s3

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


def _random_file(size_bytes: int):
    file_content = secrets.token_bytes(size_bytes)
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
def moto_s3():
    """Very basic moto-based S3 backend.

    This is a fixture that can be used to test S3 interactions using moto.
    Note that this is not a complete S3 backend, in particular authentication
    and validation of requests is not implemented.
    """
    with mock_s3():
        client = botocore.session.get_session().create_client("s3")
        client.create_bucket(Bucket=BUCKET_NAME)
        client.create_bucket(Bucket=OTHER_BUCKET_NAME)
        yield client


def test_s3_bucket_exists(moto_s3):
    assert s3_bucket_exists(moto_s3, BUCKET_NAME)
    assert not s3_bucket_exists(moto_s3, MISSING_BUCKET_NAME)


def test_s3_object_exists(moto_s3):
    with pytest.raises(botocore.exceptions.ClientError):
        s3_object_exists(moto_s3, MISSING_BUCKET_NAME, "key")

    assert not s3_object_exists(moto_s3, BUCKET_NAME, "key")
    moto_s3.put_object(Bucket=BUCKET_NAME, Key="key", Body=b"hello")
    assert s3_object_exists(moto_s3, BUCKET_NAME, "key")


def test_presigned_upload_moto(moto_s3):
    """Test the presigned upload with moto

    This doesn't actually test the signature, see test_presigned_upload_minio
    """
    file_content, checksum = _random_file(128)
    key = f"{checksum}.dat"
    upload_info = generate_presigned_upload(
        moto_s3, BUCKET_NAME, key, "sha256", checksum, len(file_content), 60
    )

    # Upload the file
    r = requests.post(
        upload_info["url"], data=upload_info["fields"], files={"file": file_content}
    )
    assert r.status_code == 204, r.text

    # Make sure the object is actually there
    obj = moto_s3.get_object(Bucket=BUCKET_NAME, Key=key)
    assert obj["Body"].read() == file_content


@pytest.fixture(scope="session")
def minio_client(demo_urls):
    """Create a S3 client that uses minio from the demo as backend"""
    yield botocore.session.get_session().create_client(
        "s3",
        endpoint_url=demo_urls["minio"],
        aws_access_key_id="console",
        aws_secret_access_key="console123",
    )


@pytest.fixture(scope="session")
def test_bucket(minio_client):
    """Create a test bucket that is cleaned up after the test session"""
    bucket_name = f"dirac-test-{secrets.token_hex(8)}"
    minio_client.create_bucket(Bucket=bucket_name)
    yield bucket_name
    for obj in minio_client.list_objects(Bucket=bucket_name)["Contents"]:
        minio_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    minio_client.delete_bucket(Bucket=bucket_name)


@pytest.mark.parametrize(
    "content,checksum,size,expected_error",
    [
        # Make sure a valid request works
        [*_random_file(128), 128, None],
        # Check with invalid sizes
        [*_random_file(128), 127, "exceeds the maximum"],
        [*_random_file(128), 129, "smaller than the minimum"],
        # Check with invalid checksum
        [_random_file(128)[0], _random_file(128)[1], 128, "ContentChecksumMismatch"],
    ],
)
def test_presigned_upload_minio(
    minio_client, test_bucket, content, checksum, size, expected_error
):
    """Test the presigned upload with Minio

    This is a more complete test that checks that the presigned upload works
    and is properly validated by Minio. This is not possible with moto as it
    doesn't actually validate the signature.
    """
    key = f"{checksum}.dat"
    # Prepare the signed URL
    upload_info = generate_presigned_upload(
        minio_client, test_bucket, key, "sha256", checksum, size, 60
    )
    # Ensure the URL doesn't work
    r = requests.post(
        upload_info["url"], data=upload_info["fields"], files={"file": content}
    )
    if expected_error is None:
        assert r.status_code == 204, r.text
        assert s3_object_exists(minio_client, test_bucket, key)
    else:
        assert r.status_code == 400, r.text
        assert expected_error in r.text
        assert not s3_object_exists(minio_client, test_bucket, key)
