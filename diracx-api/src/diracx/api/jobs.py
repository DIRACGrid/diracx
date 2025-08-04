from __future__ import annotations

__all__ = ("create_sandbox", "download_sandbox")

import hashlib
import logging
import os
import tarfile
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Literal

import httpx
import zstandard

from diracx.client.aio import AsyncDiracClient
from diracx.client.models import SandboxInfo

from .utils import with_client

logger = logging.getLogger(__name__)

SANDBOX_CHECKSUM_ALGORITHM = "sha256"
SANDBOX_COMPRESSION: Literal["zst"] = "zst"


@contextmanager
def tarfile_open(fileobj: BinaryIO):
    """Context manager to extend tarfile.open to support reading zstd compressed files.

    This is only needed for Python <=3.13.
    """
    # Save current position and read magic bytes
    current_pos = fileobj.tell()
    magic = fileobj.read(4)
    fileobj.seek(current_pos)

    # Read magic bytes to determine compression format
    if magic.startswith(b"\x28\xb5\x2f\xfd"):  # zstd magic number
        dctx = zstandard.ZstdDecompressor()
        with dctx.stream_reader(fileobj) as decompressor:
            with tarfile.open(fileobj=decompressor, mode="r|") as tf:
                yield tf
    else:
        with tarfile.open(fileobj=fileobj, mode="r") as tf:
            yield tf


@with_client
async def create_sandbox(paths: list[Path], *, client: AsyncDiracClient) -> str:
    """Create a sandbox from the given paths and upload it to the storage backend.

    Any paths that are directories will be added recursively.
    The returned value is the PFN of the sandbox in the storage backend and can
    be used to submit jobs.
    """
    with tempfile.TemporaryFile(mode="w+b") as tar_fh:
        # Create zstd compressed tar with level 18 and long matching enabled
        compression_params = zstandard.ZstdCompressionParameters.from_level(
            18, enable_ldm=1
        )
        cctx = zstandard.ZstdCompressor(compression_params=compression_params)
        with cctx.stream_writer(tar_fh, closefd=False) as compressor:
            with tarfile.open(fileobj=compressor, mode="w|") as tf:
                for path in paths:
                    logger.debug(
                        "Adding %s to sandbox as %s", path.resolve(), path.name
                    )
                    tf.add(path.resolve(), path.name, recursive=True)
        tar_fh.seek(0)

        hasher = getattr(hashlib, SANDBOX_CHECKSUM_ALGORITHM)()
        while data := tar_fh.read(512 * 1024):
            hasher.update(data)
        checksum = hasher.hexdigest()
        tar_fh.seek(0)
        logger.debug("Sandbox checksum is %s", checksum)

        sandbox_info = SandboxInfo(
            checksum_algorithm=SANDBOX_CHECKSUM_ALGORITHM,
            checksum=checksum,
            size=os.stat(tar_fh.fileno()).st_size,
            format=f"tar.{SANDBOX_COMPRESSION}",
        )

        res = await client.jobs.initiate_sandbox_upload(sandbox_info)
        if res.url:
            logger.debug("Uploading sandbox for %s", res.pfn)
            files = {"file": ("file", tar_fh)}
            async with httpx.AsyncClient() as httpx_client:
                response = await httpx_client.post(
                    res.url, data=res.fields, files=files
                )
                # TODO: Handle this error better
                response.raise_for_status()

            logger.debug(
                "Sandbox uploaded for %s with status code %s",
                res.pfn,
                response.status_code,
            )
        else:
            logger.debug("%s already exists in storage backend", res.pfn)
        return res.pfn


@with_client
async def download_sandbox(pfn: str, destination: Path, *, client: AsyncDiracClient):
    """Download a sandbox from the storage backend to the given destination."""
    res = await client.jobs.get_sandbox_file(pfn=pfn)
    logger.debug("Downloading sandbox for %s", pfn)
    with tempfile.TemporaryFile(mode="w+b") as fh:
        async with httpx.AsyncClient() as http_client:
            response = await http_client.get(res.url)
            # TODO: Handle this error better
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                fh.write(chunk)
        fh.seek(0)
        logger.debug("Sandbox downloaded for %s", pfn)

        with tarfile_open(fh) as tf:
            tf.extractall(path=destination, filter="data")
        logger.debug("Extracted %s to %s", pfn, destination)
