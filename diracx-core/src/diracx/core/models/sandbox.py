"""Sandbox-related models for upload and download metadata.

This module defines the data models used to describe sandbox checksums,
formats, and upload/download responses.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class ChecksumAlgorithm(StrEnum):
    """Supported checksum algorithms for sandbox payloads."""

    SHA256 = "sha256"


class SandboxFormat(StrEnum):
    """Supported archive formats for sandbox payloads."""

    TAR_BZ2 = "tar.bz2"
    TAR_ZST = "tar.zst"


class SandboxInfo(BaseModel):
    """Metadata describing a sandbox archive payload."""

    checksum_algorithm: ChecksumAlgorithm
    checksum: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    size: int = Field(ge=1)
    format: SandboxFormat


class SandboxType(StrEnum):
    """The role of a sandbox in a job lifecycle."""

    Input = "Input"
    Output = "Output"


class SandboxDownloadResponse(BaseModel):
    """Response payload for a sandbox download URL."""

    url: str
    expires_in: int


class SandboxUploadResponse(BaseModel):
    """Response payload for a sandbox upload request."""

    pfn: str
    url: str | None = None
    fields: dict[str, str] = {}
