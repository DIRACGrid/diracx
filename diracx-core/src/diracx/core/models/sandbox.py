"""Models for sandbox metadata and upload or download responses."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from ._base import BaseModel


class ChecksumAlgorithm(StrEnum):
    """Algorithms used to calculate sandbox checksums."""

    SHA256 = "sha256"


class SandboxFormat(StrEnum):
    """Archive formats supported for sandboxes."""

    TAR_BZ2 = "tar.bz2"
    TAR_ZST = "tar.zst"


class SandboxInfo(BaseModel):
    """Metadata describing a sandbox archive."""

    checksum_algorithm: ChecksumAlgorithm
    """Algorithm used to calculate the checksum."""
    checksum: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    """Hexadecimal checksum of the sandbox archive."""
    size: int = Field(ge=1)
    """Sandbox size in bytes."""
    format: SandboxFormat
    """Archive format of the sandbox."""


class SandboxType(StrEnum):
    """Types of sandboxes associated with a job."""

    Input = "Input"
    Output = "Output"


class SandboxDownloadResponse(BaseModel):
    """Response containing a URL for downloading a sandbox."""

    url: str
    """URL from which the sandbox can be downloaded."""
    expires_in: int
    """Number of seconds until the URL expires."""


class SandboxUploadResponse(BaseModel):
    """Response containing details for uploading a sandbox."""

    pfn: str
    """Physical file name assigned to the uploaded sandbox."""
    url: str | None = None
    """Optional upload URL."""
    fields: dict[str, str] = {}
    """Form fields required for the upload."""
