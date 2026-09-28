"""Models for sandbox metadata and upload or download responses."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class ChecksumAlgorithm(StrEnum):
    """Algorithms used to calculate sandbox checksums."""

    SHA256 = "sha256"


class SandboxFormat(StrEnum):
    """Archive formats supported for sandboxes."""

    TAR_BZ2 = "tar.bz2"
    TAR_ZST = "tar.zst"


class SandboxInfo(BaseModel):
    """Metadata describing a sandbox archive.

    Attributes:
        checksum_algorithm: Algorithm used to calculate the checksum.
        checksum: Hexadecimal checksum of the sandbox archive.
        size: Sandbox size in bytes.
        format: Archive format of the sandbox.
    """

    checksum_algorithm: ChecksumAlgorithm
    checksum: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    size: int = Field(ge=1)
    format: SandboxFormat


class SandboxType(StrEnum):
    """Types of sandboxes associated with a job."""

    Input = "Input"
    Output = "Output"


class SandboxDownloadResponse(BaseModel):
    """Response containing a URL for downloading a sandbox.

    Attributes:
        url: URL from which the sandbox can be downloaded.
        expires_in: Number of seconds until the URL expires.
    """

    url: str
    expires_in: int


class SandboxUploadResponse(BaseModel):
    """Response containing details for uploading a sandbox.

    Attributes:
        pfn: Physical file name assigned to the uploaded sandbox.
        url: Optional upload URL.
        fields: Form fields required for the upload.
    """

    pfn: str
    url: str | None = None
    fields: dict[str, str] = {}
