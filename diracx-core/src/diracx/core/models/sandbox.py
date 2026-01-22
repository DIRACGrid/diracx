from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class ChecksumAlgorithm(StrEnum):
    SHA256 = "sha256"


class SandboxFormat(StrEnum):
    TAR_BZ2 = "tar.bz2"
    TAR_ZST = "tar.zst"


class SandboxInfo(BaseModel):
    checksum_algorithm: ChecksumAlgorithm
    checksum: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    size: int = Field(ge=1)
    format: SandboxFormat


class SandboxType(StrEnum):
    Input = "Input"
    Output = "Output"


class SandboxDownloadResponse(BaseModel):
    url: str
    expires_in: int


class SandboxUploadResponse(BaseModel):
    pfn: str
    url: str | None = None
    fields: dict[str, str] = {}
