"""Common Workflow Language utilities shared by the client and the services."""

from __future__ import annotations

__all__ = [
    "CANONICAL_VERSION",
    "CanonicalProcess",
    "canonical_bytes",
    "decompose",
    "format_reference",
    "pack",
    "parse_reference",
    "unpack",
]

from .canonical import (
    CANONICAL_VERSION,
    CanonicalProcess,
    canonical_bytes,
    decompose,
    format_reference,
    pack,
    parse_reference,
    unpack,
)
