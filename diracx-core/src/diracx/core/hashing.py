"""Shared digests used by DiracX (DX-ADR-004 and later content hashes).

These helpers are pure encoding + hash functions. Callers in logic/db own when
to compute them; feeder plugins never produce ``InputHash`` themselves.
"""

from __future__ import annotations

__all__ = ["compute_input_hash"]

import json
from typing import Any

import xxhash


def compute_input_hash(
    lfn: str | None,
    descriptor: dict[str, Any] | None,
) -> bytes:
    """Return the 16-byte XXH3-128 digest for an ``(LFN, Descriptor)`` pair.

    The canonical byte form is UTF-8 JSON of ``{"descriptor": ..., "lfn": ...}``
    with sorted keys, compact separators, and ``ensure_ascii=True``. ``None``
    values are encoded as JSON ``null``. This encoding is internal to DiracX
    and may be migrated later (DX-ADR-004 open issue on canonicalisation).

    Args:
        lfn: Logical file name, or ``None`` for non-file inputs.
        descriptor: Opaque descriptor mapping, or ``None``.

    Returns:
        Sixteen digest bytes suitable for ``Inputs.InputHash``.
    """
    canonical = json.dumps(
        {"descriptor": descriptor, "lfn": lfn},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return xxhash.xxh3_128_digest(canonical)
