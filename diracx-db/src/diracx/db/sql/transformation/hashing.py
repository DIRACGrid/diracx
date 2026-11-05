"""InputHash digests for TransformationDB (DX-ADR-004).

Computed on insert so feeders do not supply the digest. Reuses
:func:`diracx.core.hashing.hash_json` (SHA-256 over JCS), then truncates to
16 bytes for ``Inputs.InputHash``.
"""

from __future__ import annotations

from typing import Any

from diracx.core.hashing import hash_json


def _input_hash(lfn: str | None, descriptor: dict[str, Any] | None) -> bytes:
    """Return a 16-byte digest of the canonical ``(LFN, Descriptor)`` pair.

    Digests with :func:`~diracx.core.hashing.hash_json`, then keeps the
    leftmost 16 bytes so ``Inputs.InputHash`` stays ``BINARY(16)``.
    ``descriptor is None`` is treated as ``{}`` so missing and empty
    descriptors dedupe together. ``lfn is None`` stays JSON ``null`` (no file).

    Args:
        lfn: Logical file name, or ``None`` when the input is not a file.
        descriptor: Opaque JSON object, or ``None`` (normalised to ``{}``).

    Returns:
        Leftmost 16 bytes of the SHA-256 digest.
    """
    envelope = {"LFN": lfn, "Descriptor": {} if descriptor is None else descriptor}
    return hash_json(envelope)[:16]
