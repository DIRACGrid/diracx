"""Canonical JSON bytes and content digests for the Transformation System.

Canonical form is RFC 8785 JSON Canonicalization Scheme (JCS), via the
``rfc8785`` package. Digests:

* :func:`hash_json` — SHA-256 (32 raw bytes) for content-addressed documents
  (``Processes``, ``Requirements``, ``ResolvedHints``); see DX-ADR-007.
* :func:`input_hash` — SHA-256 truncated to the leftmost 16 bytes for
  ``Inputs.InputHash`` over the canonical ``(LFN, Descriptor)`` envelope;
  see DX-ADR-004.

JCS rules that matter here (stable forever for stored digests):

* JSON types only; object keys must be strings.
* Object keys sorted per JCS; compact UTF-8 with no insignificant whitespace.
* Integers must lie in the IEEE-754 safe-integer domain (about ±2**53).
* ``-0.0`` is normalised to ``0``.
* No silent coercion of Pydantic models — callers dump first.
* ``tuple`` is accepted by ``rfc8785`` as a JSON array (same as ``list``);
  pass ``list`` at call sites to avoid accidental equivalence.

CWL-specific preparation (``doc``/``label``, ``$import``, nested ``run`` refs)
is not performed here; compile that into a JSON value, then call
:func:`hash_json`.
"""

from __future__ import annotations

import hashlib
from typing import Any

import rfc8785

__all__ = [
    "canonical_json_bytes",
    "hash_json",
    "input_hash",
]


def canonical_json_bytes(value: Any) -> bytes:
    """Return RFC 8785 (JCS) canonical UTF-8 bytes for ``value``.

    Args:
        value: A JSON-compatible Python value (see module docstring).

    Returns:
        Canonical UTF-8 encoding of ``value``.

    Raises:
        ValueError: through rfc8785 specific subclasses (e.g. IntegerDomainError).
    """
    return rfc8785.dumps(value)


def hash_json(value: Any) -> bytes:
    """Return the SHA-256 digest of the JCS canonical form of ``value``.

    Used as the primary key for content-addressed documents
    (``Processes``, ``Requirements``, ``ResolvedHints``). Today this is always
    SHA-256; an optional algorithm parameter can be added later if another
    cryptographic digest is required. Input dedupe uses :func:`input_hash`
    (truncated SHA-256) instead.

    Args:
        value: A JSON-compatible Python value.

    Returns:
        Raw SHA-256 digest (length 32).
    """
    return hashlib.sha256(canonical_json_bytes(value)).digest()


def input_hash(lfn: str | None, descriptor: dict[str, Any] | None) -> bytes:
    """Return a 16-byte digest of the canonical ``(LFN, Descriptor)`` pair.

    Digests with SHA-256 over JCS bytes, then keeps the leftmost 16 bytes so
    ``Inputs.InputHash`` stays ``BINARY(16)``. ``descriptor is None`` is treated
    as ``{}`` so missing and empty descriptors dedupe together. ``lfn is None``
    stays JSON ``null`` (no file).

    Args:
        lfn: Logical file name, or ``None`` when the input is not a file.
        descriptor: Opaque JSON object, or ``None`` (normalised to ``{}``).

    Returns:
        Leftmost 16 bytes of the SHA-256 digest.
    """
    envelope = {"LFN": lfn, "Descriptor": {} if descriptor is None else descriptor}
    return hashlib.sha256(canonical_json_bytes(envelope)).digest()[:16]
