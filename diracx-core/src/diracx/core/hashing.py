"""Canonical JSON bytes and content digests for the Transformation System.

Canonical form is RFC 8785 JSON Canonicalization Scheme (JCS), via the
``rfc8785`` package. Digests:

* :func:`hash_json` — SHA-256 (32 raw bytes) for content-addressed documents
  (``Processes``, ``Requirements``, ``ResolvedHints``); see DX-ADR-007.
* :func:`input_hash` — XXH3-128 (16 raw bytes) for ``Inputs.InputHash`` over
  the canonical ``(LFN, Descriptor)`` envelope; see DX-ADR-004.

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
import xxhash

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
    cryptographic digest is required. Non-cryptographic digests (e.g. input
    dedupe) stay on :func:`input_hash`.

    Args:
        value: A JSON-compatible Python value.

    Returns:
        Raw SHA-256 digest (length 32).
    """
    return hashlib.sha256(canonical_json_bytes(value)).digest()


def input_hash(lfn: str | None, descriptor: dict[str, Any] | None) -> bytes:
    """Return the XXH3-128 digest of the canonical ``(LFN, Descriptor)`` pair.

    ``descriptor is None`` is treated as ``{}`` so missing and empty descriptors
    dedupe together. ``lfn is None`` stays JSON ``null`` (no file).

    Args:
        lfn: Logical file name, or ``None`` when the input is not a file.
        descriptor: Opaque JSON object, or ``None`` (normalised to ``{}``).

    Returns:
        Raw XXH3-128 digest (length 16).
    """
    envelope = {"LFN": lfn, "Descriptor": {} if descriptor is None else descriptor}
    return xxhash.xxh3_128_digest(canonical_json_bytes(envelope))
