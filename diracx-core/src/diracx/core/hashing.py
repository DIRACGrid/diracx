"""Canonical JSON bytes and content digests for the Transformation System.

Content-addressed rows (``Processes``, ``Requirements``, ``ResolvedHints``) use
:func:`hash_json` (SHA-256, 32 raw bytes). ``Inputs.InputHash`` reuses the
same canonicaliser via :func:`input_hash` (XXH3-128, 16 raw bytes). See
DX-ADR-004 and DX-ADR-007.

Canonical form (stable forever for stored digests):

* JSON types only: ``null``, ``bool``, signed 64-bit ``int``, finite
  ``float``, ``str``, ``list``, ``dict`` with string keys.
* Object keys sorted lexicographically as UTF-8 code units (Python
  ``sort_keys=True``).
* Compact separators ``(",", ":")``, ``ensure_ascii=False``, UTF-8 encoding.
* No silent coercion of Pydantic models or other objects — callers dump first.
* Floats that are exact integers are left as floats if the caller passed a
  float; prefer ``int`` at the call site when the value is integral.

CWL-specific preparation (``doc``/``label``, ``$import``, nested ``run`` refs)
is not performed here; compile that into a JSON value, then call
:func:`hash_json`.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any

import xxhash

__all__ = [
    "canonical_json_bytes",
    "hash_json",
    "input_hash",
]

_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


def canonical_json_bytes(value: Any) -> bytes:
    """Return stable UTF-8 bytes for a JSON-compatible value.

    Args:
        value: A JSON-compatible Python value (see module docstring).

    Returns:
        Canonical UTF-8 encoding of ``value``.

    Raises:
        TypeError: If ``value`` is not JSON-compatible.
        ValueError: If a number is non-finite or outside signed int64.
    """
    cleaned = _ensure_json_value(value)
    return json.dumps(
        cleaned,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def hash_json(value: Any) -> bytes:
    """Return the SHA-256 digest of the canonical form of ``value``.

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

    Args:
        lfn: Logical file name, or ``None`` when the input is not a file.
        descriptor: Opaque JSON object, or ``None``.

    Returns:
        Raw XXH3-128 digest (length 16).
    """
    envelope = {"LFN": lfn, "Descriptor": descriptor}
    return xxhash.xxh3_128_digest(canonical_json_bytes(envelope))


def _ensure_json_value(value: Any) -> Any:
    """Return ``value`` if JSON-compatible, else raise ``TypeError``/``ValueError``."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        if not _INT64_MIN <= value <= _INT64_MAX:
            raise ValueError(f"Integer {value} is outside the signed 64-bit range")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Non-finite floats are not JSON-compatible")
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return [_ensure_json_value(item) for item in value]
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"Dict keys must be str, got {type(key).__name__}")
            cleaned[key] = _ensure_json_value(item)
        return cleaned
    raise TypeError(f"Value of type {type(value).__name__} is not JSON-compatible")
