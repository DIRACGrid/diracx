"""JWKS key management scripts.

See https://datatracker.ietf.org/doc/html/rfc7517 for further details.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

from joserfc.jwk import JWKRegistry, Key, KeySet
from uuid_utils import uuid7

logger = logging.getLogger(__name__)

# ---------- Helpers ----------------------------------------------------------


def load_jwks(path: Path) -> KeySet:
    """Return a (possibly empty) JWKSet."""
    if path.exists():
        return KeySet.import_key_set(json.loads(path.read_text()))
    logger.warning("JWKS file %s not found – creating a new one", path)
    return KeySet(keys=[])


def save_jwks(path: Path, jwks: KeySet) -> None:
    """Write JWKSet to disk *including* private parts."""
    path.write_text(json.dumps(jwks.as_dict(private=True), indent=2))
    logger.info("JWKS written to %s", path)


def new_key(
    kty: str = "OKP",
    crv_or_size: str | int = "Ed25519",
) -> Key:
    """Create a fresh private signing key."""
    parameters = {
        "key_ops": ["sign", "verify"],
        "alg": "EdDSA",
        "kid": uuid7().hex,
    }
    return JWKRegistry.generate_key(
        key_type=kty,
        crv_or_size=crv_or_size,
        private=True,
        parameters=parameters,  # type: ignore[arg-type]
    )


# ---------- CLI --------------------------------------------------------------


async def rotate_jwk(args):
    """Rotate keys in a JWKS file by inserting a new key at index 0 (active)."""
    logger.info("Rotating JWKs...")

    crv_or_size = args.crv_or_size
    if isinstance(crv_or_size, str) and crv_or_size.isdigit():
        crv_or_size = int(crv_or_size)

    jwks_path = Path(args.jwks_path)
    jwks = load_jwks(jwks_path)

    # Current key (at index 0) is set to "verify" only
    if len(jwks.keys) > 0:
        active_key = jwks.keys[0]
        active_key_dict = active_key.as_dict(private=True)
        active_key_dict["key_ops"] = sorted(
            set(active_key_dict.get("key_ops", [])) - {"sign"}
        )
        jwks.keys[0] = JWKRegistry.import_key(active_key_dict)

    jwk = new_key(args.kty, crv_or_size)
    jwks.keys.insert(0, jwk)

    save_jwks(jwks_path, jwks)


async def delete_jwk(args):
    """Delete a JWK from a JWKS file."""
    logger.info("Deleting JWK...")

    path = Path(args.jwks_path)
    jwks = load_jwks(path)
    jwks.keys = [k for k in jwks.keys if k.get("kid") != args.kid]
    save_jwks(path, jwks)


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    rotate_jwk_parser = subparsers.add_parser(
        "rotate-jwk", help="Rotate JWK keys in a JWKS file"
    )
    rotate_jwk_parser.add_argument(
        "--jwks-path", required=True, help="Path to the existing (old) JWKS JSON file."
    )

    rotate_jwk_parser.add_argument(
        "--kty", default="OKP", help="Key type for the new key."
    )
    rotate_jwk_parser.add_argument(
        "--crv-or-size", default="Ed25519", help="Curve or size for the new key."
    )
    rotate_jwk_parser.set_defaults(func=rotate_jwk)

    delete_jwk_parser = subparsers.add_parser(
        "delete-jwk", help="Delete a JWK key from a JWKS file"
    )
    delete_jwk_parser.add_argument(
        "--jwks-path", required=True, help="Path to the JWKS JSON file."
    )
    delete_jwk_parser.add_argument(
        "--kid", required=True, help="Key ID (kid) of the key to delete."
    )
    delete_jwk_parser.set_defaults(func=delete_jwk)

    args = parser.parse_args()
    logger.setLevel(logging.INFO)
    asyncio.run(args.func(args))


if __name__ == "__main__":
    parse_args()
