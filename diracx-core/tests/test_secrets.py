from __future__ import annotations

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import TypeAdapter

from diracx.core.settings import TokenSigningKey


def compare_keys(key1, key2):
    """Compare two keys by checking their public keys."""
    key1_public = key1.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    key2_public = key2.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    assert key1_public == key2_public


def test_token_signing_key(tmp_path):
    private_key = Ed25519PrivateKey.generate()
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    key_file = tmp_path / "private_key.pem"
    key_file.write_text(private_key_pem)

    adapter = TypeAdapter(TokenSigningKey)

    # Test that we can load a key from a file
    compare_keys(
        adapter.validate_python(f"{key_file}").jwk.get_private_key(), private_key
    )
    compare_keys(
        adapter.validate_python(f"file://{key_file}").jwk.get_private_key(),
        private_key,
    )

    # Test with can load the PEM data directly
    compare_keys(
        adapter.validate_python(private_key_pem).jwk.get_private_key(), private_key
    )
