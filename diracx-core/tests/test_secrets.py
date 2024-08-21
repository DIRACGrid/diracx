from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import TypeAdapter

from diracx.core.settings import TokenSigningKey


def compare_keys(key1, key2):
    """Compare two keys by checking their public numebrs."""
    assert key1.public_key().public_numbers() == key2.public_key().public_numbers()


def test_token_signing_key(tmp_path):
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=1024,
    )
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
