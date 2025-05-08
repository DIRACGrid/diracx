from __future__ import annotations

import json

from joserfc.jwk import KeySet, OKPKey
from pydantic import TypeAdapter
from uuid_utils import uuid7

from diracx.core.settings import TokenSigningKeyStore


def test_token_signing_key(tmp_path):
    keyset = KeySet(
        keys=[
            OKPKey.generate_key(
                parameters={
                    "key_ops": ["sign", "verify"],
                    "alg": "EdDSA",
                    "kid": uuid7().hex,
                }
            )
        ]
    )

    jwks_file = tmp_path / "jwks.json"
    jwks_file.write_text(json.dumps(keyset.as_dict(private=True)))

    adapter = TypeAdapter(TokenSigningKeyStore)

    # Test that we can load a keystore from a file
    assert (
        adapter.validate_python(f"{jwks_file}").jwks.keys[0].kid == keyset.keys[0].kid
    )
    assert (
        adapter.validate_python(f"file://{jwks_file}").jwks.keys[0].kid
        == keyset.keys[0].kid
    )

    # Test with can load the keystore data directly from a JSON string
    assert (
        adapter.validate_python(json.dumps(keyset.as_dict(private=True)))
        .jwks.keys[0]
        .kid
        == keyset.keys[0].kid
    )
