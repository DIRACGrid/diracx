#!/usr/bin/env python3
"""Generate OpenAPI specification from DiracX FastAPI app.

This script creates docs/openapi.json from the DiracX FastAPI application.
It uses the same approach as the test suite to create a minimal app configuration
with test settings and a dummy git config repository.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from git import Repo
from joserfc.jwk import KeySet, OKPKey

from diracx.core.config import Config
from diracx.core.extensions import select_from_extension
from diracx.core.settings import (
    AuthSettings,
    DevelopmentSettings,
    SandboxStoreSettings,
)
from diracx.routers import create_app_inner


def create_test_config_repo() -> Path:
    """Create a minimal git repository with test configuration.

    Returns:
        Path to the temporary git repository

    """
    tmp_path = Path(tempfile.mkdtemp(prefix="diracx-config-"))

    repo = Repo.init(tmp_path, initial_branch="master")
    cs_file = tmp_path / "default.yml"
    example_cs = Config.model_validate(
        {
            "DIRAC": {},
            "Registry": {
                "lhcb": {
                    "DefaultGroup": "lhcb_user",
                    "DefaultProxyLifeTime": 432000,
                    "DefaultStorageQuota": 2000,
                    "IdP": {
                        "URL": "https://idp-server.invalid",
                        "ClientID": "test-idp",
                    },
                    "Users": {
                        "b824d4dc-1f9d-4ee8-8df5-c0ae55d46041": {
                            "PreferedUsername": "chaen",
                            "Email": None,
                        },
                        "c935e5ed-2g0e-5ff9-9eg6-d1bf66e57152": {
                            "PreferedUsername": "albdr",
                            "Email": None,
                        },
                    },
                    "Groups": {
                        "lhcb_user": {
                            "Properties": ["NormalUser", "PrivateLimitedDelegation"],
                            "Users": [
                                "b824d4dc-1f9d-4ee8-8df5-c0ae55d46041",
                                "c935e5ed-2g0e-5ff9-9eg6-d1bf66e57152",
                            ],
                        },
                        "lhcb_prmgr": {
                            "Properties": ["NormalUser", "ProductionManagement"],
                            "Users": ["b824d4dc-1f9d-4ee8-8df5-c0ae55d46041"],
                        },
                        "lhcb_tokenmgr": {
                            "Properties": ["NormalUser", "ProxyManagement"],
                            "Users": ["c935e5ed-2g0e-5ff9-9eg6-d1bf66e57152"],
                        },
                    },
                }
            },
            "Operations": {},
            "Systems": {
                "WorkloadManagement": {
                    "Databases": {
                        "JobDB": {
                            "DBName": "xyz",
                            "Host": "xyz",
                            "Port": 9999,
                            "MaxRescheduling": 3,
                        },
                        "JobLoggingDB": {
                            "DBName": "xyz",
                            "Host": "xyz",
                            "Port": 9999,
                        },
                        "PilotAgentsDB": {
                            "DBName": "xyz",
                            "Host": "xyz",
                            "Port": 9999,
                        },
                        "SandboxMetadataDB": {
                            "DBName": "xyz",
                            "Host": "xyz",
                            "Port": 9999,
                        },
                        "TaskQueueDB": {
                            "DBName": "xyz",
                            "Host": "xyz",
                            "Port": 9999,
                        },
                        "ElasticJobParametersDB": {
                            "DBName": "xyz",
                            "Host": "xyz",
                            "Port": 9999,
                        },
                        "VirtualMachineDB": {
                            "DBName": "xyz",
                            "Host": "xyz",
                            "Port": 9999,
                        },
                    },
                },
            },
        }
    )
    cs_file.write_text(example_cs.model_dump_json())
    repo.index.add(["default.yml"])  # Use relative path
    repo.index.commit("Initial configuration")
    return tmp_path


def create_test_auth_settings() -> AuthSettings:
    """Create test authentication settings with a generated key.

    Returns:
        AuthSettings instance with test configuration

    """
    from cryptography.fernet import Fernet
    from uuid_utils import uuid7

    private_key = OKPKey.generate_key(
        parameters={
            "key_ops": ["sign", "verify"],
            "alg": "EdDSA",
            "kid": uuid7().hex,
        }
    )
    fernet_key = Fernet.generate_key().decode()

    return AuthSettings(
        token_issuer="http://lalalalalalalalalalalala.invalid/",  # noqa: S106
        token_keystore=json.dumps(KeySet([private_key]).as_dict(private_keys=True)),
        state_key=fernet_key,
        allowed_redirects=[
            "http://diracx.test.invalid:8000/api/docs/oauth2-redirect",
        ],
    )


def create_test_sandbox_settings() -> SandboxStoreSettings:
    """Create test sandbox store settings for S3.

    Returns:
        SandboxStoreSettings instance with test configuration

    """
    return SandboxStoreSettings(
        bucket_name="sandboxes",
        s3_client_kwargs={
            "endpoint_url": "http://localhost:27132",
            "aws_access_key_id": "testing",
            "aws_secret_access_key": "testing",
        },
        auto_create_bucket=True,
    )


def generate_openapi_spec(output_path: Path) -> None:
    """Generate OpenAPI specification and save to file.

    Args:
        output_path: Path where the openapi.json file should be saved

    """
    print("🔧 Creating test configuration repository...")
    config_repo = create_test_config_repo()

    try:
        print("🔧 Setting up test settings...")
        auth_settings = create_test_auth_settings()
        sandbox_settings = create_test_sandbox_settings()
        dev_settings = DevelopmentSettings()

        print("🔧 Discovering DiracX services...")
        from diracx.core.config import ConfigSource
        from diracx.routers.access_policies import BaseAccessPolicy

        enabled_systems = {
            e.name for e in select_from_extension(group="diracx.services")
        }
        database_urls = {
            e.name: "sqlite+aiosqlite:///:memory:"
            for e in select_from_extension(group="diracx.dbs.sql")
        }
        os_database_conn_kwargs = {
            e.name: {"sqlalchemy_dsn": "sqlite+aiosqlite:///:memory:"}
            for e in select_from_extension(group="diracx.dbs.os")
        }

        # Setup access policies
        all_access_policies = {
            e.name: BaseAccessPolicy.available_implementations(e.name)
            for e in select_from_extension(group="diracx.access_policies")
        }

        config_source = ConfigSource.create_from_url(
            backend_url=f"git+file://{config_repo}"
        )

        print("🔧 Creating FastAPI application...")
        app = create_app_inner(
            enabled_systems=enabled_systems,
            all_service_settings=[auth_settings, sandbox_settings, dev_settings],
            database_urls=database_urls,
            os_database_conn_kwargs=os_database_conn_kwargs,
            config_source=config_source,
            all_access_policies=all_access_policies,
        )

        print("📝 Generating OpenAPI specification...")
        openapi_schema = app.openapi()

        print(f"💾 Writing to {output_path.relative_to(output_path.parent.parent)}...")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(openapi_schema, indent=2))

        # Print some statistics
        paths_count = len(openapi_schema.get("paths", {}))
        schemas_count = len(openapi_schema.get("components", {}).get("schemas", {}))
        print(
            f"✅ Generated OpenAPI spec with {paths_count} paths and {schemas_count} schemas"
        )

    finally:
        # Clean up the temporary config repository
        import shutil

        shutil.rmtree(config_repo, ignore_errors=True)


def main():
    """Main entry point."""
    repo_root = Path(__file__).parent.parent
    output_path = repo_root / "docs" / "openapi.json"

    print("🚀 Generating OpenAPI specification for DiracX...\n")
    try:
        generate_openapi_spec(output_path)
        print("\n✅ OpenAPI specification generated successfully!")
        return 0
    except Exception as e:
        print(f"\n❌ Failed to generate OpenAPI specification: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
