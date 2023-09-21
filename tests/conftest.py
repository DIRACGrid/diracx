from __future__ import annotations

import contextlib
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import requests
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from git import Repo
from moto.server import ThreadedMotoServer

from diracx.core.config import Config, ConfigSource
from diracx.core.preferences import get_diracx_preferences
from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.routers import create_app_inner
from diracx.routers.auth import AuthSettings, create_token
from diracx.routers.job_manager.sandboxes import SandboxStoreSettings

# to get a string like this run:
# openssl rand -hex 32
ALGORITHM = "HS256"
ISSUER = "http://lhcbdirac.cern.ch/"
AUDIENCE = "dirac"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


def pytest_addoption(parser):
    parser.addoption(
        "--regenerate-client",
        action="store_true",
        default=False,
        help="Regenerate the AutoREST client",
    )
    parser.addoption(
        "--demo-dir",
        type=Path,
        default=None,
        help="Path to a diracx-charts directory with the demo running",
    )


def pytest_collection_modifyitems(config, items):
    """
    Disable the test_regenerate_client if not explicitly asked for
    """
    if config.getoption("--regenerate-client"):
        # --regenerate-client given in cli: allow client re-generation
        return
    skip_regen = pytest.mark.skip(reason="need --regenerate-client option to run")
    found = False
    for item in items:
        if item.name == "test_regenerate_client":
            item.add_marker(skip_regen)
            found = True
    # It's ok not to find it if we run only specific tests
    if not found and not config.getoption("file_or_dir"):
        raise RuntimeError("Could not find test_regenerate_client")


@pytest.fixture
def test_auth_settings() -> AuthSettings:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    yield AuthSettings(
        token_key=pem,
        allowed_redirects=[
            "http://diracx.test.invalid:8000/api/docs/oauth2-redirect",
        ],
    )


@pytest.fixture(scope="session")
def aio_moto():
    """Start the moto server in a separate thread and return the base URL

    The mocking provided by moto doesn't play nicely with aiobotocore so we use
    the server directly. See https://github.com/aio-libs/aiobotocore/issues/755
    """
    port = 27132
    server = ThreadedMotoServer(port=port)
    server.start()
    yield {
        "endpoint_url": f"http://localhost:{port}",
        "aws_access_key_id": "testing",
        "aws_secret_access_key": "testing",
    }
    server.stop()


@pytest.fixture(scope="function")
def test_sandbox_settings(aio_moto) -> SandboxStoreSettings:
    yield SandboxStoreSettings(
        bucket_name="sandboxes",
        s3_client_kwargs=aio_moto,
        auto_create_bucket=True,
    )


@pytest.fixture
def with_app(test_auth_settings, test_sandbox_settings, with_config_repo):
    """
    Create a DiracxApp with hard coded configuration for test
    """
    app = create_app_inner(
        enabled_systems={".well-known", "auth", "config", "jobs"},
        all_service_settings=[
            test_auth_settings,
            test_sandbox_settings,
        ],
        database_urls={
            "JobDB": "sqlite+aiosqlite:///:memory:",
            "JobLoggingDB": "sqlite+aiosqlite:///:memory:",
            "TaskQueueDB": "sqlite+aiosqlite:///:memory:",
            "AuthDB": "sqlite+aiosqlite:///:memory:",
            "SandboxMetadataDB": "sqlite+aiosqlite:///:memory:",
        },
        os_database_conn_kwargs={
            # TODO: JobParametersDB
        },
        config_source=ConfigSource.create_from_url(
            backend_url=f"git+file://{with_config_repo}"
        ),
    )

    @contextlib.asynccontextmanager
    async def create_db_schemas(app=app):
        """Create DB schema's based on the DBs available in app.dependency_overrides"""
        from diracx.db.sql.utils import BaseSQLDB

        for k, v in app.dependency_overrides.items():
            # Ignore dependency overrides which aren't BaseSQLDB.transaction
            if k.__func__ != BaseSQLDB.transaction.__func__:
                continue
            # The first argument of the overridden BaseSQLDB.transaction is the DB object
            db = v.args[0]
            assert isinstance(db, BaseSQLDB), (k, db)
            # Fill the DB schema
            async with db.engine.begin() as conn:
                # set PRAGMA foreign_keys=ON if sqlite
                if db._db_url.startswith("sqlite"):
                    await conn.exec_driver_sql("PRAGMA foreign_keys=ON")
                await conn.run_sync(db.metadata.create_all)

        yield

    # Add create_db_schemas to the end of the lifetime_functions so that the
    # other lifetime_functions (i.e. those which run db.engine_context) have
    # already been ran
    app.lifetime_functions.append(create_db_schemas)

    yield app


@pytest.fixture
def with_config_repo(tmp_path):
    repo = Repo.init(tmp_path, initial_branch="master")
    cs_file = tmp_path / "default.yml"
    example_cs = Config.parse_obj(
        {
            "DIRAC": {},
            "Registry": {
                "lhcb": {
                    "DefaultGroup": "lhcb_user",
                    "DefaultProxyLifeTime": 432000,
                    "DefaultStorageQuota": 2000,
                    "IdP": {
                        "URL": "https://lhcb-auth.web.cern.ch",
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
                        "lhcb_tokenmgr": {
                            "Properties": ["NormalUser", "ProxyManagement"],
                            "Users": ["c935e5ed-2g0e-5ff9-9eg6-d1bf66e57152"],
                        },
                    },
                }
            },
            "Operations": {"Defaults": {}},
        }
    )
    cs_file.write_text(example_cs.json())
    repo.index.add([cs_file])  # add it to the index
    repo.index.commit("Added a new file")
    yield tmp_path


@pytest.fixture
def test_client(with_app):
    with TestClient(with_app) as test_client:
        yield test_client


@pytest.fixture
def normal_user_client(test_client, test_auth_settings):
    payload = {
        "sub": "testingVO:yellow-sub",
        "exp": datetime.now()
        + timedelta(test_auth_settings.access_token_expire_minutes),
        "aud": AUDIENCE,
        "iss": ISSUER,
        "dirac_properties": [NORMAL_USER],
        "jti": str(uuid4()),
        "preferred_username": "preferred_username",
        "dirac_group": "test_group",
        "vo": "lhcb",
    }
    token = create_token(payload, test_auth_settings)
    test_client.headers["Authorization"] = f"Bearer {token}"
    test_client.dirac_token_payload = payload
    yield test_client


@pytest.fixture
def admin_user_client(test_client, test_auth_settings):
    payload = {
        "sub": "testingVO:yellow-sub",
        "aud": AUDIENCE,
        "iss": ISSUER,
        "dirac_properties": [JOB_ADMINISTRATOR],
        "jti": str(uuid4()),
        "preferred_username": "preferred_username",
        "dirac_group": "test_group",
        "vo": "lhcb",
    }
    token = create_token(payload, test_auth_settings)
    test_client.headers["Authorization"] = f"Bearer {token}"
    test_client.dirac_token_payload = payload
    yield test_client


@pytest.fixture(scope="session")
def demo_dir(request) -> Path:
    demo_dir = request.config.getoption("--demo-dir")
    if demo_dir is None:
        pytest.skip("Requires a running instance of the DiracX demo")
    demo_dir = (demo_dir / ".demo").resolve()
    yield demo_dir


@pytest.fixture(scope="session")
def demo_urls(demo_dir):
    helm_values = yaml.safe_load((demo_dir / "values.yaml").read_text())
    yield helm_values["developer"]["urls"]


@pytest.fixture(scope="session")
def demo_kubectl_env(demo_dir):
    """Get the dictionary of environment variables for kubectl to control the demo"""
    kube_conf = demo_dir / "kube.conf"
    if not kube_conf.exists():
        raise RuntimeError(f"Could not find {kube_conf}, is the demo running?")

    env = {
        **os.environ,
        "KUBECONFIG": str(kube_conf),
        "PATH": f"{demo_dir}:{os.environ['PATH']}",
    }

    # Check that we can run kubectl
    pods_result = subprocess.check_output(
        ["kubectl", "get", "pods"], env=env, text=True
    )
    assert "diracx" in pods_result

    yield env


@pytest.fixture
def cli_env(monkeypatch, tmp_path, demo_urls, demo_dir):
    """Set up the environment for the CLI"""
    diracx_url = demo_urls["diracx"]
    ca_path = demo_dir / "demo-ca.pem"
    if not ca_path.exists():
        raise RuntimeError(f"Could not find {ca_path}, is the demo running?")

    # Ensure the demo is working
    r = requests.get(f"{diracx_url}/api/openapi.json", verify=ca_path)
    r.raise_for_status()
    assert r.json()["info"]["title"] == "Dirac"

    env = {
        "DIRACX_URL": diracx_url,
        "DIRACX_CA_PATH": str(ca_path),
        "HOME": str(tmp_path),
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    yield env

    # The DiracX preferences are cached however when testing this cache is invalid
    get_diracx_preferences.cache_clear()


@pytest.fixture
async def with_cli_login(monkeypatch, capfd, cli_env, tmp_path):
    from .cli.test_login import test_login

    try:
        credentials = await test_login(monkeypatch, capfd, cli_env)
    except Exception:
        pytest.skip("Login failed, fix test_login to re-enable this test")

    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text(credentials)
    monkeypatch.setenv("DIRACX_CREDENTIALS_PATH", str(credentials_path))
    yield
