from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from git import Repo

from diracx.core.config import Config, LocalGitConfigSource, get_config
from diracx.core.properties import SecurityProperty
from diracx.db.auth.db import AuthDB
from diracx.db.jobs.db import JobDB
from diracx.routers import app
from diracx.routers.auth import create_access_token

# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "21e98a30bb41420dc601dea1dc1f85ecee3b4d702547bea355c07ab44fd7f3c3"
ALGORITHM = "HS256"
ISSUER = "http://lhcbdirac.cern.ch/"
AUDIENCE = "dirac"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


@pytest.fixture
def with_config_repo(tmp_path, monkeypatch):
    monkeypatch.setattr(
        app,
        "dependency_overrides",
        {get_config: lambda: LocalGitConfigSource(tmp_path).read_config()},
    )

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
                            "DN": "/DC=invalid/DC=testca/OU=Organic Units/OU=Users/CN=chaen/CN=1234/CN=Christophe Haen",
                            "CA": "/DC=invalid/DC=testca/CN=Test CA",
                        }
                    },
                    "Groups": {
                        "lhcb_user": {
                            "Properties": ["NormalUser", "PrivateLimitedDelegation"],
                            "Users": ["b824d4dc-1f9d-4ee8-8df5-c0ae55d46041"],
                        }
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
    LocalGitConfigSource.clear_caches()


@pytest.fixture
def fake_secrets(monkeypatch, with_config_repo):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    monkeypatch.setenv("DIRACX_SECRET_DB_URL_AUTH", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("DIRACX_SECRET_DB_URL_JOBS", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("DIRACX_SECRET_TOKEN_KEY", pem)
    monkeypatch.setenv("DIRACX_SECRET_CONFIG", f"file://{with_config_repo}")
    yield


@pytest.fixture
def disable_events(monkeypatch):
    monkeypatch.setattr(app.router, "on_startup", [])
    monkeypatch.setattr(app.router, "on_shutdown", [])
    yield


@pytest.fixture
async def job_engine():
    await JobDB.make_engine("sqlite+aiosqlite:///:memory:")
    yield
    await JobDB.destroy_engine()


@pytest.fixture
async def auth_engine():
    await AuthDB.make_engine("sqlite+aiosqlite:///:memory:")
    yield
    await AuthDB.destroy_engine()


@pytest.fixture
def test_client(with_config_repo, disable_events, auth_engine, job_engine):
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def normal_user_client(test_client):
    payload = {
        "sub": "testingVO:yellow-sub",
        "aud": AUDIENCE,
        "iss": ISSUER,
        "dirac_properties": [SecurityProperty.NORMAL_USER],
        "jti": str(uuid4()),
        "preferred_username": "preferred_username",
        "dirac_group": "test_group",
        "vo": "lhcb",
    }
    token = create_access_token(payload)
    test_client.headers["Authorization"] = f"Bearer {token}"
    test_client.dirac_token_payload = payload
    yield test_client
