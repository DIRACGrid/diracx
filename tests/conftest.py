from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from git import Repo

from diracx.core.config import Config, LocalGitConfigSource
from diracx.core.properties import SecurityProperty
from diracx.core.settings import ServiceSettingsBase
from diracx.routers import create_app_inner
from diracx.routers.auth import AuthSettings, create_access_token
from diracx.routers.configuration import ConfigSettings

# to get a string like this run:
# openssl rand -hex 32
ALGORITHM = "HS256"
ISSUER = "http://lhcbdirac.cern.ch/"
AUDIENCE = "dirac"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


@pytest.fixture
def test_auth_settings() -> AuthSettings:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    yield AuthSettings(token_key=pem)


@pytest.fixture
def test_settings(with_config_repo, test_auth_settings) -> list[ServiceSettingsBase]:
    yield [
        test_auth_settings,
        ConfigSettings(backend_url=f"file://{with_config_repo}"),
    ]


@pytest.fixture
def with_app(test_settings):
    yield create_app_inner(
        *test_settings,
        database_urls={
            "JobDB": "sqlite+aiosqlite:///:memory:",
            "AuthDB": "sqlite+aiosqlite:///:memory:",
        },
    )


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
def test_client(with_app):
    with TestClient(with_app) as test_client:
        yield test_client


@pytest.fixture
def normal_user_client(test_client, test_auth_settings):
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
    token = create_access_token(payload, test_auth_settings)
    test_client.headers["Authorization"] = f"Bearer {token}"
    test_client.dirac_token_payload = payload
    yield test_client
